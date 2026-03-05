import asyncio
import ssl
import aiohttp
from aiohttp_socks import ProxyConnector
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.scraper.base.concurrency import RateLimiter

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


class RetryableHTTPError(Exception):
    """Raised to trigger tenacity retry on transient HTTP errors."""

    pass


class RequestService:
    def __init__(
        self,
        rps: float = 10,
        verbose: bool = False,
        proxy_service=None,
        max_workers: int = 10,
    ):
        self.rps = rps
        self.verbose = verbose
        self.max_workers = max_workers
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
        }
        self._default_session: aiohttp.ClientSession | None = None
        self._proxy_sessions: dict[str, aiohttp.ClientSession] = {}
        self._rate_limiter = RateLimiter(rps)
        self.proxy_service = proxy_service

    async def _ensure_session(self, proxy: str | None = None) -> aiohttp.ClientSession:
        """Get or create the aiohttp session."""
        if proxy:
            if proxy not in self._proxy_sessions or self._proxy_sessions[proxy].closed:
                connector = ProxyConnector.from_url(proxy, ssl=_SSL_CTX)
                timeout = aiohttp.ClientTimeout(total=120)
                self._proxy_sessions[proxy] = aiohttp.ClientSession(
                    headers=self.headers,
                    connector=connector,
                    timeout=timeout,
                )
            return self._proxy_sessions[proxy]

        if self._default_session is None or self._default_session.closed:
            # limit_per_host caps simultaneous TCP connections to any single server,
            # preventing 'Connection closed' from servers that reject high concurrency.
            connector = aiohttp.TCPConnector(
                ssl=_SSL_CTX, limit=100, limit_per_host=self.max_workers
            )
            timeout = aiohttp.ClientTimeout(total=120)
            self._default_session = aiohttp.ClientSession(
                headers=self.headers,
                connector=connector,
                timeout=timeout,
            )
        return self._default_session

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        retry=retry_if_exception_type(RetryableHTTPError),
        reraise=True,
    )
    async def _do_request(
        self,
        url: str,
        method: str = "GET",
        json: dict | None = None,
        payload: list | dict | None = None,
        timeout: int = 120,
        **kwargs,
    ) -> aiohttp.ClientResponse:
        """Internal: single-attempt HTTP request; raises RetryableHTTPError for transient failures."""
        await self._rate_limiter.acquire()
        proxy = None
        if self.proxy_service:
            proxy = await self.proxy_service.get_proxy()
            if self.verbose and proxy:
                logger.info(f"Using proxy: {proxy}")

        session = await self._ensure_session(proxy=proxy)
        req_timeout = aiohttp.ClientTimeout(total=timeout)

        try:
            if method == "POST":
                post_kwargs = kwargs.copy()
                post_kwargs["timeout"] = req_timeout
                post_kwargs["ssl"] = _SSL_CTX
                if json is not None:
                    post_kwargs["json"] = json
                if payload is not None:
                    post_kwargs["data"] = payload

                resp = await session.post(url, **post_kwargs)
            else:
                resp = await session.get(
                    url,
                    timeout=req_timeout,
                    ssl=_SSL_CTX,
                    **kwargs,
                )

            # Pre-read the body so callers can use it freely
            await resp.read()

            # Check for Portuguese server error in raw bytes (ASCII-safe pattern)
            # to avoid decoding the entire body to text on every request.
            if b"O servidor encontrou um erro interno, ou est" in (resp._body or b""):
                raise RetryableHTTPError("Server overloaded / internal error")

            if resp.status in (408, 429, 500, 502, 503, 504):
                raise RetryableHTTPError(f"HTTP {resp.status}")

            return resp
        except RetryableHTTPError:
            raise
        except (
            aiohttp.ClientError,
            aiohttp.ServerDisconnectedError,
            asyncio.TimeoutError,
        ) as e:
            # Only reset isolated proxy sessions — never close the shared
            # default session here, because that would kill ALL other
            # concurrent in-flight requests on the same session.
            # The TCPConnector handles per-connection recycling automatically.
            if proxy:
                if (
                    proxy in self._proxy_sessions
                    and not self._proxy_sessions[proxy].closed
                ):
                    await self._proxy_sessions[proxy].close()
                self._proxy_sessions.pop(proxy, None)
            raise RetryableHTTPError(str(e)) from e

    async def make_request(
        self,
        url: str,
        method: str = "GET",
        json: dict | None = None,
        payload: list | dict | None = None,
        timeout: int = 60,
        **kwargs,
    ) -> aiohttp.ClientResponse | None:
        """Make async HTTP request with automatic retry on transient errors.

        Returns an aiohttp.ClientResponse that has already been read
        (response.read() called), so .status, .text(), .json() etc. are safe.
        Returns None for 4xx client errors or after all retries are exhausted.
        """
        try:
            return await self._do_request(url, method, json, payload, timeout, **kwargs)
        except Exception:
            return None

    async def get_soup(
        self, url: str, method: str = "GET", **kwargs
    ) -> BeautifulSoup | None:
        """Get BeautifulSoup object from given url (async)."""
        resp = await self.make_request(url, method=method, **kwargs)
        if resp is None:
            return None
        # _do_request already called resp.read() to pre-buffer the body; using
        # resp.text() here correctly returns the buffered content instead of b"".
        body = await resp.text(errors="replace")
        return BeautifulSoup(body, "html.parser")

    async def cleanup(self):
        """Clean up aiohttp sessions."""
        if self._default_session and not self._default_session.closed:
            await self._default_session.close()

        for session in self._proxy_sessions.values():
            if not session.closed:
                await session.close()
        self._proxy_sessions.clear()

    @staticmethod
    def detect_content_info(response: aiohttp.ClientResponse) -> tuple[str, str]:
        """Extract filename and content type from an HTTP response.

        Returns:
            Tuple of ``(filename, content_type)``.
        """
        content_type = (response.content_type or "").lower()
        filename = "document"
        cd = response.headers.get("Content-Disposition", "")
        if "filename=" in cd:
            try:
                filename = cd.split("filename=")[1].strip("\"'").split(";")[0].strip()
            except Exception:
                pass
        if filename == "document":
            if "html" in content_type:
                filename = "document.html"
            elif "pdf" in content_type:
                filename = "document.pdf"
        return filename, content_type
