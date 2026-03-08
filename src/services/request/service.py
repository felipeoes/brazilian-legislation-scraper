import asyncio
import ssl
import aiohttp
from aiohttp_socks import ProxyConnector
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.scraper.base.concurrency import RateLimiter


def _make_ssl_context(verify: bool = True) -> ssl.SSLContext:
    """Build an SSL context. When *verify* is ``False`` certificate
    checks are disabled — use only for sites with broken certificates."""
    ctx = ssl.create_default_context()
    if not verify:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


class RetryableHTTPError(Exception):
    """Raised to trigger tenacity retry on transient HTTP errors."""

    pass


class FailedRequest:
    """Sentinel returned by ``make_request`` / ``get_soup`` on failure.

    Falsy so that ``if not resp:`` works identically to the old
    ``if resp is None:`` pattern, but carries diagnostic information.

    Attributes:
        url: The URL that was requested.
        status: HTTP status code if the server responded, else ``None``.
        reason: Human-readable error description.
    """

    __slots__ = ("url", "status", "reason")

    def __init__(
        self, url: str = "", status: int | None = None, reason: str = ""
    ) -> None:
        self.url = url
        self.status = status
        self.reason = reason

    def __bool__(self) -> bool:
        return False

    def __repr__(self) -> str:
        parts = [f"url={self.url!r}"]
        if self.status is not None:
            parts.append(f"status={self.status}")
        if self.reason:
            parts.append(f"reason={self.reason!r}")
        return f"FailedRequest({', '.join(parts)})"


class RequestService:
    def __init__(
        self,
        rps: float = 10,
        verbose: bool = False,
        proxy_service=None,
        max_retries: int = 5,
        verify_ssl: bool = False,
        disable_cookies: bool = False,
    ):
        self.rps = rps
        self.verbose = verbose
        self.max_retries = max_retries
        self._ssl_ctx = _make_ssl_context(verify_ssl)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
        }
        self._disable_cookies = disable_cookies
        self._default_session: aiohttp.ClientSession | None = None
        self._proxy_sessions: dict[str, aiohttp.ClientSession] = {}
        self._rate_limiter = RateLimiter(rps)
        self.proxy_service = proxy_service
        self._retry_strategy = AsyncRetrying(
            stop=stop_after_attempt(max_retries),
            wait=wait_exponential(multiplier=2, min=2, max=60),
            retry=retry_if_exception_type(RetryableHTTPError),
            reraise=True,
        )

    _MAX_PROXY_SESSIONS = 50

    async def _ensure_session(self, proxy: str | None = None) -> aiohttp.ClientSession:
        """Get or create the aiohttp session."""
        if proxy:
            if proxy not in self._proxy_sessions or self._proxy_sessions[proxy].closed:
                # Evict oldest sessions when the pool grows too large
                if len(self._proxy_sessions) >= self._MAX_PROXY_SESSIONS:
                    oldest_key = next(iter(self._proxy_sessions))
                    old_session = self._proxy_sessions.pop(oldest_key)
                    if not old_session.closed:
                        await old_session.close()
                connector = ProxyConnector.from_url(proxy, ssl=self._ssl_ctx)
                timeout = aiohttp.ClientTimeout(total=120)
                self._proxy_sessions[proxy] = aiohttp.ClientSession(
                    headers=self.headers,
                    connector=connector,
                    timeout=timeout,
                )
            return self._proxy_sessions[proxy]

        if self._default_session is None or self._default_session.closed:
            connector = aiohttp.TCPConnector(ssl=self._ssl_ctx)
            timeout = aiohttp.ClientTimeout(total=120)
            kwargs: dict = {
                "headers": self.headers,
                "connector": connector,
                "timeout": timeout,
            }
            if self._disable_cookies:
                kwargs["cookie_jar"] = aiohttp.DummyCookieJar()
            self._default_session = aiohttp.ClientSession(**kwargs)
        return self._default_session

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
                post_kwargs["ssl"] = self._ssl_ctx
                if json is not None:
                    post_kwargs["json"] = json
                if payload is not None:
                    post_kwargs["data"] = payload

                resp = await session.post(url, **post_kwargs)
            else:
                resp = await session.get(
                    url,
                    timeout=req_timeout,
                    ssl=self._ssl_ctx,
                    **kwargs,
                )

            retryable = {403, 408, 429, 500, 502, 503, 504}

            if resp.status in retryable:
                resp.release()
                raise RetryableHTTPError(f"HTTP {resp.status}")

            return resp
        except RetryableHTTPError:
            raise
        except (
            aiohttp.ClientError,
            aiohttp.ServerDisconnectedError,
            asyncio.TimeoutError,
        ) as e:
            if proxy:
                if (
                    proxy in self._proxy_sessions
                    and not self._proxy_sessions[proxy].closed
                ):
                    await self._proxy_sessions[proxy].close()
                self._proxy_sessions.pop(proxy, None)
            raise RetryableHTTPError(str(e)) from e

    async def _run_with_retry(
        self, url, method, json, payload, timeout, kwargs, on_response
    ):
        """Run a request with the shared tenacity retry loop.

        Calls ``_do_request`` on each attempt, then passes the response to
        *on_response* which may return a result or raise ``RetryableHTTPError``
        to trigger another attempt.

        Returns whatever *on_response* returns on success, or a falsy
        ``FailedRequest`` on permanent failure.
        """
        last_error: Exception | None = None
        attempt_count = 0
        try:
            async for attempt in self._retry_strategy:
                with attempt:
                    attempt_count += 1
                    try:
                        resp = await self._do_request(
                            url, method, json, payload, timeout, **kwargs
                        )
                        return await on_response(resp)
                    except RetryableHTTPError as e:
                        last_error = e
                        raise
        except RetryableHTTPError:
            reason = f"{last_error} (failed after {attempt_count} attempt{'s' if attempt_count != 1 else ''})"
            return FailedRequest(url=url, reason=reason)
        except Exception as e:
            return FailedRequest(url=url, reason=str(e))

    async def fetch_bytes(
        self,
        url: str,
        method: str = "GET",
        json: dict | None = None,
        payload: list | dict | None = None,
        timeout: int = 120,
        **kwargs,
    ) -> tuple[bytes, aiohttp.ClientResponse] | FailedRequest:
        """Make a request and read the full response body inside the retry loop.

        Unlike ``make_request``, the body read is part of the retried operation,
        so ``ContentLengthError`` / ``ClientPayloadError`` (truncated responses)
        are retried automatically like any other transient network error.

        Returns a ``(bytes, ClientResponse)`` tuple on success so callers can
        still inspect headers/content-type, or a falsy ``FailedRequest`` on
        permanent failure.
        """

        async def _on_response(resp: aiohttp.ClientResponse):
            try:
                body = await resp.read()
            except aiohttp.ClientPayloadError as e:
                raise RetryableHTTPError(str(e)) from e
            return body, resp

        return await self._run_with_retry(
            url, method, json, payload, timeout, kwargs, _on_response
        )

    async def make_request(
        self,
        url: str,
        method: str = "GET",
        json: dict | None = None,
        payload: list | dict | None = None,
        timeout: int = 120,
        **kwargs,
    ) -> aiohttp.ClientResponse | FailedRequest:
        """Make async HTTP request with automatic retry on transient errors.

        Returns an ``aiohttp.ClientResponse`` on success,
        or a **falsy** ``FailedRequest`` on failure — use ``if not resp:``
        to branch on errors and inspect ``resp.status`` / ``resp.reason``
        for diagnostics.
        """

        async def _on_response(resp: aiohttp.ClientResponse):
            return resp

        return await self._run_with_retry(
            url, method, json, payload, timeout, kwargs, _on_response
        )

    async def get_soup(
        self, url: str, method: str = "GET", **kwargs
    ) -> BeautifulSoup | FailedRequest:
        """Get BeautifulSoup object from given url (async).

        Returns a ``FailedRequest`` (falsy) instead of ``None`` on failure.
        """
        resp = await self.make_request(url, method=method, **kwargs)
        if not resp:
            return resp
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
