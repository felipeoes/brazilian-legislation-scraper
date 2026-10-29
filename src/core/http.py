"""Buffered HTTP responses with rate limiting and retries."""

import asyncio
import json
import ssl
from dataclasses import dataclass
from email.message import Message
from typing import Any

import aiohttp
from aiohttp_socks import (
    ProxyConnectionError,
    ProxyConnector,
    ProxyError,
    ProxyTimeoutError,
)
from bs4 import BeautifulSoup
from multidict import CIMultiDict
from pyrate_limiter import Duration, InMemoryBucket, Limiter, Rate
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.core.proxy import ProxyService


class RateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise ValueError("rps must be positive")
        self.requests_per_second = requests_per_second
        self._limiter = Limiter(
            InMemoryBucket(
                [Rate(1, max(1, Duration.SECOND * (1 / requests_per_second)))]
            )
        )

    async def acquire(self) -> None:
        await self._limiter.try_acquire_async("req")


class HttpError(Exception):
    def __init__(self, url: str, status: int | None, reason: str) -> None:
        self.url, self.status, self.reason = (url, status, reason)
        super().__init__(f"{url}: HTTP {status}: {reason}")


class _RetryableError(HttpError):
    pass


@dataclass(frozen=True)
class Response:
    url: str
    status: int
    headers: dict[str, str]
    content: bytes

    def _message(self) -> Message:
        message = Message()
        for key, value in self.headers.items():
            message[key] = value
        return message

    @property
    def text(self) -> str:
        charset = self._message().get_content_charset() or "utf-8"
        try:
            return self.content.decode(charset, errors="replace")
        except LookupError:
            return self.content.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.text)

    def soup(self) -> BeautifulSoup:
        return BeautifulSoup(
            self.content, "lxml", from_encoding=self._message().get_content_charset()
        )

    @property
    def content_type(self) -> str:
        return self._message().get_content_type()

    @property
    def filename(self) -> str:
        return self._message().get_filename() or "document"


class HttpClient:
    def __init__(
        self,
        rps: float = 10,
        max_retries: int = 6,
        proxy_service: ProxyService | None = None,
        verify_ssl: bool = False,
        disable_cookies: bool = False,
        proxy_timeout: int = 15,
    ) -> None:
        if max_retries < 1:
            raise ValueError("max_retries must be positive")
        self.max_retries = max_retries
        self.limiter = RateLimiter(rps)
        self.proxy_service = proxy_service
        self._ssl_ctx = ssl.create_default_context()
        if not verify_ssl:
            self._ssl_ctx.check_hostname = False
            self._ssl_ctx.verify_mode = ssl.CERT_NONE
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
        }
        self._disable_cookies = disable_cookies
        self._proxy_timeout = proxy_timeout
        self._default_session: aiohttp.ClientSession | None = None
        self._proxy_sessions: dict[str, aiohttp.ClientSession] = {}
        self._session_lock = asyncio.Lock()

    async def _ensure_session(self, proxy: str | None = None) -> aiohttp.ClientSession:
        async with self._session_lock:
            session = (
                self._proxy_sessions.get(proxy) if proxy else self._default_session
            )
            if session is None or session.closed:
                connector = (
                    ProxyConnector.from_url(proxy, ssl=self._ssl_ctx)
                    if proxy
                    else aiohttp.TCPConnector(ssl=self._ssl_ctx)
                )
                session = aiohttp.ClientSession(
                    headers=self.headers,
                    connector=connector,
                    cookie_jar=aiohttp.DummyCookieJar()
                    if self._disable_cookies
                    else None,
                )
                if proxy:
                    self._proxy_sessions[proxy] = session
                else:
                    self._default_session = session
            return session

    async def request(
        self,
        url: str,
        method: str = "GET",
        *,
        ok_statuses: tuple[int, ...] = (),
        timeout: float = 120,
        **kwargs: Any,
    ) -> Response:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=2, min=5, max=60),
            retry=retry_if_exception_type(_RetryableError),
            reraise=True,
        ):
            with attempt:
                await self.limiter.acquire()
                proxy = (
                    await self.proxy_service.get_proxy() if self.proxy_service else None
                )
                session = await self._ensure_session(proxy)
                try:
                    request_timeout = aiohttp.ClientTimeout(
                        total=timeout, connect=self._proxy_timeout if proxy else None
                    )
                    async with session.request(
                        method, url, timeout=request_timeout, **kwargs
                    ) as response:
                        if (
                            response.status >= 400
                            and response.status not in ok_statuses
                        ):
                            error_cls = (
                                _RetryableError
                                if response.status
                                in {403, 408, 429, 500, 502, 503, 504}
                                else HttpError
                            )
                            raise error_cls(
                                url,
                                response.status,
                                response.reason or "Request failed",
                            )
                        content = await response.read()
                        return Response(
                            str(response.url),
                            response.status,
                            CIMultiDict(response.headers),
                            content,
                        )
                except (
                    aiohttp.ClientError,
                    TimeoutError,
                    ConnectionError,
                    ProxyError,
                    ProxyConnectionError,
                    ProxyTimeoutError,
                ) as exc:
                    raise _RetryableError(url, None, str(exc)) from exc
        raise AssertionError("Retry loop did not execute")

    async def close(self) -> None:
        sessions = list(self._proxy_sessions.values())
        if self._default_session is not None:
            sessions.append(self._default_session)
        results = await asyncio.gather(
            *(s.close() for s in sessions), return_exceptions=True
        )
        self._proxy_sessions.clear()
        self._default_session = None
        if self.proxy_service:
            await self.proxy_service.cleanup()
        failures = [r for r in results if isinstance(r, Exception)]
        if failures:
            raise ExceptionGroup("HTTP cleanup failed", failures)
