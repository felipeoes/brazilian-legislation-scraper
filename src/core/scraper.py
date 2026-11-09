"""Bounded listing → fetch → save pipeline shared by every scraper."""

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any, ClassVar
from urllib.parse import urljoin

from loguru import logger

from src.config import LOG_DIR
from src.core.browser import BrowserPool
from src.core.convert import (
    bytes_to_markdown,
    detect_extension,
    html_to_markdown,
    sniff_extension,
)
from src.core.document import DocRef, Document, clean_title
from src.core.http import HttpClient
from src.core.ocr import OCR, usage_scope
from src.core.store import Store

DEFAULT_VALID_SITUATION = "Não consta revogação expressa"
DEFAULT_INVALID_SITUATION = "Revogada"


@dataclass
class RunResult:
    docs: int = 0
    errors: int = 0
    skipped: int = 0
    listed: int = 0
    seconds: float = 0.0
    listing_seconds: float = 0.0
    fetch_seconds: list[float] = field(default_factory=list, repr=False)

    def stats(self) -> dict:
        """Performance summary stored with the run (see `main.py stats`)."""
        times = sorted(self.fetch_seconds)

        def quantile(q: float) -> float | None:
            return (
                round(times[min(len(times) - 1, int(len(times) * q))], 2)
                if times
                else None
            )

        return {
            "seconds": round(self.seconds, 1),
            "listing_seconds": round(self.listing_seconds, 1),
            "listed": self.listed,
            "docs": self.docs,
            "errors": self.errors,
            "skipped": self.skipped,
            "docs_per_minute": round(self.docs * 60 / self.seconds, 1)
            if self.seconds
            else 0.0,
            "fetch_p50": quantile(0.5),
            "fetch_p95": quantile(0.95),
            "fetch_max": quantile(1.0),
        }


class BaseScraper:
    name: ClassVar[str]
    base_url: ClassVar[str]
    types: ClassVar[dict | list] = {}
    situations: ClassVar[dict | list] = {}
    yearless: ClassVar[bool] = False

    def __init__(
        self,
        *,
        store: Store,
        http: HttpClient,
        years: range,
        ocr: OCR | None = None,
        browser: BrowserPool | None = None,
        workers: int = 16,
        overwrite: bool = False,
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be positive")
        self.store, self.http, self.ocr = (store, http, ocr)
        self.browser = browser or BrowserPool(limiter=http.limiter)
        self.years, self.workers, self.overwrite = (years, workers, overwrite)
        self._stack = AsyncExitStack()
        self._run_id: int | None = None
        self._result = RunResult()

    async def __aenter__(self) -> BaseScraper:
        try:
            self._stack.push_async_callback(self.http.close)
            self._stack.push_async_callback(self.browser.close)
            await self._stack.enter_async_context(self.store)
            await asyncio.to_thread(LOG_DIR.mkdir, parents=True, exist_ok=True)
            sink = logger.add(
                LOG_DIR / f"{self.name}.log",
                filter=lambda record: record["extra"].get("scraper") == self.name,
            )
            self._stack.callback(logger.remove, sink)
            return self
        except BaseException:
            await self._stack.aclose()
            raise

    async def __aexit__(self, *exc: Any) -> None:
        await self._stack.aclose()

    async def setup(self) -> None:
        pass

    async def list_documents(self, year: int | None) -> AsyncIterator[DocRef]:
        """List each type separately so one failing type doesn't stop the others."""
        for name, value in self.types.items():
            try:
                async for ref in self.list_type(year, name, value):
                    yield ref
            except Exception as exc:
                await self.error({"year": year, "type": name}, exc)

    async def list_type(
        self, year: int | None, name: str, value: Any
    ) -> AsyncIterator[DocRef]:
        raise NotImplementedError
        yield

    async def fetch(self, ref: DocRef) -> Document | None:
        """None is recorded as an error; skip unwanted refs while listing instead."""
        raise NotImplementedError

    async def error(self, ref: DocRef, exc: Exception) -> None:
        self._result.errors += 1
        message = str(exc)
        if exc.__cause__ is not None:
            message += f" (caused by {type(exc.__cause__).__name__}: {exc.__cause__})"
        await self.store.record_error(
            run_id=self._run_id,
            message=message,
            year=ref.get("year"),
            type=ref.get("type", ""),
            situation=ref.get("situation", ""),
            title=ref.get("title", ""),
            url=ref.get("document_url", ""),
        )
        logger.error(f"{self.name}: {ref.get('title', 'listing')}: {message}")

    async def run(self) -> RunResult:
        self._result = RunResult()
        started = time.perf_counter()
        self._run_id = await self.store.start_run(self.name)
        scope = f"{self.name}:{self._run_id}"
        token = usage_scope.set(scope)
        with logger.contextualize(scraper=self.name):
            try:
                await self.setup()
                # Overwrite re-scrapes everything; each save replaces the old row.
                await self._scrape(set() if self.overwrite else await self.store.keys())
            except Exception as exc:
                await self.error({}, exc)
            finally:
                self._result.seconds = time.perf_counter() - started
                stats = {
                    **self._result.stats(),
                    "years": f"{self.years.start}-{self.years.stop - 1}",
                    "workers": self.workers,
                    "http": dict(self.http.stats),
                    "browser_navigations": self.browser.navigations,
                }
                try:
                    await self.store.finish_run(
                        self._run_id,
                        self._result.docs,
                        self._result.errors,
                        self.ocr.usage_for(scope) if self.ocr else {},
                        stats,
                    )
                finally:
                    usage_scope.reset(token)
        return self._result

    async def _scrape(self, keys: set[tuple[str, str]]) -> None:
        queue: asyncio.Queue[DocRef | None] = asyncio.Queue(maxsize=self.workers * 2)
        pending: set[tuple[str, str]] = set()
        started = time.perf_counter()

        async def produce() -> None:
            # One queue for all years, so workers never wait at a year boundary.
            for year in [None] if self.yearless else self.years:
                try:
                    async for ref in self.list_documents(year):
                        ref = dict(ref)
                        ref.setdefault("year", year)
                        if ref["year"] is None:
                            await self.error(ref, ValueError("Listing row has no year"))
                            continue
                        if self.yearless and ref["year"] not in self.years:
                            continue
                        ref["title"] = clean_title(ref["title"])
                        key = (ref["document_url"], ref["title"])
                        if key in keys or key in pending:
                            self._result.skipped += 1
                            continue
                        pending.add(key)
                        self._result.listed += 1
                        await queue.put(ref)
                except Exception as exc:
                    await self.error({"year": year}, exc)
            self._result.listing_seconds = time.perf_counter() - started
            for _ in range(self.workers):
                await queue.put(None)

        async def consume() -> None:
            while (ref := (await queue.get())) is not None:
                key = (ref["document_url"], ref["title"])
                fetch_started = time.perf_counter()
                try:
                    doc = await self.fetch(dict(ref))
                    if doc is None:
                        raise ValueError("Fetch produced no document")
                    await self.store.save(doc, ref_url=key[0], ref_title=key[1])
                    self._result.docs += 1
                    self._result.fetch_seconds.append(
                        time.perf_counter() - fetch_started
                    )
                    keys.add(key)
                    del doc
                except Exception as exc:
                    await self.error(ref, exc)
                finally:
                    pending.discard(key)
                    queue.task_done()

        async with asyncio.TaskGroup() as group:
            group.create_task(produce())
            for _ in range(self.workers):
                group.create_task(consume())

    async def paginate(
        self,
        fetch_page: Callable[[int], Awaitable[list[DocRef]]],
        total_pages: int,
        start: int = 1,
    ) -> AsyncIterator[DocRef]:
        """Fetch pages in bounded batches; a failed page is recorded and skipped."""
        for first in range(start, total_pages + 1, self.workers):
            tasks = [
                asyncio.create_task(fetch_page(page))
                for page in range(first, min(first + self.workers, total_pages + 1))
            ]
            try:
                pages = await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            for page, refs in enumerate(pages, first):
                if isinstance(refs, Exception):
                    await self.error({"title": f"listing page {page}"}, refs)
                    continue
                if isinstance(refs, BaseException):
                    raise refs
                for ref in refs:
                    yield ref

    async def download(self, url: str) -> tuple[str, bytes, str]:
        response = await self.http.request(url)
        sniffed = sniff_extension(response.content)
        declared = detect_extension(response.content_type, response.filename)
        if (declared == ".pdf" or url.lower().endswith(".pdf")) and sniffed != ".pdf":
            raise ValueError(f"Expected PDF from {url}")
        ext = sniffed or declared
        markdown = await bytes_to_markdown(
            response.content,
            response.headers.get("Content-Type", response.content_type),
            url,
            self.http,
            self.ocr,
        )
        return (markdown, response.content, ext)

    async def markdown(self, *, html: str, base_url: str | None = None) -> str:
        async def fetch_image(url: str) -> bytes:
            return (await self.http.request(url)).content

        return await html_to_markdown(html, base_url or self.base_url, fetch_image)

    async def fetch_pdf(self, ref: DocRef) -> Document:
        url = urljoin(self.base_url, ref.get("_pdf_link") or ref["document_url"])
        markdown, raw, ext = await self.download(url)
        return Document(
            **{
                **ref,
                "document_url": url,
                "text_markdown": markdown,
                "raw": raw,
                "raw_ext": ext,
            }
        )

    async def fetch_html(
        self, ref: DocRef, html: str, url: str, raw: bytes, raw_ext: str = ".mhtml"
    ) -> Document:
        """Build a Document from page HTML; raw is the browser MHTML or the HTTP body (".html")."""
        return Document(
            **{
                **ref,
                "document_url": url,
                "text_markdown": await self.markdown(html=html, base_url=url),
                "raw": raw,
                "raw_ext": raw_ext,
            }
        )
