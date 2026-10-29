"""Bounded listing → fetch → save pipeline shared by every scraper."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any, ClassVar
from urllib.parse import urljoin

from loguru import logger

from src.config import LOG_DIR
from src.core.browser import BrowserPool
from src.core.convert import bytes_to_markdown, detect_extension, html_to_markdown
from src.core.document import DocRef, Document
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
        raise NotImplementedError
        yield

    async def fetch(self, ref: DocRef) -> Document | list[Document] | None:
        raise NotImplementedError

    async def error(self, ref: DocRef, exc: Exception) -> None:
        self._result.errors += 1
        await self.store.record_error(
            run_id=self._run_id,
            message=str(exc),
            year=ref.get("year"),
            type=ref.get("type", ""),
            situation=ref.get("situation", ""),
            title=ref.get("title", ""),
            url=ref.get("document_url", ""),
        )
        logger.error(f"{self.name}: {ref.get('title', 'listing')}: {exc}")

    async def run(self) -> RunResult:
        self._result = RunResult()
        self._run_id = await self.store.start_run(self.name)
        scope = f"{self.name}:{self._run_id}"
        token = usage_scope.set(scope)
        with logger.contextualize(scraper=self.name):
            try:
                await self.setup()
                keys = await self.store.keys()
                if self.overwrite:
                    for year in self.years:
                        await self.store.reset_year(year)
                    keys = await self.store.keys()
                for year in [None] if self.yearless else self.years:
                    await self._run_year(year, keys)
            except Exception as exc:
                await self.error({}, exc)
            finally:
                try:
                    await self.store.finish_run(
                        self._run_id,
                        self._result.docs,
                        self._result.errors,
                        self.ocr.usage_for(scope) if self.ocr else {},
                    )
                finally:
                    usage_scope.reset(token)
        return self._result

    async def _run_year(self, year: int | None, keys: set[tuple[str, str]]) -> None:
        queue: asyncio.Queue[DocRef | None] = asyncio.Queue(maxsize=self.workers * 2)
        pending: set[tuple[str, str]] = set()

        async def produce() -> None:
            try:
                async for ref in self.list_documents(year):
                    ref = dict(ref)
                    ref.setdefault("year", year)
                    if self.yearless and ref.get("year") not in self.years:
                        continue
                    key = (ref["document_url"], ref["title"])
                    if key in keys or key in pending:
                        self._result.skipped += 1
                        continue
                    pending.add(key)
                    await queue.put(ref)
            except Exception as exc:
                await self.error({"year": year}, exc)
            for _ in range(self.workers):
                await queue.put(None)

        async def consume() -> None:
            while (ref := (await queue.get())) is not None:
                key = (ref["document_url"], ref["title"])
                try:
                    result = await self.fetch(dict(ref))
                    docs = (
                        result
                        if isinstance(result, list)
                        else [result]
                        if result is not None
                        else []
                    )
                    if not docs:
                        raise ValueError("Fetch produced no document")
                    await self.store.save_many(docs, ref_url=key[0], ref_title=key[1])
                    self._result.docs += len(docs)
                    keys.add(key)
                    del result, docs
                except Exception as exc:
                    await self.error(ref, exc)
                finally:
                    pending.discard(key)
                    queue.task_done()

        async with asyncio.TaskGroup() as group:
            group.create_task(produce())
            for _ in range(self.workers):
                group.create_task(consume())

    def iter_type_items(self) -> list[tuple[str, Any, str, Any]]:
        types = (
            self.types.items()
            if isinstance(self.types, dict)
            else ((name, name) for name in self.types)
        )
        situations = (
            self.situations.items()
            if isinstance(self.situations, dict)
            else ((name, name) for name in self.situations)
        )
        situations = list(situations) or [(DEFAULT_VALID_SITUATION, None)]
        return [
            (name, value, situation, sid)
            for name, value in types
            for situation, sid in situations
        ]

    async def paginate(
        self,
        fetch_page: Callable[[int], Awaitable[list[DocRef]]],
        total_pages: int,
        start: int = 1,
    ) -> AsyncIterator[DocRef]:
        for first in range(start, total_pages + 1, self.workers):
            tasks = [
                asyncio.create_task(fetch_page(page))
                for page in range(first, min(first + self.workers, total_pages + 1))
            ]
            try:
                pages = await asyncio.gather(*tasks)
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            for refs in pages:
                for ref in refs:
                    yield ref

    async def download(self, url: str) -> tuple[str, bytes, str]:
        response = await self.http.request(url)
        ext = detect_extension(response.content_type, response.filename)
        if ext == ".pdf" or url.lower().endswith(".pdf"):
            if not response.content.startswith(b"%PDF"):
                raise ValueError(f"Expected PDF from {url}")
        if response.content.startswith(b"%PDF"):
            ext = ".pdf"
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
        url = urljoin(self.base_url, ref.get("pdf_link") or ref["document_url"])
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
        self, ref: DocRef, html: str, url: str, mhtml: bytes
    ) -> Document:
        return Document(
            **{
                **ref,
                "document_url": url,
                "text_markdown": await self.markdown(html=html, base_url=url),
                "raw": mhtml,
                "raw_ext": ".mhtml",
            }
        )
