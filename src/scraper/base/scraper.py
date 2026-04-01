from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Coroutine

import aiofiles
import aiohttp
import urllib3
from bs4 import BeautifulSoup, Tag
from loguru import logger
from src.config import LOG_DIR, SAVE_DIR, STATE_LEGISLATION_SAVE_DIR
from src.database.saver import FileSaver, aggregate_types_summary
from src.services.browser.playwright import BrowserService

if TYPE_CHECKING:
    pass

from tqdm import tqdm

from src.scraper.base.converter import (
    MarkdownConverter,
    clean_norm_soup,
    valid_markdown,
)
from src.scraper.base.browser_mixin import BrowserMixin
from src.scraper.base.persistence import PersistenceManager
from src.services.proxy.service import ProxyService
from src.services.request.service import RequestService

if TYPE_CHECKING:
    from src.services.ocr.config import LLMConfig
    from src.services.ocr.llm import LLMOCRService
from src.scraper.base.schemas import ScrapedDocument

# suppress urllib3 InsecureRequestWarning (verify=False is used intentionally for some gov sites)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# suppress httpx and urllib3 logging
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)


YEAR_START = 1808
DEFAULT_VALID_SITUATION = "Não consta revogação expressa"
DEFAULT_INVALID_SITUATION = "Revogada"

DEFAULT_LLM_PROMPT = """Você é um especialista de extração e formatação de textos jurídicos. O documento fornecido é uma norma jurídica. Extraia todo o conteúdo principal e formate-o em Markdown, seguindo rigorosamente estas regras:

*   **Fidelidade Absoluta (CRÍTICO):** Transcreva o texto exata e literalmente como aparece no documento. Não altere nenhuma palavra, não corrija gramática e não modifique a pontuação. Preservar a exatidão legal é essencial. Não introduza nenhuma palavra ou frase que não esteja presente no documento original.
*   **Estrutura Legal:** Respeite rigorosamente a numeração e a hierarquia legislativa: títulos, capítulos, seções, artigos (Art.), parágrafos (§), incisos (algarismos romanos: I, II, III) e alíneas (letras: a, b, c).
*   **Formatação Markdown:**
    * Use títulos Markdown (`##` ou `###`) para títulos, capítulos e seções.
    * Aplique **negrito** ou *itálico* exatamente onde o texto original estiver em destaque.
    * Caso haja tabelas, preserve a formatação tabular usando a sintaxe de tabelas do Markdown.
    * Se houver uma *ementa* (o bloco de texto que resume a norma, geralmente recuado à direita no topo), formate-a como citação (usando `>` antes do bloco).
*   **Continuidade:** O texto pode ser continuação de uma página anterior ou terminar de forma abrupta. Extraia desde a primeira palavra válida até a última, mesmo que comece ou termine no meio de uma frase.
*   **Limpeza e Exclusões (ATENÇÃO):** Ignore cabeçalhos (headers), rodapés (footers), números de página, datas de impressão ou marcas d'água. **Exclua obrigatoriamente qualquer nota editorial ou aviso legal que inicie com "Este texto não substitui..." ou "Esse texto não substitui..." ou outras notas e observações similares, independentemente de onde apareçam na página.**

Nota: o documento recebido pode estar em branco ou inválido. Nesses casos, retorne uma string vazia ("") e nada além disso.

Retorne **EXCLUSIVAMENTE** o conteúdo extraído. Não inclua a tag ```markdown, não inclua saudações, introduções ou qualquer explicação adicional, antes ou depois do texto."""


from src.scraper.base.summary_utils import (  # noqa: E402
    _build_llm_usage_summary,
    _build_run_summary,
    _coerce_summary_runs,
    flatten_results,
    merge_context,
)


class BaseScraper(BrowserMixin):
    """Base class for legislation scrapers (async)"""

    _iterate_situations: bool = False
    _mhtml_wait_until: str = "load"
    _mhtml_timeout: int = 60_000  # 60s per attempt × 5 attempts = up to 5 minutes total
    _pymupdf_image_size_limit: float = 0.1

    def __init__(
        self,
        base_url: str,
        name: str,
        types: list | dict,
        situations: list | dict,
        year_start: int = YEAR_START,
        year_end: int = datetime.now().year,
        docs_save_dir: Path = SAVE_DIR,
        llm_config: LLMConfig | None = None,
        llm_prompt: str = DEFAULT_LLM_PROMPT,
        use_browser: bool = False,
        multiple_pages: bool = False,
        headless: bool = True,
        proxy_config: dict | None = None,
        rps: float = 10,
        max_workers: int = 50,
        max_retries: int = 6,
        verbose: bool = False,
        overwrite: bool = False,
        disable_cookies: bool = False,
        proxy_timeout: int = 15,
    ):
        self.base_url = base_url
        self.name = name
        self.types = types
        self.situations = situations
        self.year_start = year_start
        self.year_end = year_end
        self.docs_save_dir = Path(docs_save_dir) / name.upper()
        self.log_dir = LOG_DIR / self.name
        self.llm_config = llm_config
        self.llm_prompt = llm_prompt
        self._runtime_log_sink_id: int | None = None
        self._initialize_runtime_log_sink()

        try:
            with logger.contextualize(scraper=self.name):
                self.proxy_service = None
                if proxy_config:
                    self.proxy_service = ProxyService(
                        config=proxy_config, verbose=verbose
                    )

                self.request_service = RequestService(
                    rps=rps,
                    verbose=verbose,
                    proxy_service=self.proxy_service,
                    max_retries=max_retries,
                    disable_cookies=disable_cookies,
                    proxy_timeout=proxy_timeout,
                )
                self.ocr_service: LLMOCRService | None = None
                if self.llm_config:
                    from src.services.ocr.llm import LLMOCRService

                    self.ocr_service = LLMOCRService(
                        prompt=self.llm_prompt,
                        llm_config=self.llm_config,
                        verbose=verbose,
                    )
                self.use_browser = use_browser
                self.multiple_pages = multiple_pages
                self.headless = headless
                self.verbose = verbose
                self.overwrite = overwrite
                self.rps = rps
                self.max_workers = max_workers
                self.years = list(range(self.year_start, self.year_end + 1))
                self.count = 0
                self.error_count = 0
                self._scrape_start: float | None = None
                self._types_summary: dict[str, dict] = {}
                self._mhtml_browser: BrowserService | None = None

                self.browser_service: BrowserService | None = (
                    BrowserService(
                        multiple_pages=multiple_pages,
                        max_workers=max_workers,
                        headless=headless,
                        verbose=verbose,
                        owner_class_name=self.__class__.__name__,
                    )
                    if use_browser
                    else None
                )
                self.saver: FileSaver | None = None
                self._scraped_keys: set[tuple[str, str]] = set()
                self._overwrite_reset_years: set[int] = set()

                # Composed delegates
                self._converter = MarkdownConverter(self)
                self._persister = PersistenceManager(self)

                self._initialize_saver()
                self._log_initialization()
        except Exception:
            with logger.contextualize(scraper=self.name):
                logger.exception(f"{self.__class__.__name__} initialization failed")
            self._remove_runtime_log_sink()
            raise

    @property
    def runtime_log_path(self) -> Path:
        return self.log_dir / "runtime.log"

    def _initialize_runtime_log_sink(self) -> None:
        self.runtime_log_path.parent.mkdir(parents=True, exist_ok=True)
        scraper_name = self.name
        self._runtime_log_sink_id = logger.add(
            self.runtime_log_path,
            level="DEBUG",
            mode="w",
            enqueue=True,
            encoding="utf-8",
            filter=lambda record, scraper_name=scraper_name: (
                record["extra"].get("scraper") == scraper_name
            ),
        )

    def _remove_runtime_log_sink(self) -> None:
        sink_id = getattr(self, "_runtime_log_sink_id", None)
        if sink_id is None:
            return
        logger.remove(sink_id)
        self._runtime_log_sink_id = None

    def _log_initialization(self):
        init_log = (
            f"{self.__class__.__name__} initialized | "
            f"name={self.name} | base_url={self.base_url} | "
            f"years={self.year_start}-{self.year_end} | "
            f"types={len(self.types) if self.types else 0} | "
            f"situations={len(self.situations) if self.situations else 0} | "
            f"save_dir={self.docs_save_dir} | "
            f"log_dir={self.log_dir} | "
            f"use_browser={self.use_browser} | "
            f"rps={self.rps} | "
            f"max_workers={self.max_workers} | "
            f"max_retries={self.request_service.max_retries} | "
            f"verbose={self.verbose} | "
            f"overwrite={self.overwrite} | "
            f"mhtml_wait_until={self._mhtml_wait_until} | "
            f"mhtml_timeout={self._mhtml_timeout} | "
        )

        if self.llm_config:
            init_log += f"llm_config={self.llm_config} | "

        logger.debug(init_log)

    @property
    def default_situation(self) -> str:
        """Return the first configured situation, or 'Não consta' if none defined."""
        return next(iter(self.situations), "Não consta")

    def _initialize_saver(self):
        """Initialize saver class. Called automatically at end of __init__."""
        self.saver = FileSaver(
            self.docs_save_dir,
            log_dir=self.log_dir,
            verbose=self.verbose,
            max_workers=self.max_workers,
        )

    # ------------------------------------------------------------------
    # HTTP requests (async via RequestService)
    # ------------------------------------------------------------------

    async def _fetch_soup_with_retry(self, url: str) -> BeautifulSoup:
        """Fetch URL and return BeautifulSoup, raising on failure.

        Retries are handled internally by ``RequestService``.
        """
        soup = await self.request_service.get_soup(url)
        if not soup:
            reason = soup.reason
            raise RuntimeError(f"Failed to fetch {url}: {reason}")
        return soup

    # ------------------------------------------------------------------
    # Markdown utilities — delegate to MarkdownConverter
    # ------------------------------------------------------------------

    def _clean_norm_soup(
        self,
        soup: BeautifulSoup | Tag,
        *,
        remove_disclaimers: bool = True,
        unwrap_links: bool = True,
        remove_images: bool = False,
        remove_empty_tags: bool = True,
        unwrap_fonts: bool = False,
        strip_styles: bool = False,
        remove_style_tags: bool = False,
        remove_script_tags: bool = False,
    ) -> BeautifulSoup | Tag:
        return clean_norm_soup(
            soup,
            remove_disclaimers=remove_disclaimers,
            unwrap_links=unwrap_links,
            remove_images=remove_images,
            remove_empty_tags=remove_empty_tags,
            unwrap_fonts=unwrap_fonts,
            strip_styles=strip_styles,
            remove_style_tags=remove_style_tags,
            remove_script_tags=remove_script_tags,
        )

    async def _html_to_markdown(self, html_content: str) -> str:
        return await self._converter.html_to_markdown(html_content)

    async def _bytes_to_markdown(
        self,
        body: bytes,
        filename: str = "document.pdf",
        content_type: str = "",
    ) -> str:
        return await self._converter.bytes_to_markdown(body, filename, content_type)

    async def _get_markdown(
        self,
        url: str | None = None,
        response: aiohttp.ClientResponse | None = None,
        stream: BytesIO | None = None,
        html_content: str | None = None,
        filename: str | None = None,
        base_url: str | None = None,
    ) -> str:
        return await self._converter.get_markdown(
            url=url,
            response=response,
            stream=stream,
            html_content=html_content,
            filename=filename,
            base_url=base_url,
        )

    # ------------------------------------------------------------------
    # Document file saving & resume — delegate to persistence / converter
    # ------------------------------------------------------------------

    async def _response_to_markdown(
        self,
        body: bytes,
        filename: str | None = None,
        content_type: str = "",
    ) -> str:
        return await self._converter.response_to_markdown(body, filename, content_type)

    async def _download_and_convert(
        self,
        url: str,
    ) -> tuple[str, bytes, str]:
        return await self._converter.download_and_convert(url)

    async def _process_doc(
        self,
        doc_info: dict,
        url: str,
        text_markdown: str,
        raw_content: bytes,
        content_ext: str,
        error_prefix: str = "Invalid content",
    ) -> ScrapedDocument | None:
        """Validate markdown and populate *doc_info*, or save an error."""
        valid, reason = valid_markdown(text_markdown)
        if not valid:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                norm_type=doc_info.get("type", ""),
                html_link=url,
                error_message=f"{error_prefix}: {reason}",
            )
            return None

        # Merge url/text_markdown/raw_content into a copy of doc_info so that
        # any keys already present in doc_info (e.g. document_url) are
        # overridden cleanly rather than causing a "multiple values" TypeError.
        merged = {
            **doc_info,
            "text_markdown": text_markdown,
            "document_url": url,
            "raw_content": raw_content,
            "content_extension": content_ext,
        }
        return ScrapedDocument(**merged)

    async def _process_pdf_doc(
        self,
        doc_info: dict,
        pdf_link_key: str = "pdf_link",
    ) -> ScrapedDocument | None:
        """Download a PDF, convert to markdown, validate, and populate doc_info."""
        pdf_link = doc_info.pop(pdf_link_key, "") or doc_info.get("document_url", "")
        title = doc_info.get("title", "")

        if self._is_already_scraped(pdf_link, title):
            return None

        text_markdown, raw_content, content_ext = await self._download_and_convert(
            pdf_link
        )
        return await self._process_doc(
            doc_info,
            pdf_link,
            text_markdown,
            raw_content,
            content_ext,
            error_prefix="Failed to process PDF",
        )

    async def _process_html_doc(
        self,
        doc_info: dict,
        html_content: str,
        url: str,
        mhtml_content: bytes,
    ) -> ScrapedDocument | None:
        """Convert HTML to markdown, validate, and populate doc_info.

        Stores *mhtml_content* as the raw file (`.mhtml` extension).
        """
        text_markdown = await self._get_markdown(
            html_content=html_content, base_url=url
        )
        return await self._process_doc(
            doc_info,
            url,
            text_markdown,
            mhtml_content,
            ".mhtml",
            error_prefix="Invalid markdown",
        )

    async def _get_docs_links(self, *args, **kwargs) -> list[dict] | None:
        """Template method: return a list of document metadata dicts from a listing page."""
        raise NotImplementedError(
            "This method should be implemented in the child class."
        )

    async def _get_doc_data(
        self, *args, **kwargs
    ) -> "ScrapedDocument" | list["ScrapedDocument"] | None:
        """Template method: fetch and parse a single document's content."""
        raise NotImplementedError(
            "This method should be implemented in the child class."
        )

    # ------------------------------------------------------------------
    # Persistence — delegate to PersistenceManager
    # ------------------------------------------------------------------

    async def _save_doc_result(self, doc_result: dict) -> dict | None:
        return await self._persister.save_doc_result(doc_result)

    async def _load_scraped_keys(self, year: int) -> None:
        return await self._persister.load_scraped_keys(year)

    def _is_already_scraped(self, document_url: str, title: str = "") -> bool:
        return self._persister.is_already_scraped(document_url, title)

    async def _save_doc_error(
        self,
        *,
        title: str,
        year: str | int = "",
        situation: str = "",
        norm_type: str = "",
        html_link: str = "",
        error_message: str = "Document processing failed",
        **extra,
    ) -> None:
        return await self._persister.save_doc_error(
            title=title,
            year=year,
            situation=situation,
            norm_type=norm_type,
            html_link=html_link,
            error_message=error_message,
            **extra,
        )

    # ------------------------------------------------------------------
    # Results gathering
    # ------------------------------------------------------------------

    async def _with_save(self, coro, context: dict):
        result = await coro
        if result is None:
            return None

        is_list = isinstance(result, list)
        items = result if is_list else [result]
        saved = []
        for r in items:
            doc = merge_context(r, context)
            s = await self._save_doc_result(doc)
            if s is None:
                logger.warning(
                    f"Save failed for '{doc.get('title', '?')}', discarding result"
                )
                continue
            saved.append(s)
        if not saved:
            return None
        return saved if is_list else saved[0]

    async def _save_gather_errors(
        self,
        results: list,
        context: dict,
        desc: str = "",
    ) -> list:
        ctx = {"year": "", "type": "", "situation": "", **context}
        valid = []
        for result in results:
            if isinstance(result, Exception):
                self.error_count += 1
                logger.error(f"{desc} | Error: {result}")
                if self.saver:
                    error_data = {
                        "title": desc or "Unknown",
                        "html_link": "",
                        **ctx,
                    }
                    await self.saver.save_error(error_data, error_message=str(result))
                continue
            if result is None:
                continue
            valid.append(result)
        return valid

    async def _gather_results(
        self,
        tasks: list,
        context: dict | None = None,
        desc: str = "",
    ) -> list:
        if not tasks:
            return []

        if self.verbose:
            progress = tqdm(total=len(tasks), desc=desc or "Gathering")
            wrapped_tasks = [asyncio.create_task(task) for task in tasks]
            for task in wrapped_tasks:
                task.add_done_callback(lambda _: progress.update())
            try:
                results = await asyncio.gather(*wrapped_tasks, return_exceptions=True)
            finally:
                progress.close()
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        return await self._save_gather_errors(results, context or {}, desc)

    async def _process_documents(
        self,
        documents: list,
        *,
        year: int,
        norm_type: str,
        situation: str = "",
        desc: str = "",
        doc_data_fn=None,
        doc_data_kwargs: dict | None = None,
    ) -> list[dict]:
        """Wrap each document through _get_doc_data -> _with_save -> _gather_results."""
        ctx = {"year": year, "type": norm_type, "situation": situation}
        fn = doc_data_fn or self._get_doc_data
        kw = doc_data_kwargs or {}
        # Enrich each doc with context fields (year/type/situation) so that
        # ScrapedDocument validation succeeds when _get_doc_data calls
        # _process_pdf_doc/_process_html_doc.  Doc-specific values take
        # precedence over context defaults (dict merge order: ctx first, then doc).
        enriched = [{**ctx, **doc} for doc in documents]
        tasks = [self._with_save(fn(doc, **kw), ctx) for doc in enriched]
        results = await self._gather_results(
            tasks,
            context=ctx,
            desc=desc or f"{self.name} | {norm_type}",
        )
        logger.debug(
            f"Finished scraping for Year: {year} | Type: {norm_type} "
            f"| Situation: {situation} | Results: {len(results)}"
        )
        return results

    async def _fetch_all_pages(
        self,
        make_task: Callable[[int], Coroutine],
        total_pages: int,
        *,
        start_page: int = 2,
        context: dict | None = None,
        desc: str = "",
    ) -> list:
        """Fetch pages ``start_page``..``total_pages`` concurrently and flatten."""
        if total_pages < start_page:
            return []
        tasks = [make_task(page) for page in range(start_page, total_pages + 1)]
        results = await self._gather_results(tasks, context=context, desc=desc)
        return flatten_results(results)

    async def _paginate_until_end(
        self,
        *,
        make_task: Callable[[int], Coroutine[Any, Any, tuple[list[dict], bool]]],
        context: dict,
        desc: str = "",
        initial_batch: int = 1,
        batch_growth: int | None = None,
        max_batch: int | None = None,
        max_iterations: int = 1000,
    ) -> list[dict]:
        """Fetch pages in growing batches until a page signals end-of-results."""
        batch = initial_batch
        growth = batch_growth if batch_growth is not None else self.max_workers
        cap = max_batch or self.max_workers
        page = 1
        all_docs: list[dict] = []
        iterations = 0

        while iterations < max_iterations:
            tasks = [make_task(p) for p in range(page, page + batch)]
            results = await self._gather_results(tasks, context=context, desc=desc)

            reached_end = False
            batch_docs: list[dict] = []
            for docs, ended in results:
                if ended:
                    reached_end = True
                if docs:
                    batch_docs.extend(docs)

            all_docs.extend(batch_docs)
            if reached_end or not batch_docs:
                break

            page += batch
            batch = min(batch + growth, cap)
            iterations += 1
        else:
            logger.warning(
                f"_paginate_until_end: reached max_iterations={max_iterations} "
                f"without end signal — stopping ({desc})"
            )

        return all_docs

    async def _before_scrape(self) -> None:
        """Hook called once before year iteration begins. Optional to implement in child class."""
        pass

    async def _scrape_type(self, norm_type: str, norm_type_id, year: int) -> list[dict]:
        """Scrape all documents of a single type for a year."""
        raise NotImplementedError(
            "This method should be implemented in the child class."
        )

    async def _scrape_situation_type(
        self,
        year: int,
        situation: str,
        situation_id,
        norm_type: str,
        norm_type_id,
    ) -> list[dict]:
        """Scrape all documents of a single situation+type for a year."""
        raise NotImplementedError(
            "Scrapers with _iterate_situations = True must implement "
            "_scrape_situation_type."
        )

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape norms for a specific year."""
        if self._iterate_situations and self.situations:
            sit_items = (
                self.situations.items()
                if isinstance(self.situations, dict)
                else [(s, s) for s in self.situations]
            )
            type_items = (
                self.types.items()
                if isinstance(self.types, dict)
                else [(t, None) for t in self.types]
            )
            tasks = [
                self._scrape_situation_type(year, sit, sit_id, nt, nt_id)
                for sit, sit_id in sit_items
                for nt, nt_id in type_items
            ]
        elif isinstance(self.types, dict):
            tasks = [
                self._scrape_type(nt, nt_id, year) for nt, nt_id in self.types.items()
            ]
        else:
            tasks = [self._scrape_type(nt, None, year) for nt in self.types]

        valid = await self._gather_results(
            tasks,
            context={"year": year, "type": "", "situation": ""},
            desc=f"{self.name} | Year {year}",
        )

        return flatten_results(valid)

    # ------------------------------------------------------------------
    # Main scrape flow
    # ------------------------------------------------------------------

    def _track_results(self, results: list[dict]) -> None:
        """Update lightweight summary counters from a batch of results."""
        aggregate_types_summary(results, self._types_summary)

    async def scrape(self) -> int:
        """Scrape data from all years (async).

        Returns:
            Total number of documents scraped.
        """
        with logger.contextualize(scraper=self.name):
            try:
                if not self.saver:
                    raise RuntimeError(
                        "Saver is not initialized. Call _initialize_saver() in the child class __init__ method."
                    )

                if self.use_browser:
                    await self.initialize_playwright()

                await self._before_scrape()

                self._scrape_start = time.time()

                logger.info(f"Starting from {self.year_start}")

                years_progress = tqdm(
                    self.years,
                    desc=f"{self.__class__.__name__} | Years",
                )

                for year in years_progress:
                    years_progress.set_description(
                        f"{self.__class__.__name__} | Year: {year}"
                    )
                    await self._load_scraped_keys(year)

                    year_results = await self._scrape_year(year)
                    if year_results:
                        self._track_results(year_results)
                        self.count += len(year_results)

                    if self.saver:
                        await self.saver.flush(year)

                await self._save_summary()
                return self.count
            except Exception:
                logger.exception(f"{self.__class__.__name__} scrape failed")
                raise

    async def _save_summary(self) -> None:
        """Write a summary JSON file with final scraping statistics."""
        if not self.saver:
            return

        await self.saver.flush_all()

        elapsed = time.time() - (self._scrape_start or time.time())
        llm_usage_by_model = self.ocr_service.usage_stats if self.ocr_service else {}
        completed_at = datetime.now().isoformat()
        run_summary = _build_run_summary(
            scraper=self.__class__.__name__,
            year_start=self.year_start,
            year_end=self.year_end,
            total_documents=self.count,
            total_errors=self.error_count,
            elapsed_seconds=elapsed,
            completed_at=completed_at,
            types_summary=self._types_summary,
            llm_usage=_build_llm_usage_summary(llm_usage_by_model),
        )

        dataset_summary = await self.saver.get_dataset_summary()

        summary_path = Path(self.saver.save_dir) / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)

        existing_summary: dict[str, Any] = {}
        if summary_path.exists():
            try:
                async with aiofiles.open(summary_path, "r", encoding="utf-8") as f:
                    existing_summary = json.loads(await f.read())
                if not isinstance(existing_summary, dict):
                    logger.warning(
                        f"Invalid summary format in {summary_path}; resetting history."
                    )
                    existing_summary = {}
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning(
                    f"Could not read existing summary history from {summary_path}: {exc}"
                )

        runs = _coerce_summary_runs(existing_summary)
        runs.append(run_summary)

        summary = {
            "scraper": run_summary["scraper"],
            "year_start": dataset_summary["year_start"],
            "year_end": dataset_summary["year_end"],
            "total_documents": dataset_summary["total_documents"],
            "types_summary": dataset_summary["types_summary"],
            "total_errors": run_summary["total_errors"],
            "elapsed_seconds": run_summary["elapsed_seconds"],
            "elapsed_human": run_summary["elapsed_human"],
            "completed_at": run_summary["completed_at"],
            "llm_usage": run_summary["llm_usage"],
            "runs": runs,
        }

        async with aiofiles.open(summary_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(summary, ensure_ascii=False, indent=2))

        done_log = (
            f"{run_summary['scraper']} | Done — {run_summary['total_documents']} docs "
            f"this run, {summary['total_documents']} total saved, "
            f"{run_summary['total_errors']} errors, {run_summary['elapsed_human']}"
        )
        done_log += f", {run_summary['llm_usage']['human']}"

        logger.info(done_log)
        print(done_log)

    async def cleanup(self):
        """Clean up aiohttp session, Playwright browser, etc.

        Safe to call multiple times (idempotent).
        """
        if getattr(self, "_cleaned_up", False):
            return
        self._cleaned_up = True
        try:
            with logger.contextualize(scraper=self.name):
                try:
                    if hasattr(self, "request_service"):
                        await self.request_service.cleanup()
                    if self.browser_service:
                        await self.browser_service.cleanup()
                    mhtml_browser: BrowserService | None = getattr(
                        self, "_mhtml_browser", None
                    )
                    if mhtml_browser is not None:
                        await mhtml_browser.cleanup()
                    if self.saver:
                        await self.saver.cleanup()
                except Exception:
                    logger.exception(f"{self.__class__.__name__} cleanup failed")
                    raise
        finally:
            self._remove_runtime_log_sink()


class StateScraper(BaseScraper):
    """Convenience base for state-level legislation scrapers.

    Automatically applies ``STATE_LEGISLATION_SAVE_DIR`` as the default
    ``docs_save_dir`` when the environment variable is set.
    """

    def __init__(self, *args, **kwargs):
        if STATE_LEGISLATION_SAVE_DIR:
            kwargs.setdefault("docs_save_dir", STATE_LEGISLATION_SAVE_DIR)
        super().__init__(*args, **kwargs)

    def _normalize_type(
        self,
        raw_type: str,
        known_types: dict | list | None = None,
        aliases: dict | None = None,
        fallback: str = "",
    ) -> str:
        """Normalize a raw type string to a canonical type name."""
        types = known_types if known_types is not None else self.types
        cleaned = re.sub(r"\s+", " ", str(raw_type or "")).strip()
        if not cleaned:
            return fallback

        lower = cleaned.casefold()

        if aliases:
            mapped = aliases.get(lower)
            if mapped is not None:
                return mapped

        type_keys = list(types.keys()) if isinstance(types, dict) else list(types)
        for key in type_keys:
            if lower == key.casefold():
                return key

        return fallback or cleaned

    async def _fetch_and_save_constitution(
        self,
        url: str,
        title: str,
        year: int,
        **extra,
    ) -> ScrapedDocument | None:
        """Download a state constitution, convert to markdown, save, and track."""
        if self._is_already_scraped(url, title):
            logger.debug(f"Constitution already scraped, skipping: {title}")
            return None

        text_markdown, raw_content, content_ext = await self._download_and_convert(url)
        if not text_markdown or not text_markdown.strip():
            logger.error(f"Failed to get markdown for constitution: {title}")
            return None

        # If the download returned HTML, capture MHTML for archival
        if content_ext and content_ext.lstrip(".") == "html":
            try:
                raw_content = await self._capture_mhtml(url)
                content_ext = ".mhtml"
            except Exception as exc:
                logger.warning(f"MHTML capture failed for constitution {url}: {exc}")

        doc_info = {
            "year": year,
            "type": "Constituição Estadual",
            "title": title,
            "situation": DEFAULT_VALID_SITUATION,
            "text_markdown": text_markdown,
            "document_url": url,
            "raw_content": raw_content,
            "content_extension": content_ext,
            **extra,
        }
        doc_obj = ScrapedDocument(**doc_info)

        saved = await self._save_doc_result(doc_obj)
        if saved is not None:
            # _save_doc_result currently returns dict (from PersistenceManager)
            # but we want to return ScrapedDocument from this method.
            # Let's check what _save_doc_result returns.
            pass
        self._track_results([doc_info])
        self.count += 1
        logger.debug(f"Fetched constitution: {title}")
        return doc_obj
