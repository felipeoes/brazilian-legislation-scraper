import asyncio
import json
import logging
import os
import string
import time
import re

import aiofiles
import aiohttp
import urllib3

from io import BytesIO
from os import environ
from datetime import datetime
from typing import cast
from bs4 import BeautifulSoup
from playwright.async_api import Page
from src.services.browser.playwright import BrowserService
from loguru import logger
from tqdm import tqdm
from tqdm.asyncio import tqdm as async_tqdm
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from markitdown import MarkItDown
from pathlib import Path
from src.database.saver import ERROR_LOG_DIR, FileSaver
from src.utils import clean_md_tag
from src.scraper.base.concurrency import run_in_thread
from src.services.proxy.service import ProxyService
from src.services.request.service import FailedRequest, RequestService
from src.services.ocr.llm import LLMOCRService

# suppress urllib3 InsecureRequestWarning (verify=False is used intentionally for some gov sites)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# suppress httpx and urllib3 logging
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)


YEAR_START = 1808
DEFAULT_VALID_SITUATION = "Não consta revogação expressa"
DEFAULT_INVALID_SITUATION = "Revogada"

STATE_LEGISLATION_SAVE_DIR = environ.get("STATE_LEGISLATION_SAVE_DIR")

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


def _format_duration(seconds: float) -> str:
    """Format seconds into a human-readable string."""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    parts = []
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)


class BaseScraper:
    """Base class for legislation scrapers (async)"""

    def __init__(
        self,
        base_url: str,
        name: str,
        types: list | dict,
        situations: list | dict,
        year_start: int = YEAR_START,
        year_end: int = datetime.now().year,
        docs_save_dir: Path = Path(environ.get("SAVE_DIR", "outputs/legislation")),
        llm_config: dict | None = None,
        llm_prompt: str = DEFAULT_LLM_PROMPT,
        use_browser: bool = False,
        multiple_pages: bool = False,
        headless: bool = True,
        proxy_config: dict | None = None,
        rps: float = 20,
        max_workers: int = 16,
        max_retries: int = 3,
        verbose: bool = False,
        overwrite: bool = False,
        disable_cookies: bool = False,
    ):
        self.base_url = base_url
        self.name = name
        self.types = types
        self.situations = situations
        self.year_start = year_start
        self.year_end = year_end
        self.docs_save_dir = Path(docs_save_dir) / name.upper()
        self.llm_config = llm_config or {}
        self.llm_prompt = llm_prompt

        self.proxy_service = None
        if proxy_config:
            self.proxy_service = ProxyService(config=proxy_config, verbose=verbose)

        self.request_service = RequestService(
            rps=rps,
            verbose=verbose,
            proxy_service=self.proxy_service,
            max_workers=max_workers,
            max_retries=max_retries,
            disable_cookies=disable_cookies,
        )
        self.ocr_service = (
            LLMOCRService(
                prompt=self.llm_prompt,
                llm_config=self.llm_config,
                verbose=verbose,
            )
            if self.llm_config
            else None
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

        self._markitdown = MarkItDown()
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
        self._initialize_saver()
        self._log_initialization()

    def _log_initialization(self):
        init_log = (
            f"{self.__class__.__name__} initialized | "
            f"name={self.name} | base_url={self.base_url} | "
            f"years={self.year_start}-{self.year_end} | "
            f"types={len(self.types) if self.types else 0} | "
            f"situations={len(self.situations) if self.situations else 0} | "
            f"save_dir={self.docs_save_dir} | "
            f"use_browser={self.use_browser} | "
            f"rps={self.rps} | "
            f"max_workers={self.max_workers} | "
        )

        if self.llm_config:
            init_log += f"llm_config={self.llm_config} | "

        if self.verbose:
            logger.info(init_log)

    # ------------------------------------------------------------------
    # Playwright (async browser automation)
    # ------------------------------------------------------------------

    @property
    def page(self) -> Page | None:
        """Active Playwright page (single-page mode)."""
        return self.browser_service.page if self.browser_service else None

    @property
    def pages(self) -> list[Page]:
        """All open Playwright pages (multi-page pool mode)."""
        return self.browser_service.pages if self.browser_service else []

    async def initialize_playwright(self):
        """Initialize Playwright browser (async — must be called from scrape())."""
        if self.browser_service:
            await self.browser_service.initialize()

    async def _get_available_page(self) -> Page:
        """Get available page from the pool (async)."""
        if not self.browser_service:
            raise RuntimeError("Browser service is not initialized.")
        return await self.browser_service.get_available_page()

    async def _release_page(self, page: Page):
        """Release page back to the pool."""
        if self.browser_service:
            self.browser_service.release_page(page)

    def _initialize_saver(self):
        """Initialize saver class. Called automatically at end of __init__."""
        error_dir = str(Path(ERROR_LOG_DIR) / self.name)
        self._mhtml_browser: BrowserService | None = None

        async def _capture_mhtml(url: str) -> bytes:
            if self._mhtml_browser is None:
                self._mhtml_browser = BrowserService(
                    headless=True,
                    multiple_pages=True,
                    max_workers=self.max_workers,
                    owner_class_name=f"{self.__class__.__name__}_mhtml",
                )
                await self._mhtml_browser.initialize()
            page = await self._mhtml_browser.get_available_page()
            try:
                return await self._mhtml_browser.capture_mhtml(url, page=page)
            finally:
                self._mhtml_browser.release_page(page)

        self.saver = FileSaver(
            self.docs_save_dir,
            error_log_dir=error_dir,
            verbose=self.verbose,
            max_workers=self.max_workers,
            mhtml_capture_fn=_capture_mhtml,
        )

    # ------------------------------------------------------------------
    # HTTP requests (async via RequestService)
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=15),
        reraise=True,
    )
    async def _browser_get_soup(
        self, url: str, page: Page | None = None
    ) -> BeautifulSoup:
        """Get BeautifulSoup object from given url using Playwright (async)."""
        if not self.browser_service:
            raise RuntimeError("Browser service is not initialized.")
        return await self.browser_service.get_soup(url, page)

    @retry(
        stop=stop_after_attempt(7),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        reraise=True,
    )
    async def _fetch_soup_with_retry(self, url: str) -> BeautifulSoup:
        """Fetch URL and return BeautifulSoup with automatic retry on failure."""
        soup = await self.request_service.get_soup(url)
        if isinstance(soup, FailedRequest):
            raise RuntimeError(f"Failed to fetch {url}")
        if not isinstance(soup, BeautifulSoup):
            raise RuntimeError(f"Failed to fetch {url}")
        return soup

    # ------------------------------------------------------------------
    # Markdown utilities
    # ------------------------------------------------------------------

    def _clean_markdown(
        self,
        text: str,
        replace: list[tuple[str, str]] | None = None,
    ) -> str:
        """Clean markdown text by removing links and applying custom replacements."""
        text = clean_md_tag(text)
        text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
        if replace:
            for find, replacement in replace:
                text = re.sub(find, replacement, text)
        return text.strip()

    @staticmethod
    def _wrap_html(content: str) -> str:
        """Wrap HTML fragment in <html><body> tags for markitdown conversion."""
        return f"<html><body>{content}</body></html>"

    async def _html_to_markdown(self, html_content: str) -> str:
        """Wrap an HTML fragment and convert it to cleaned markdown in one step."""
        wrapped = self._wrap_html(html_content)
        md = await self._convert_to_md(
            BytesIO(
                wrapped.encode("latin-1")
            ),  # using latin-1 because it's Brazilian portuguese text
            filename="document.html",
        )
        return self._clean_markdown(md)

    # ------------------------------------------------------------------
    # PDF / Image processing
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    async def _convert_to_md(
        self,
        source: BytesIO,
        filename: str = "document.html",
    ) -> str:
        """Convert a BytesIO stream to markdown via markitdown (with retry)."""
        _, ext = os.path.splitext(filename)
        if not ext:
            ext = ".html"

        try:
            source.seek(0)
            result = await run_in_thread(
                self._markitdown.convert_stream,
                source,
                file_extension=ext,
            )
            markdown = result.text_content or ""

            if not markdown or not markdown.strip():
                raise ValueError("markitdown returned empty content")

            markdown = clean_md_tag(markdown.strip())
            markdown = re.sub(r"(\n[-|]{3,}\s*)+$", "", markdown).strip()

            return markdown

        except Exception as e:
            error_msg = str(e).lower()
            if "invalid float value" in error_msg or "gray stroke color" in error_msg:
                logger.warning(
                    f"Document contains invalid color definitions, skipping: {e}"
                )
                return ""
            if "data format error" in error_msg or "not valid" in error_msg:
                logger.warning(f"Invalid or corrupted document: {e}")
                return ""
            raise

    async def _bytes_to_markdown(
        self,
        body: bytes,
        filename: str = "document.pdf",
        content_type: str = "",
    ) -> str:
        """Convert raw bytes to markdown with markitdown, falling back to OCR for PDFs."""
        is_pdf = "pdf" in content_type or body[:4] == b"%PDF"

        try:
            text_markdown = await self._convert_to_md(
                BytesIO(body),
                filename=filename or ("document.pdf" if is_pdf else "document.html"),
            )
            if self._valid_markdown(text_markdown, min_length=50)[0]:
                return text_markdown.strip()
        except Exception as e:
            logger.warning(f"markitdown extraction failed: {e}")

        if is_pdf and self.ocr_service:
            return await self.ocr_service.pdf_to_markdown(body)

        if is_pdf:
            logger.warning(
                "PDF extraction yielded little text and no OCR service configured."
            )
        return ""

    async def _get_markdown(
        self,
        url: str | None = None,
        response: aiohttp.ClientResponse | None = None,
        stream: BytesIO | None = None,
        html_content: str | None = None,
        filename: str | None = None,
    ) -> str:
        """Get markdown from various input sources using markitdown.

        Priority: stream > html_content > response > url
        """
        result = ""
        try:
            if stream is not None:
                raw = stream.read()

                if filename:
                    ext = Path(filename).suffix.lower()
                    is_image = ext in [".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".gif"]
                else:
                    is_image = raw[:4] != b"%PDF"

                if is_image and self.ocr_service:
                    result = await self.ocr_service.images_to_markdown([raw])
                elif is_image:
                    logger.warning(
                        "No LLM OCR service configured; cannot process image."
                    )
                else:
                    result = await self._bytes_to_markdown(
                        raw, filename=filename or "document.pdf"
                    )

            elif html_content is not None:
                buffer = BytesIO(html_content.encode("utf-8"))
                result = await self._convert_to_md(
                    buffer,
                    filename="document.html",
                )

            else:
                resp = response
                if not resp and url:
                    resp = await self.request_service.make_request(url)
                if resp and not isinstance(resp, FailedRequest):
                    client_resp = cast(aiohttp.ClientResponse, resp)
                    body = await client_resp.read()
                    resp_filename, content_type = (
                        self.request_service.detect_content_info(client_resp)
                    )
                    used_filename = filename or resp_filename
                    result = await self._bytes_to_markdown(
                        body, filename=used_filename, content_type=content_type
                    )

        except Exception as e:
            logger.error(f"Error converting to markdown: {e}")

        return self._clean_markdown(result) if result else ""

    # ------------------------------------------------------------------
    # Hooks for child classes
    # ------------------------------------------------------------------

    def _build_doc_result(
        self,
        *,
        year: int,
        norm_type: str,
        situation: str,
        title: str,
        text_markdown: str,
        document_url: str,
        **extra,
    ) -> dict:
        """Build a standardized document result dict."""
        return {
            "year": year,
            "type": norm_type,
            "situation": situation,
            "title": title,
            "text_markdown": text_markdown,
            "document_url": document_url,
            **extra,
        }

    # ------------------------------------------------------------------
    # Document file saving & resume
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_extension(content_type: str, filename: str | None = None) -> str:
        """Determine file extension from content type or filename."""
        if filename:
            ext = Path(filename).suffix
            if ext:
                return ext

        ct = (content_type or "").lower()
        if "pdf" in ct:
            return ".pdf"
        if "html" in ct:
            return ".html"
        if "xml" in ct:
            return ".xml"
        if "msword" in ct or "officedocument.wordprocessing" in ct:
            return ".docx"
        if "rtf" in ct:
            return ".rtf"
        if "plain" in ct:
            return ".txt"
        return ".bin"

    async def _download_and_convert(
        self,
        url: str,
    ) -> tuple[str, bytes, str]:
        """Download content from URL, convert to markdown, and return raw bytes.

        Returns:
            Tuple of ``(markdown, raw_bytes, file_extension)``.
        """
        resp = await self.request_service.make_request(url)
        if not resp or isinstance(resp, FailedRequest):
            return "", b"", ""

        client_resp = cast(aiohttp.ClientResponse, resp)
        body = await client_resp.read()
        filename, content_type = self.request_service.detect_content_info(client_resp)
        ext = self._detect_extension(content_type, filename)

        if "pdf" in (content_type or "").lower() or body[:4] == b"%PDF":
            markdown = await self._get_markdown(stream=BytesIO(body))
            if not ext or ext == ".bin":
                ext = ".pdf"
        else:
            md_filename = filename or f"document{ext}"
            markdown = await self._convert_to_md(BytesIO(body), filename=md_filename)
            markdown = self._clean_markdown(markdown)

        return markdown, body, ext

    async def _save_doc_result(self, doc_result: dict) -> dict | None:
        """Save a document result immediately via FileSaver."""
        if not self.saver:
            return None

        raw_content = doc_result.pop("_raw_content", None)
        content_ext = doc_result.pop("_content_extension", None)

        return await self.saver.save_document(
            doc_data=doc_result,
            raw_content=raw_content,
            content_extension=content_ext,
        )

    def _with_save(self, coro, context: dict):
        """Wrap a _get_doc_data coroutine to save its result immediately with context.

        Each wrapped task calls ``_save_doc_result`` as soon as it finishes,
        rather than deferring saves until after ``asyncio.gather`` collects
        all results.
        """

        async def _wrapper():
            result = await coro
            if result is None:
                return None

            if isinstance(result, list):
                saved = []
                for r in result:
                    doc = {**context, **r}
                    if not doc.get("situation") and context.get("situation"):
                        doc["situation"] = context["situation"]
                    if doc.get("year") is None and context.get("year") is not None:
                        doc["year"] = context["year"]
                    s = await self._save_doc_result(doc)
                    saved.append(s if s is not None else doc)
                return saved

            doc_result = {**context, **result}
            if not doc_result.get("situation") and context.get("situation"):
                doc_result["situation"] = context["situation"]
            if doc_result.get("year") is None and context.get("year") is not None:
                doc_result["year"] = context["year"]
            saved = await self._save_doc_result(doc_result)
            return saved if saved is not None else doc_result

        return _wrapper()

    async def _load_scraped_keys(self, year: int) -> None:
        """Load already-scraped document keys for a year (resume support)."""
        if self.saver:
            self._scraped_keys = await self.saver.get_scraped_keys(year)
            if self._scraped_keys and self.verbose:
                logger.info(
                    f"{self.__class__.__name__} | Year {year}: "
                    f"{len(self._scraped_keys)} documents already scraped"
                )
        else:
            self._scraped_keys = set()

    def _is_already_scraped(self, document_url: str, title: str = "") -> bool:
        """Check if a document has already been scraped (resume support)."""
        if self.overwrite:
            return False
        return (document_url, title) in self._scraped_keys

    def _format_search_url(self, *args, **kwargs) -> str:
        """Template method: build a search/listing URL for the scraper's site."""
        raise NotImplementedError(
            "This method should be implemented in the child class."
        )

    async def _get_docs_links(self, *args, **kwargs) -> list[dict] | None:
        """Template method: return a list of document metadata dicts from a listing page."""
        raise NotImplementedError(
            "This method should be implemented in the child class."
        )

    async def _get_doc_data(self, *args, **kwargs) -> dict | list[dict] | None:
        """Template method: fetch and parse a single document's content."""
        raise NotImplementedError(
            "This method should be implemented in the child class."
        )

    async def _save_doc_error(
        self,
        *,
        title: str,
        year: str | int,
        situation: str = "",
        norm_type: str = "",
        html_link: str = "",
        error_message: str = "Document processing failed",
        **extra,
    ) -> None:
        """Persist a document-level processing error via the saver."""
        if not self.saver:
            return
        error_data = {
            "title": title,
            "year": str(year),
            "situation": situation,
            "type": norm_type,
            "html_link": html_link,
            **extra,
        }
        await self.saver.save_error(error_data, error_message=error_message)

    _SERVER_ERROR_PATTERNS: list[str] = [
        "the requested url was not found on this server",
        "failed to open stream",
        "http request failed",
        "service unavailable",
        "doesn't work properly without javascript enabled",
    ]

    def _valid_markdown(
        self,
        text_markdown: str | None,
        min_length: int = 100,
    ) -> tuple[bool, str]:
        """Validate markdown text using common patterns found across all scrapers."""
        if not text_markdown:
            return False, "text_markdown is None or empty"

        stripped = text_markdown.strip()
        if not stripped:
            return False, "text_markdown is empty after strip"

        cleaned = stripped.translate(
            str.maketrans("", "", string.punctuation + string.whitespace)
        )
        if not cleaned:
            return False, "text_markdown contains only punctuation/whitespace"

        lower = stripped.lower()
        for pattern in self._SERVER_ERROR_PATTERNS:
            if pattern in lower:
                return False, f"text_markdown contains server error: {pattern}"

        if len(stripped) < min_length:
            return (
                False,
                f"text_markdown too short ({len(stripped)} < {min_length} chars)",
            )

        return True, ""

    async def _save_gather_errors(
        self,
        results: list,
        context: dict,
        desc: str = "",
        min_length: int = 100,
    ) -> list:
        """Filter asyncio.gather results: persist exceptions via save_error, return valid results."""
        ctx = {"year": "", "type": "", "situation": "", **context}
        valid = []
        for result in results:
            if isinstance(result, BaseException):
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
            if isinstance(result, dict) and "text_markdown" in result:
                is_valid, reason = self._valid_markdown(
                    result["text_markdown"], min_length=min_length
                )
                if not is_valid:
                    self.error_count += 1
                    title = result.get("title", desc or "Unknown")
                    logger.warning(
                        f"{desc} | Invalid text_markdown for '{title}': {reason}"
                    )
                    if self.saver:
                        error_data = {
                            "title": title,
                            "html_link": result.get("document_url", ""),
                            "text_markdown": result["text_markdown"],
                            **ctx,
                        }
                        await self.saver.save_error(
                            error_data,
                            error_message=reason,
                        )
                    continue
            valid.append(result)
        return valid

    async def _gather_results(
        self,
        tasks: list,
        context: dict | None = None,
        desc: str = "",
        min_length: int = 100,
    ) -> list:
        """Run tasks with asyncio.gather and filter errors.

        Args:
            tasks: List of coroutines to execute.
            context: Dict merged into error records (year, type, situation).
            desc: Label for log messages and progress bar.
            min_length: Minimum character count for valid text_markdown.
        """
        if not tasks:
            return []

        if self.verbose:

            async def wrap_task(coro):
                try:
                    return await coro
                except BaseException as e:
                    return e

            wrapped_tasks = [wrap_task(t) for t in tasks]
            results = await async_tqdm.gather(*wrapped_tasks, desc=desc or "Gathering")
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        return await self._save_gather_errors(
            results, context or {}, desc, min_length=min_length
        )

    @staticmethod
    def _flatten_results(results: list) -> list[dict]:
        """Flatten a list that may contain dicts and/or lists of dicts."""
        flat: list[dict] = []
        for item in results:
            if isinstance(item, list):
                flat.extend(item)
            elif item:
                flat.append(item)
        return flat

    async def _scrape_type(self, norm_type: str, norm_type_id, year: int) -> list[dict]:
        """Scrape all documents of a single type for a year.

        Override in child classes to implement the actual scraping logic.
        """
        raise NotImplementedError(
            "This method should be implemented in the child class."
        )

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape norms for a specific year.

        Default implementation gathers all types concurrently via
        ``_scrape_type``. Override in child classes that need custom
        year-level logic (e.g. Acre, Ceará, MS, RN).
        """
        if isinstance(self.types, dict):
            tasks = [
                self._scrape_type(nt, nt_id, year) for nt, nt_id in self.types.items()
            ]
        else:
            tasks = [self._scrape_type(nt, None, year) for nt in self.types]

        valid = await self._gather_results(
            tasks,
            context={"year": year},
            desc=f"{self.name} | Year {year}",
        )

        return self._flatten_results(valid)

    # ------------------------------------------------------------------
    # Main scrape flow
    # ------------------------------------------------------------------

    def _track_results(self, results: list[dict]) -> None:
        """Update lightweight summary counters from a batch of results."""
        for doc in results:
            doc_type = doc.get("type", "Unknown")
            doc_situation = doc.get("situation", "Unknown")
            if doc_type not in self._types_summary:
                self._types_summary[doc_type] = {"total": 0, "situations": {}}
            self._types_summary[doc_type]["total"] += 1
            self._types_summary[doc_type]["situations"][doc_situation] = (
                self._types_summary[doc_type]["situations"].get(doc_situation, 0) + 1
            )

    async def scrape(self) -> int:
        """Scrape data from all years (async).

        Returns:
            Total number of documents scraped.
        """
        if not self.saver:
            raise RuntimeError(
                "Saver is not initialized. Call _initialize_saver() in the child class __init__ method."
            )

        if self.use_browser:
            await self.initialize_playwright()

        self._scrape_start = time.time()

        logger.info(f"Starting from {self.year_start}")

        years_progress = tqdm(
            self.years,
            desc=f"{self.__class__.__name__} | Years",
        )

        for year in years_progress:
            years_progress.set_description(f"{self.__class__.__name__} | Year: {year}")
            await self._load_scraped_keys(year)

            year_results = await self._scrape_year(year)
            if year_results:
                self._track_results(year_results)
                self.count += len(year_results)

            if self.saver:
                await self.saver.flush(year)

        await self._save_summary()
        return self.count

    async def _save_summary(self) -> None:
        """Write a summary JSON file with final scraping statistics."""
        if not self.saver:
            return

        await self.saver.flush_all()

        elapsed = time.time() - (self._scrape_start or time.time())

        summary = {
            "scraper": self.__class__.__name__,
            "year_start": self.year_start,
            "year_end": self.year_end,
            "total_documents": self.count,
            "total_errors": self.error_count,
            "elapsed_seconds": round(elapsed, 2),
            "elapsed_human": _format_duration(elapsed),
            "completed_at": datetime.now().isoformat(),
            "types_summary": self._types_summary,
        }

        summary_path = Path(self.saver.save_dir) / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(summary_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(summary, ensure_ascii=False, indent=2))

        logger.info(
            f"{summary['scraper']} | Done — {summary['total_documents']} docs, "
            f"{summary['total_errors']} errors, {summary['elapsed_human']}"
        )

    async def cleanup(self):
        """Clean up aiohttp session, Playwright browser, etc.

        Safe to call multiple times (idempotent).
        """
        if getattr(self, "_cleaned_up", False):
            return
        self._cleaned_up = True
        if hasattr(self, "request_service"):
            await self.request_service.cleanup()
        if self.browser_service:
            await self.browser_service.cleanup()
        mhtml_browser: BrowserService | None = getattr(self, "_mhtml_browser", None)
        if mhtml_browser is not None:
            await mhtml_browser.cleanup()
        if self.saver:
            await self.saver.cleanup()


class StateScraper(BaseScraper):
    """Convenience base for state-level legislation scrapers.

    Automatically applies ``STATE_LEGISLATION_SAVE_DIR`` as the default
    ``docs_save_dir`` when the environment variable is set.
    """

    def __init__(self, *args, **kwargs):
        if STATE_LEGISLATION_SAVE_DIR:
            kwargs.setdefault("docs_save_dir", STATE_LEGISLATION_SAVE_DIR)
        super().__init__(*args, **kwargs)
