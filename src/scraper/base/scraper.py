import asyncio
import json
import logging
import string
import time
import re

import aiofiles
import aiohttp
import urllib3

from typing import Optional
from io import BytesIO
from os import environ
from datetime import datetime
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

from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import DocumentStream
from docling_core.types.doc.document import ContentLayer, DocItemLabel
from pathlib import Path
from src.database.saver import ERROR_LOG_DIR, FileSaver
from src.utils import clean_md_tag
from src.utils.openvpn import OpenVPNManager
from src.scraper.base.concurrency import run_in_thread
from src.services.request.service import RequestService
from src.services.ocr.llm import LLMOCRService

# suppress urllib3 InsecureRequestWarning (verify=False is used intentionally for some gov sites)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# suppress httpx and urllib3 logging
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("docling.backend.html_backend").setLevel(logging.ERROR)

YEAR_START = 1808
DEFAULT_VALID_SITUATION = "Não consta revogação expressa"
DEFAULT_INVALID_SITUATION = "Revogada"

STATE_LEGISLATION_SAVE_DIR = environ.get("STATE_LEGISLATION_SAVE_DIR")


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
        llm_config: Optional[dict] = None,
        llm_prompt: str = """Você é um um especialista de extração e formatação de textos jurídicos. O documento fornecido é uma norma jurídica. Extraia todo o conteúdo principal e formate-o em Markdown, seguindo rigorosamente estas regras:

*   **Fidelidade Absoluta (CRÍTICO):** Transcreva o texto exata e literalmente como aparece no documento. Não altere nenhuma palavra, não corrija gramática e não modifique a pontuação. Preservar a exatidão legal é essencial. Não introduza nenhuma palavra ou frase que não esteja presente no documento original.
*   **Estrutura Legal:** Respeite rigorosamente a numeração e a hierarquia legislativa: títulos, capítulos, seções, artigos (Art.), parágrafos (§), incisos (algarismos romanos: I, II, III) e alíneas (letras: a, b, c).
*   **Formatação Markdown:**
    * Use títulos Markdown (`##` ou `###`) para títulos, capítulos e seções.
    * Aplique **negrito** ou *itálico* exatamente onde o texto original estiver em destaque.
    * Caso haja tabelas, preserve a formatação tabular usando a sintaxe de tabelas do Markdown.
    * Se houver uma *ementa* (o bloco de texto que resume a norma, geralmente recuado à direita no topo), formate-a como citação (usando `>` no início do bloco).
*   **Continuidade:** O texto pode ser continuação de uma página anterior ou terminar de forma abrupta. Extraia desde a primeira palavra válida até a última, mesmo que comece ou termine no meio de uma frase.
*   **Limpeza e Exclusões (ATENÇÃO):** Ignore cabeçalhos (headers), rodapés (footers), números de página, datas de impressão ou marcas d'água. **Exclua obrigatoriamente qualquer nota editorial ou aviso legal que inicie com "Este texto não substitui..." ou "Esse texto não substitui..." ou outras notas e observações similares, independentemente de onde apareçam na página.**

Nota: o documento recebido pode estar em branco ou inválido. Nesses casos, retorne uma string vazia ("") e nada além disso.

Retorne **EXCLUSIVAMENTE** o conteúdo extraído. Não inclua a tag ```markdown, não inclua saudações, introduções ou qualquer explicação adicional, antes ou depois do texto.""",
        use_browser: bool = False,
        multiple_pages: bool = False,
        headless: bool = True,
        use_browser_vpn: bool = False,
        vpn_extension_path: Optional[str] = None,
        vpn_extension_page: Optional[str] = None,
        use_openvpn: bool = False,
        config_files: Optional[list] = None,
        openvpn_credentials_map: Optional[dict] = None,
        proxies: Optional[dict] = None,
        proxy_config: Optional[dict] = None,
        rps: float = 20,
        max_workers: int = 16,
        verbose: bool = False,
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
            from src.services.proxy.service import ProxyService

            self.proxy_service = ProxyService(config=proxy_config, verbose=verbose)

        self.request_service = RequestService(
            rps=rps,
            verbose=verbose,
            proxy_service=self.proxy_service,
            max_workers=max_workers,
        )
        self.ocr_service = (
            LLMOCRService(
                prompt=self.llm_prompt,
                request_service=self.request_service,
                llm_config=self.llm_config,
                verbose=verbose,
            )
            if self.llm_config
            else None
        )
        self.use_browser = use_browser
        self.multiple_pages = multiple_pages
        self.headless = headless
        self.use_openvpn = use_openvpn
        self.vpn_extension_path = vpn_extension_path
        self.vpn_extension_page = vpn_extension_page
        self.config_files = config_files
        self.openvpn_credentials_map = openvpn_credentials_map
        self.verbose = verbose
        self.proxies = proxies
        self.rps = rps
        self.max_workers = max_workers
        self.years = list(range(self.year_start, self.year_end + 1))
        self.results = []
        self.count = 0
        self.error_count = 0
        self._scrape_start: Optional[float] = None

        # Initialize Docling converter for HTML documents
        self.doc_converter = DocumentConverter()
        # Browser service — initialised in scrape() via initialize_playwright()
        self.browser_service: Optional[BrowserService] = (
            BrowserService(
                use_vpn=use_browser_vpn,
                vpn_extension_path=vpn_extension_path,
                multiple_pages=multiple_pages,
                max_workers=max_workers,
                headless=headless,
                verbose=verbose,
                owner_class_name=self.__class__.__name__,
            )
            if use_browser
            else None
        )
        self.saver: Optional[FileSaver] = None
        self.openvpn_manager: Optional[OpenVPNManager] = None
        self._initialize_openvpn_manager()
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
            f"use_openvpn={self.use_openvpn} | "
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
    def page(self) -> Optional[Page]:
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
        self.saver = FileSaver(
            self.docs_save_dir, error_log_dir=error_dir, verbose=self.verbose
        )

    def _initialize_openvpn_manager(self):
        """Initialize openvpn manager"""
        if self.use_openvpn:
            self.openvpn_manager = OpenVPNManager(
                config_files=self.config_files if self.config_files is not None else [],
                credentials_map=self.openvpn_credentials_map,
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
        self, url: str, page: Optional[Page] = None
    ) -> BeautifulSoup:
        """Get BeautifulSoup object from given url using Playwright (async)."""
        if not self.browser_service:
            raise RuntimeError("Browser service is not initialized.")
        return await self.browser_service.get_soup(url, page)

    # ------------------------------------------------------------------
    # Markdown utilities
    # ------------------------------------------------------------------

    def _clean_markdown(
        self,
        text: str,
        replace: Optional[list[tuple[str, str]]] = None,
    ) -> str:
        """Clean markdown text by removing links and applying custom replacements.

        Args:
            text: Input markdown string.
            replace: Optional list of ``(find, replacement)`` tuples applied in order.

        Returns:
            Cleaned markdown string with link syntax stripped (text preserved)
            and all replacements applied.
        """
        text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
        if replace:
            for find, replacement in replace:
                text = re.sub(find, replacement, text)
        return text.strip()

    def _clean_md_tag(self, md_content: str) -> str:
        """Strip markdown code block wrappers if present."""
        return clean_md_tag(md_content)

    def _check_text_length(self, text: str, min_length: int = 50) -> bool:
        """Return True if *text* meets the minimum character threshold.

        Args:
            text: Text to evaluate (typically a markdown string).
            min_length: Minimum number of non-whitespace characters required
                (default 50).

        Returns:
            ``True`` if the stripped text length is >= *min_length*, ``False``
            otherwise.
        """
        return len(text.strip()) >= min_length

    @staticmethod
    def _wrap_html(content: str) -> str:
        """Wrap HTML fragment in <html><body> tags for Docling conversion."""
        return f"<html><body>{content}</body></html>"

    # ------------------------------------------------------------------
    # PDF / Image processing
    # ------------------------------------------------------------------

    async def _get_pdf_image_markdown(self, pdf_content: bytes) -> str:
        """Convert PDF bytes to markdown via the LLM OCR service."""
        if not self.ocr_service:
            logger.warning("No LLM OCR service configured; cannot process PDF.")
            return ""
        return await self.ocr_service.pdf_to_markdown(pdf_content)

    async def _process_pdf_with_fallback(
        self,
        url: str,
        min_length: int = 50,
    ) -> tuple[str, str]:
        """Download a PDF and extract markdown, falling back to LLM OCR if needed.

        Tries Docling conversion first. If the result is too short (likely a
        scanned/image PDF), falls back to page-by-page LLM OCR.

        Args:
            url: URL of the PDF to download.
            min_length: Minimum character threshold for the Docling result to be
                accepted without falling back to OCR.

        Returns:
            Tuple of ``(text_markdown, document_url)``. Both are empty strings
            on failure.
        """
        response = await self.request_service.make_request(url)
        if not response:
            return "", ""

        content = await response.read()
        if not content:
            return "", ""

        # Try Docling first
        text_markdown = await self._get_markdown(response=response)
        if text_markdown and self._check_text_length(text_markdown, min_length):
            return text_markdown.strip(), url

        # Fallback to LLM OCR for image-based PDFs
        try:
            text_markdown = await self._get_pdf_image_markdown(content)
        except Exception as e:
            logger.error(f"OCR fallback failed for {url}: {e}")
            return "", ""

        if text_markdown and text_markdown.strip():
            return text_markdown.strip(), url

        return "", ""

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    async def _convert_to_md(
        self,
        source: str | BytesIO,
        remove_contents: Optional[dict[DocItemLabel, list[str]]] = None,
        remove_hyperlinks: bool = False,
        export_md_kwargs: Optional[dict] = None,
        filename: str = "document",
    ) -> str:
        """Convert a source (URL or BytesIO stream) to markdown via Docling (with retry).

        Args:
            source: URL string or BytesIO stream with document content
            remove_contents: Dict mapping DocItemLabel to list of keywords to filter out
            remove_hyperlinks: If True, remove all hyperlinks from the document
            export_md_kwargs: Keyword arguments forwarded directly to
                ``doc.export_to_markdown()``. Defaults to
                ``{"included_content_layers": {ContentLayer.BODY}}``.

        Returns:
            Markdown string
        """
        try:
            if isinstance(source, BytesIO):
                doc_stream = DocumentStream(name=filename, stream=source)
                result = await run_in_thread(self.doc_converter.convert, doc_stream)
            else:
                # Direct URL — Docling fetches and detects format (handles redirects)
                result = await run_in_thread(self.doc_converter.convert, source)

            doc = result.document

            # Apply filtering if requested
            if remove_contents or remove_hyperlinks:
                items_to_delete = []

                for item, item_index in doc.iterate_items():
                    if item.content_layer == ContentLayer.BODY:
                        # Filter by label and keywords
                        if remove_contents and item.label in remove_contents:
                            keywords = remove_contents[item.label]
                            if any(keyword in item.text for keyword in keywords):
                                items_to_delete.append(item)

                        # Remove hyperlinks if requested
                        if remove_hyperlinks and hasattr(item, "hyperlink"):
                            item.hyperlink = None

                if items_to_delete:
                    doc.delete_items(node_items=items_to_delete)

            kwargs = export_md_kwargs or {}
            kwargs.setdefault("included_content_layers", {ContentLayer.BODY})
            markdown = doc.export_to_markdown(**kwargs)

            if not markdown or not markdown.strip():
                raise ValueError("Docling returned empty content")

            markdown = self._clean_md_tag(markdown.strip())

            # Strip trailing horizontal-rule / table-border artefacts that
            # docling emits when the HTML source is wrapped in an outer <table>.
            # These look like "---…---" or "---…---|" lines at the very end.
            markdown = re.sub(r"(\n[-|]{3,}\s*)+$", "", markdown).strip()

            return markdown

        except Exception as e:
            error_msg = str(e).lower()
            # Non-retryable errors — return empty string immediately
            if "invalid float value" in error_msg or "gray stroke color" in error_msg:
                logger.warning(
                    f"Document contains invalid color definitions, skipping: {e}"
                )
                return ""
            if "data format error" in error_msg or "not valid" in error_msg:
                logger.warning(f"Invalid or corrupted document: {e}")
                return ""
            # Let other exceptions be retried
            raise

    async def _get_markdown(
        self,
        url: Optional[str] = None,
        response: Optional[aiohttp.ClientResponse] = None,
        stream: Optional[BytesIO] = None,
        html_content: Optional[str] = None,
        remove_contents: Optional[dict[DocItemLabel, list[str]]] = None,
        remove_hyperlinks: bool = False,
        export_md_kwargs: Optional[dict] = None,
    ) -> str:
        """Get markdown from various input sources using Docling.

        Priority: stream > html_content > response > url
        """
        try:
            if stream is not None:
                raw = stream.read()
                if raw[:4] == b"%PDF":
                    if not self.ocr_service:
                        logger.warning(
                            "No LLM OCR service configured; cannot process PDF."
                        )
                        return ""
                    return await self.ocr_service.pdf_to_markdown(raw)
                if not self.ocr_service:
                    logger.warning(
                        "No LLM OCR service configured; cannot process image."
                    )
                    return ""
                return await self.ocr_service.images_to_markdown([raw])

            if html_content is not None:
                buffer = BytesIO(html_content.encode("utf-8"))
                return await self._convert_to_md(
                    buffer,
                    remove_contents,
                    remove_hyperlinks,
                    export_md_kwargs,
                    filename="document.html",
                )

            if response is not None:
                body = await response.read()
                filename, content_type = self.request_service.detect_content_info(
                    response
                )

                if "pdf" in content_type or body[:4] == b"%PDF":
                    if not self.ocr_service:
                        logger.warning(
                            "No LLM OCR service configured; cannot process PDF."
                        )
                        return ""
                    return await self.ocr_service.pdf_to_markdown(body)
                return await self._convert_to_md(
                    BytesIO(body),
                    remove_contents,
                    remove_hyperlinks,
                    export_md_kwargs,
                    filename=filename,
                )

            if url:
                resp = await self.request_service.make_request(url)
                if resp is not None:
                    body = await resp.read()
                    filename, content_type = self.request_service.detect_content_info(
                        resp
                    )

                    if "pdf" in content_type or body[:4] == b"%PDF":
                        if not self.ocr_service:
                            logger.warning(
                                "No LLM OCR service configured; cannot process PDF URL."
                            )
                            return ""
                        return await self.ocr_service.pdf_to_markdown(body)
                    return await self._convert_to_md(
                        BytesIO(body),
                        remove_contents,
                        remove_hyperlinks,
                        export_md_kwargs,
                        filename=filename,
                    )

        except Exception as e:
            logger.error(f"Error converting to markdown: {e}")

        return ""

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
        """Build a standardized document result dict.

        All scrapers should use this to construct their output dicts so
        that downstream consumers see a consistent schema.

        Args:
            year: Publication year.
            norm_type: Legislation type (e.g. "Lei", "Decreto").
            situation: Vigency status (e.g. "Não consta revogação expressa").
            title: Document title / heading.
            text_markdown: Extracted markdown content.
            document_url: URL the content was fetched from.
            **extra: Any additional scraper-specific fields.

        Returns:
            Dict with all standard fields plus extras.
        """
        return {
            "year": year,
            "type": norm_type,
            "situation": situation,
            "title": title,
            "text_markdown": text_markdown,
            "document_url": document_url,
            **extra,
        }

    def _handle_blocked_access(self, *args, **kwargs):
        pass

    def _format_search_url(self, *args, **kwargs) -> str:
        """Format search URL for the given parameters"""
        raise NotImplementedError(
            "This method should be implemented in the child class."
        )

    async def _get_docs_links(self, *args, **kwargs) -> Optional[list[dict]]:
        """Get document links from the given parameters"""
        raise NotImplementedError(
            "This method should be implemented in the child class."
        )

    async def _get_doc_data(self, *args, **kwargs) -> Optional[dict | list[dict]]:
        """Get document data from the given parameters"""
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
        """Persist a document-level processing error via the saver.

        Convenience wrapper that normalises fields expected by
        ``FileSaver.save_error`` so that every scraper can log
        individual document failures with minimal boilerplate.
        """
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

    def _valid_markdown(
        self,
        text_markdown: str | None,
        min_length: int = 50,
    ) -> tuple[bool, str]:
        """Validate markdown text using common patterns found across all scrapers.

        Checks (in order):
        1. Falsy / None
        2. Empty after stripping whitespace
        3. Only punctuation (e.g. a single dot — seen in minas_gerais)
        4. Below *min_length* characters (after strip)

        Args:
            text_markdown: The text to validate.
            min_length: Minimum acceptable character count (default 50).

        Returns:
            Tuple of ``(is_valid, reason)``. *reason* is an empty string when
            valid, otherwise a short description of the failure.
        """
        if not text_markdown:
            return False, "text_markdown is None or empty"

        stripped = text_markdown.strip()
        if not stripped:
            return False, "text_markdown is empty after strip"

        # Content that is only punctuation / whitespace (e.g. ".", "...", "- .")
        cleaned = stripped.translate(
            str.maketrans("", "", string.punctuation + string.whitespace)
        )
        if not cleaned:
            return False, "text_markdown contains only punctuation/whitespace"

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
        min_length: int = 50,
    ) -> list:
        """Filter asyncio.gather results: persist exceptions via save_error, return valid results.

        Args:
            results: Output of ``await asyncio.gather(*tasks, return_exceptions=True)``.
            context: Dict with ``year``, ``type``, ``situation`` — merged into each error record.
            desc: Label for log messages.
            min_length: Minimum character count for valid text_markdown (default 50).

        Returns:
            List of non-None, non-Exception results with valid text_markdown (order preserved).
        """
        valid = []
        for result in results:
            if isinstance(result, BaseException):
                self.error_count += 1
                logger.error(f"{desc} | Error: {result}")
                if self.saver:
                    error_data = {
                        "title": desc or "Unknown",
                        "html_link": "",
                        **context,
                    }
                    await self.saver.save_error(error_data, error_message=str(result))
                continue
            if result is None:
                continue
            # Validate text_markdown if the result is a dict that contains one
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
                            **context,
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
        min_length: int = 50,
    ) -> list:
        """Run tasks with asyncio.gather and filter errors."""
        if self.verbose and tasks:

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

    async def _scrape_type(self, norm_type: str, norm_type_id, year: int) -> list[dict]:
        """Scrape all documents of a single type for a year.

        Override in child classes to implement the actual scraping logic.
        """
        raise NotImplementedError(
            "This method should be implemented in the child class."
        )

    async def _scrape_year(self, year: int, *_args, **_kwargs) -> list[dict]:
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

        all_results: list[dict] = []
        for result in valid:
            if isinstance(result, list):
                all_results.extend(result)
            elif result:
                all_results.append(result)
        return all_results

    # ------------------------------------------------------------------
    # Main scrape flow
    # ------------------------------------------------------------------

    async def scrape(self) -> list:
        """Scrape data from all years (async)."""

        # Check saver initialization
        if not self.saver:
            raise RuntimeError(
                "Saver is not initialized. Call _initialize_saver() in the child class __init__ method."
            )

        # Initialize Playwright browser if needed
        if self.use_browser:
            await self.initialize_playwright()

        self._scrape_start = time.time()

        # check if can resume from last scraped year
        resume_from = self.year_start
        forced_resume = self.year_start != YEAR_START
        if self.saver.last_year is not None and not forced_resume:
            logger.info(f"Resuming from {self.saver.last_year}")
            resume_from = int(self.saver.last_year)
        else:
            logger.info(f"Starting from {resume_from}")

        # filter years to scrape
        years_to_scrape = [y for y in self.years if y >= resume_from]

        # scrape years sequentially (types/situations within each year are concurrent)
        all_year_results = []

        years_progress = tqdm(
            years_to_scrape,
            desc=f"{self.__class__.__name__} | Years",
        )

        for year in years_progress:
            years_progress.set_description(f"{self.__class__.__name__} | Year: {year}")
            year_results = await self._scrape_year(year)
            if year_results:
                await self.saver.save(year_results)
                all_year_results.append(year_results)
            else:
                all_year_results.append([])

        for year_results in all_year_results:
            if year_results:
                self.results.extend(year_results)
                self.count += len(year_results)

        await self._save_summary()
        return self.results

    async def _save_summary(self) -> None:
        """Write a summary JSON file with final scraping statistics."""
        if not self.saver:
            return

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
        }

        summary_path = Path(self.saver.save_dir) / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(summary_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(summary, ensure_ascii=False, indent=2))

        logger.info(
            f"{summary['scraper']} | Done — {summary['total_documents']} docs, "
            f"{summary['total_errors']} errors, {summary['elapsed_human']}"
        )

    async def _change_vpn_connection(self, *_args, **_kwargs):
        """Change VPN connection (async-wrapped)."""
        if not self.use_openvpn:
            return
        if self.openvpn_manager is None:
            logger.warning("OpenVPN manager is not initialized")
            return
        await run_in_thread(self.openvpn_manager.change_vpn_connection)

    async def cleanup(self):
        """Clean up aiohttp session, Playwright browser, etc."""
        if hasattr(self, "request_service"):
            await self.request_service.cleanup()
        if self.browser_service:
            await self.browser_service.cleanup()
