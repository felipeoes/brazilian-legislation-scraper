from __future__ import annotations

import asyncio
import json
import logging
import os
import string
import time
import re

import aiofiles
import aiohttp
import fitz
import urllib3

from io import BytesIO
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Coroutine, cast
from bs4 import BeautifulSoup, Tag
from playwright.async_api import Page
from src.services.browser.playwright import BrowserService
from loguru import logger
from tqdm import tqdm
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from markitdown import MarkItDown
from pathlib import Path
from src.config import ERROR_LOG_DIR, SAVE_DIR, STATE_LEGISLATION_SAVE_DIR
from src.database.saver import FileSaver
from src.utils import clean_md_tag
from src.scraper.base.concurrency import run_in_thread
from tqdm.asyncio import tqdm as async_tqdm
from src.services.proxy.service import ProxyService
from src.services.request.service import RequestService

if TYPE_CHECKING:
    from src.services.ocr.config import LLMConfig
    from src.services.ocr.llm import LLMOCRService

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


_DISCLAIMER_RE = re.compile(
    r"(Est[ea]\s+(texto|conte[uú]do)|Ess[ea]\s+texto)\s+n[aã]o\s+substitui",
    re.IGNORECASE,
)

_SERVER_ERROR_PATTERNS: list[str] = [
    "the requested url was not found on this server",
    "file or directory not found",
    "failed to open stream",
    "http request failed",
    "service unavailable",
    "doesn't work properly without javascript enabled",
]

_CLEAN_NORM_EMPTY_TAGS = ["h1", "h2", "h3", "h4", "h5", "h6", "div", "span", "b"]


async def _capture_exception(coro):
    """Await *coro* and return any raised exception as a value instead of propagating it.

    Used by :meth:`_gather_results` in verbose mode so that ``tqdm.gather``
    can track progress even when individual tasks fail.
    """
    try:
        return await coro
    except BaseException as e:
        return e


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


def _parse_usage_counts(usage: dict) -> tuple[int, int, int]:
    """Extract (requests, successful_requests, failed_requests) from a usage dict."""
    requests = int(usage.get("requests", 0) or 0)
    failed_requests = int(usage.get("failed_requests", 0) or 0)
    successful_requests = usage.get("successful_requests")
    if successful_requests is None:
        successful_requests = max(requests - failed_requests, 0)
    else:
        successful_requests = int(successful_requests or 0)
    return requests, successful_requests, failed_requests


def _llm_usage_totals(llm_usage: dict[str, dict]) -> dict[str, int]:
    """Aggregate per-model LLM usage into a single totals dict."""
    totals = {
        "requests": 0,
        "successful_requests": 0,
        "failed_requests": 0,
        "input_tokens": 0,
        "cached_tokens": 0,
        "output_tokens": 0,
        "reasoning_tokens": 0,
    }
    for usage in llm_usage.values():
        requests, successful_requests, failed_requests = _parse_usage_counts(usage)
        totals["requests"] += requests
        totals["successful_requests"] += successful_requests
        totals["failed_requests"] += failed_requests
        for key in totals:
            if key in {"requests", "successful_requests", "failed_requests"}:
                continue
            totals[key] += int(usage.get(key, 0) or 0)
    return totals


def _format_llm_usage_counts(usage: dict[str, int]) -> str:
    """Build a compact human-readable token usage string."""
    requests, successful_requests, failed_requests = _parse_usage_counts(usage)
    return (
        f"{requests} reqs ({successful_requests} ok, {failed_requests} failed), "
        f"{int(usage.get('input_tokens', 0) or 0)} input, "
        f"{int(usage.get('cached_tokens', 0) or 0)} cached, "
        f"{int(usage.get('output_tokens', 0) or 0)} output, "
        f"{int(usage.get('reasoning_tokens', 0) or 0)} reasoning"
    )


def _format_llm_usage(llm_usage: dict[str, dict]) -> str:
    """Build a compact human-readable LLM usage string with per-model details."""
    totals = _llm_usage_totals(llm_usage)
    model_breakdown = "; ".join(
        f"{model}: {_format_llm_usage_counts(usage)}"
        for model, usage in sorted(llm_usage.items())
    )
    summary = f"LLM total {_format_llm_usage_counts(totals)}"
    if model_breakdown:
        summary += f" | {model_breakdown}"
    return summary


class BaseScraper:
    """Base class for legislation scrapers (async)"""

    _iterate_situations: bool = False

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
        rps: float = 20,
        max_workers: int = 50,
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
        self.llm_config = llm_config
        self.llm_prompt = llm_prompt

        self.proxy_service = None
        if proxy_config:
            self.proxy_service = ProxyService(config=proxy_config, verbose=verbose)

        self.request_service = RequestService(
            rps=rps,
            verbose=verbose,
            proxy_service=self.proxy_service,
            max_retries=max_retries,
            disable_cookies=disable_cookies,
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
        error_dir = ERROR_LOG_DIR / self.name
        self.saver = FileSaver(
            self.docs_save_dir,
            error_log_dir=error_dir,
            verbose=self.verbose,
            max_workers=self.max_workers,
            mhtml_capture_fn=self._capture_mhtml,
        )

    async def _capture_mhtml(self, url: str) -> bytes:
        """Capture an MHTML snapshot of a URL using a lazily-initialized browser."""
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

    # ------------------------------------------------------------------
    # HTTP requests (async via RequestService)
    # ------------------------------------------------------------------

    async def _fetch_soup_with_retry(self, url: str) -> BeautifulSoup:
        """Fetch URL and return BeautifulSoup, raising on failure.

        Retries are handled internally by ``RequestService``.
        """
        soup = await self.request_service.get_soup(url)
        if not soup:
            reason = getattr(soup, "reason", "Unexpected response")
            raise RuntimeError(f"Failed to fetch {url}: {reason}")
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
            BytesIO(wrapped.encode("utf-8")),
            filename="document.html",
        )
        return self._clean_markdown(md)

    # ------------------------------------------------------------------
    # PDF / Image processing
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((OSError, RuntimeError)),
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

            markdown = markdown.strip()
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

    @staticmethod
    def _is_pdf(body: bytes, content_type: str = "") -> bool:
        """Check if content is a PDF based on content type or magic bytes."""
        return "pdf" in content_type.lower() or body[:4] == b"%PDF"

    @staticmethod
    def _strip_html_chrome(
        soup: BeautifulSoup | Tag, extra_selectors: list[dict] | None = None
    ) -> BeautifulSoup | Tag:
        """Remove standard chrome tags (script, style, nav, header, footer, aside) from soup.

        Optionally remove elements matching *extra_selectors* (each a dict of
        ``find_all`` kwargs, e.g. ``{"class_": "rodapeTexto"}``).
        """
        for tag_name in ("script", "style", "nav", "header", "footer", "aside"):
            for el in soup.find_all(tag_name):
                el.decompose()
        if extra_selectors:
            for selector in extra_selectors:
                for el in soup.find_all(True, **selector):
                    el.decompose()
        return soup

    @staticmethod
    def _calc_pages(total: int, per_page: int) -> int:
        """Number of pages needed for *total* items at *per_page* each."""
        if total <= 0 or per_page <= 0:
            return 0
        return (total + per_page - 1) // per_page

    @staticmethod
    def _clean_norm_soup(
        soup: BeautifulSoup | Tag,
        *,
        remove_disclaimers: bool = True,
        unwrap_links: bool = True,
        remove_images: bool = True,
        remove_empty_tags: bool = True,
        unwrap_fonts: bool = False,
        strip_styles: bool = False,
        remove_style_tags: bool = False,
        remove_script_tags: bool = False,
    ) -> BeautifulSoup | Tag:
        """Content-level cleaning for norm text HTML.

        Complements ``_strip_html_chrome()`` (structural cleanup) with
        content-level artifact removal common to Brazilian legislation pages.
        All options are independently toggleable.
        """
        # --- Pass 1: decompose simple leaf-level tags (invalidates iterators) ---
        simple_remove: list[str] = []
        if remove_images:
            simple_remove.append("img")
        if remove_style_tags:
            simple_remove.append("style")
        if remove_script_tags:
            simple_remove.append("script")
        if simple_remove:
            for tag in soup.find_all(simple_remove):
                tag.decompose()

        # --- Pass 2: combined content-level cleanup (disclaimers, links, fonts, empty tags) ---
        disclaimer_tags = {"p", "span", "div"} if remove_disclaimers else set()
        empty_tag_names = set(_CLEAN_NORM_EMPTY_TAGS) if remove_empty_tags else set()
        needs_combined = (
            remove_disclaimers or unwrap_links or unwrap_fonts or remove_empty_tags
        )
        if needs_combined:
            for tag in list(soup.find_all(True)):
                if tag.decomposed:
                    continue
                name = tag.name
                if remove_disclaimers and name in disclaimer_tags:
                    txt = tag.get_text(strip=True)
                    if _DISCLAIMER_RE.search(txt) and len(txt) < 300:
                        tag.decompose()
                        continue
                if unwrap_links and name == "a":
                    if not tag.get_text(strip=True):
                        tag.decompose()
                    else:
                        tag.unwrap()
                    continue
                if unwrap_fonts and name == "font":
                    tag.unwrap()
                    continue
                if remove_empty_tags and name in empty_tag_names:
                    if not tag.get_text(strip=True):
                        tag.decompose()

        # --- Pass 3: strip inline style attributes ---
        if strip_styles:
            for tag in soup.find_all(style=True):
                del tag["style"]

        return soup

    async def _bytes_to_markdown(
        self,
        body: bytes,
        filename: str = "document.pdf",
        content_type: str = "",
    ) -> str:
        """Convert raw bytes to markdown with markitdown, falling back to OCR for PDFs."""
        is_pdf = self._is_pdf(body, content_type)

        try:
            text_markdown = await self._convert_to_md(
                BytesIO(body),
                filename=filename or ("document.pdf" if is_pdf else "document.html"),
            )
            if self._valid_markdown(text_markdown, min_length=50)[0]:
                return text_markdown.strip()
        except (OSError, ValueError, RuntimeError, TypeError) as e:
            logger.warning(f"markitdown extraction failed: {e}")

        if is_pdf:
            fitz_text = await self._pdf_bytes_to_text(body)
            if self._valid_markdown(fitz_text, min_length=50)[0]:
                return fitz_text.strip()

        if is_pdf and self.ocr_service:
            return await self.ocr_service.pdf_to_markdown(body)

        if is_pdf:
            logger.warning(
                "PDF extraction yielded little text and no OCR service configured."
            )
        return ""

    async def _pdf_bytes_to_text(self, body: bytes) -> str:
        """Extract plain text from PDF bytes via PyMuPDF."""

        def _extract() -> str:
            doc = fitz.open(stream=body, filetype="pdf")
            try:
                pages: list[str] = []
                for page in doc:
                    text = page.get_text("text").strip()
                    if text:
                        pages.append(text)
                return "\n\n".join(pages)
            finally:
                doc.close()

        try:
            return (await run_in_thread(_extract)).strip()
        except (OSError, ValueError, RuntimeError, TypeError) as e:
            logger.warning(f"PyMuPDF extraction failed: {e}")
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
                    is_image = not self._is_pdf(raw)

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
                if resp:
                    client_resp = cast(aiohttp.ClientResponse, resp)
                    body = None
                    try:
                        body = await client_resp.read()
                    except aiohttp.ClientPayloadError:
                        result = ""
                    if body:
                        resp_filename, content_type = (
                            self.request_service.detect_content_info(client_resp)
                        )
                        result = await self._response_to_markdown(
                            body, filename or resp_filename, content_type
                        )

        except Exception as e:
            logger.error(f"Error converting to markdown: {e}")

        return self._clean_markdown(result) if result else ""

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

    async def _response_to_markdown(
        self,
        body: bytes,
        filename: str | None = None,
        content_type: str = "",
    ) -> str:
        """Convert raw response bytes to markdown via the standard pipeline.

        Shared by ``_get_markdown`` (response/url branch) and
        ``_download_and_convert``.
        """
        used_filename = filename or (
            "document.pdf" if self._is_pdf(body, content_type) else "document.html"
        )
        return await self._bytes_to_markdown(
            body, filename=used_filename, content_type=content_type
        )

    async def _download_and_convert(
        self,
        url: str,
    ) -> tuple[str, bytes, str]:
        """Download content from URL, convert to markdown, and return raw bytes.

        Returns:
            Tuple of ``(markdown, raw_bytes, file_extension)``.
        """
        result = await self.request_service.fetch_bytes(url)
        if not result:
            return "", b"", ""

        body, client_resp = result
        filename, content_type = self.request_service.detect_content_info(client_resp)
        ext = self._detect_extension(content_type, filename)

        markdown = await self._response_to_markdown(body, filename, content_type)
        markdown = self._clean_markdown(markdown) if markdown else ""

        if self._is_pdf(body, content_type) and (not ext or ext == ".bin"):
            ext = ".pdf"

        return markdown, body, ext

    async def _process_doc(
        self,
        doc_info: dict,
        url: str,
        text_markdown: str,
        raw_content: bytes,
        content_ext: str,
        error_prefix: str = "Invalid content",
    ) -> dict | None:
        """Validate markdown and populate *doc_info*, or save an error.

        Shared validation/population logic used by both ``_process_pdf_doc``
        and ``_process_html_doc``.
        """
        valid, reason = self._valid_markdown(text_markdown)
        if not valid:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                norm_type=doc_info.get("type", ""),
                html_link=url,
                error_message=f"{error_prefix}: {reason}",
            )
            return None

        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url
        doc_info["_raw_content"] = raw_content
        doc_info["_content_extension"] = content_ext
        return doc_info

    async def _process_pdf_doc(
        self,
        doc_info: dict,
        pdf_link_key: str = "pdf_link",
    ) -> dict | None:
        """Download a PDF, convert to markdown, validate, and populate doc_info.

        Shared helper for PDF-only scrapers (Pará, Rondônia, Rio Grande do Norte,
        Maranhão, and others).
        """
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
    ) -> dict | None:
        """Convert HTML to markdown, validate, and populate doc_info.

        Shared helper for HTML-content scrapers (Bahia, Mato Grosso do Sul,
        Amazonas, etc.).
        """
        text_markdown = await self._get_markdown(html_content=html_content)
        return await self._process_doc(
            doc_info,
            url,
            text_markdown,
            html_content.encode("utf-8"),
            ".html",
            error_prefix="Invalid markdown",
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

    # ------------------------------------------------------------------
    # Persistence (inlined from PersistenceMixin)
    # ------------------------------------------------------------------

    async def _save_doc_result(self, doc_result: dict) -> dict | None:
        if not self.saver:
            return None

        raw_content = doc_result.pop("_raw_content", None)
        content_ext = doc_result.pop("_content_extension", None)

        return await self.saver.save_document(
            doc_data=doc_result,
            raw_content=raw_content,
            content_extension=content_ext,
        )

    async def _load_scraped_keys(self, year: int) -> None:
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
        if self.overwrite:
            return False
        return (document_url, title) in self._scraped_keys

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
        self.error_count += 1
        if not self.saver:
            return
        error_data = {
            "title": title,
            "year": year,
            "situation": situation,
            "type": norm_type,
            "html_link": html_link,
            **extra,
        }
        await self.saver.save_error(error_data, error_message=error_message)

    # ------------------------------------------------------------------
    # Results gathering (inlined from ResultsMixin)
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_year(value) -> int | None:
        """Coerce a year value to ``int``, returning ``None`` on failure."""
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return value

    @staticmethod
    def _merge_context(result: dict, context: dict) -> dict:
        doc = {**context, **result}
        if not doc.get("situation") and context.get("situation"):
            doc["situation"] = context["situation"]
        year = BaseScraper._normalize_year(doc.get("year"))
        ctx_year = BaseScraper._normalize_year(context.get("year"))
        if year is None and ctx_year is not None:
            doc["year"] = ctx_year
        elif year is not None:
            doc["year"] = year
        return doc

    def _with_save(self, coro, context: dict):
        async def _wrapper():
            result = await coro
            if result is None:
                return None

            is_list = isinstance(result, list)
            items = result if is_list else [result]
            saved = []
            for r in items:
                doc = self._merge_context(r, context)
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

        return _wrapper()

    async def _save_gather_errors(
        self,
        results: list,
        context: dict,
        desc: str = "",
        min_length: int = 50,
    ) -> list:
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
        min_length: int = 50,
    ) -> list:
        if not tasks:
            return []

        if self.verbose:
            results = await async_tqdm.gather(
                *[_capture_exception(t) for t in tasks],
                desc=desc or "Gathering",
            )
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        return await self._save_gather_errors(
            results, context or {}, desc, min_length=min_length
        )

    @staticmethod
    def _flatten_results(results: list) -> list[dict]:
        flat: list[dict] = []
        for item in results:
            if isinstance(item, list):
                flat.extend(item)
            elif item:
                flat.append(item)
        return flat

    async def _process_documents(
        self,
        documents: list,
        *,
        year: int,
        norm_type: str,
        situation: str = "NA",
        desc: str = "",
        doc_data_fn=None,
        doc_data_kwargs: dict | None = None,
    ) -> list[dict]:
        """Wrap each document through _get_doc_data -> _with_save -> _gather_results."""
        ctx = {"year": year, "type": norm_type, "situation": situation}
        fn = doc_data_fn or self._get_doc_data
        kw = doc_data_kwargs or {}
        tasks = [self._with_save(fn(doc, **kw), ctx) for doc in documents]
        results = await self._gather_results(
            tasks,
            context=ctx,
            desc=desc or f"{self.name} | {norm_type}",
        )
        if self.verbose:
            logger.info(
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
        """Fetch pages ``start_page``..``total_pages`` concurrently and flatten.

        Typical usage — call after fetching and parsing page 1 yourself::

            docs = first_page_docs
            extra = await self._fetch_all_pages(
                lambda p: self._get_docs_links(self._build_url(year, p)),
                total_pages,
                context=ctx,
                desc="SCRAPER | year | get_docs_links",
            )
            docs.extend(extra)

        Returns a flat list of all items gathered from the extra pages.
        """
        if total_pages < start_page:
            return []
        tasks = [make_task(page) for page in range(start_page, total_pages + 1)]
        results = await self._gather_results(tasks, context=context, desc=desc)
        return self._flatten_results(results)

    async def _paginate_until_end(
        self,
        *,
        make_task: Callable[[int], Coroutine[Any, Any, tuple[list[dict], bool]]],
        context: dict,
        desc: str = "",
        initial_batch: int = 1,
        batch_growth: int | None = None,
        max_batch: int | None = None,
    ) -> list[dict]:
        """Fetch pages in growing batches until a page signals end-of-results.

        ``make_task(page_number)`` must return ``(docs, reached_end)``.
        """
        batch = initial_batch
        growth = batch_growth if batch_growth is not None else self.max_workers
        cap = max_batch or self.max_workers
        page = 1
        all_docs: list[dict] = []

        while True:
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

        return all_docs

    def _valid_markdown(
        self,
        text_markdown: str | None,
        min_length: int = 50,
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
        for pattern in _SERVER_ERROR_PATTERNS:
            if pattern in lower:
                return False, f"text_markdown contains server error: {pattern}"

        if len(stripped) < min_length:
            return (
                False,
                f"text_markdown too short ({len(stripped)} < {min_length} chars)",
            )

        return True, ""

    async def _before_scrape(self) -> None:
        """Hook called once before year iteration begins.

        Override for one-time setup (constitution fetching, prefetching, etc.).
        """

    async def _scrape_type(self, norm_type: str, norm_type_id, year: int) -> list[dict]:
        """Scrape all documents of a single type for a year.

        Override in child classes to implement the actual scraping logic.
        """
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
        """Scrape all documents of a single situation+type for a year.

        Override in child classes that set ``_iterate_situations = True``.
        The default ``_scrape_year`` will call this method for every
        ``(situation, type)`` pair when ``_iterate_situations`` is enabled.

        Args:
            year: The year to scrape.
            situation: Situation label (e.g. "Não consta revogação expressa").
            situation_id: Situation identifier (dict value or same as label).
            norm_type: Norm type label (e.g. "Lei Ordinária").
            norm_type_id: Norm type identifier (dict value or None).
        """
        raise NotImplementedError(
            "Scrapers with _iterate_situations = True must implement "
            "_scrape_situation_type."
        )

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape norms for a specific year.

        When ``_iterate_situations`` is True and ``self.situations`` is
        non-empty, iterates the ``situations × types`` cartesian product
        and calls ``_scrape_situation_type``.  Otherwise iterates types
        and calls ``_scrape_type``.

        Override in child classes that need fully custom year-level
        logic (e.g. Acre, Ceará, ICMBio, Santa Catarina).
        """
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
            context={"year": year, "type": "NA", "situation": "NA"},
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

        await self._before_scrape()

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

        llm_usage_by_model = self.ocr_service.usage_stats if self.ocr_service else {}
        summary["llm_usage"] = {
            "models": llm_usage_by_model,
            "totals": _llm_usage_totals(llm_usage_by_model),
            "human": _format_llm_usage(llm_usage_by_model),
        }

        summary_path = Path(self.saver.save_dir) / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(summary_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(summary, ensure_ascii=False, indent=2))

        done_log = (
            f"{summary['scraper']} | Done — {summary['total_documents']} docs, "
            f"{summary['total_errors']} errors, {summary['elapsed_human']}"
        )
        done_log += f", {summary['llm_usage']['human']}"

        logger.info(done_log)

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

    def _normalize_type(
        self,
        raw_type: str,
        known_types: dict | list | None = None,
        aliases: dict | None = None,
        fallback: str = "",
    ) -> str:
        """Normalize a raw type string to a canonical type name.

        Performs whitespace-normalization and case-insensitive lookup against
        *known_types* (defaults to ``self.types``).  An optional *aliases* dict
        can map lower-cased raw values to canonical names before the type-key
        lookup.  Returns *fallback* (default ``""``) when no match is found.

        This covers the common pattern shared by Amapá, Bahia, Pará, and
        São Paulo.  Scrapers with extra logic (regex-based type codes, federal-
        type filtering) may still override or extend this method.
        """
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
    ) -> dict | None:
        """Download a state constitution, convert to markdown, save, and track.

        Used by Acre, Rondônia, Rio Grande do Sul, and Tocantins.

        Returns the saved document dict, or ``None`` if skipped/failed.
        """
        if self._is_already_scraped(url, title):
            if self.verbose:
                logger.info(f"Constitution already scraped, skipping: {title}")
            return None

        text_markdown, raw_content, content_ext = await self._download_and_convert(url)
        if not text_markdown or not text_markdown.strip():
            logger.error(f"Failed to get markdown for constitution: {title}")
            return None

        doc_info = {
            "year": year,
            "type": "Constituição Estadual",
            "title": title,
            "situation": DEFAULT_VALID_SITUATION,
            "text_markdown": text_markdown,
            "document_url": url,
            "_raw_content": raw_content,
            "_content_extension": content_ext,
            **extra,
        }

        saved = await self._save_doc_result(doc_info)
        if saved is not None:
            doc_info = saved
        self._track_results([doc_info])
        self.count += 1
        if self.verbose:
            logger.info(f"Fetched constitution: {title}")
        return doc_info
