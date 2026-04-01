"""Markdown conversion, HTML cleaning, and content validation.

All markdown/conversion logic extracted from BaseScraper lives here.
Access via ``self._converter`` on any BaseScraper subclass.
"""

from __future__ import annotations

import re
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, cast

import aiohttp
import fitz
import pymupdf4llm.helpers.pymupdf_rag
from html_to_markdown import ConversionOptions
from html_to_markdown import convert as html_to_md
from loguru import logger
from markitdown import MarkItDown

from src.scraper.base.content_utils import (
    _expects_pdf,
    _is_image_bytes,
    _pdf_page_count,
    calc_pages,
    clean_markdown,
    clean_norm_soup,
    detect_extension,
    infer_type_from_title,
    is_pdf,
    is_pdf_scanned,
    strip_html_chrome,
    valid_markdown,
    wrap_html,
)
from src.utils import inline_images_in_html, run_in_thread

if TYPE_CHECKING:
    from src.scraper.base.scraper import BaseScraper

# ---------------------------------------------------------------------------
# Module-level constants (used only by MarkdownConverter)
# ---------------------------------------------------------------------------

# Matches runs of 15+ consecutive lines with ≤2 characters — the digital-authentication
# sidebar watermarks that PyMuPDF extracts as individual single-character lines when text
# is rotated. Used only for pre-validation length checks (not applied to returned text).
_WATERMARK_CHECK_RE = re.compile(r"(?:\n[^\n]{0,2}){15,}")

# Matches markdown base64 image syntax produced by pymupdf4llm when it embeds
# full-page images instead of extracting text (common with older PDF generators).
_MD_BASE64_IMAGE_RE = re.compile(r"!\[[^\]]*\]\(data:image/[^)]+\)")

# Shared options for html-to-markdown conversion.
_HTML_TO_MD_OPTIONS = ConversionOptions(
    heading_style="atx",
    strip_tags=["script", "style", "svg"],
)

# Re-export all 13 utility functions for backward compatibility.
__all__ = [
    "calc_pages",
    "clean_markdown",
    "clean_norm_soup",
    "detect_extension",
    "infer_type_from_title",
    "is_pdf",
    "is_pdf_scanned",
    "strip_html_chrome",
    "valid_markdown",
    "wrap_html",
    "_expects_pdf",
    "_is_image_bytes",
    "_pdf_page_count",
    "MarkdownConverter",
]


class MarkdownConverter:
    """Handles all markdown conversion, HTML cleaning, and content validation.

    Uses a back-reference to the owning scraper so that services
    (``request_service``, ``ocr_service``) are always read from the
    scraper — compatible with ``object.__new__()`` test instantiation.
    """

    def __init__(self, scraper: BaseScraper):
        self._scraper = scraper

    # ------------------------------------------------------------------
    # Async methods — require services from the back-referenced scraper
    # ------------------------------------------------------------------

    # ---------- HTML conversion via html-to-markdown --------------------

    async def _convert_html_to_md(self, html: str) -> str:
        """Convert an HTML string to markdown via ``html-to-markdown``."""
        try:
            result = await run_in_thread(html_to_md, html, _HTML_TO_MD_OPTIONS)
            return (result or "").strip()
        except Exception as e:
            logger.debug(f"html-to-markdown conversion failed: {e}")
            return ""

    async def _create_image_fetcher(self):
        """Return an ``async (url) -> bytes | None`` using RequestService."""
        request_service = self._scraper.request_service

        async def _fetch(url: str) -> bytes | None:
            result = await request_service.fetch_bytes(url)
            if not result or not isinstance(result, tuple):
                return None
            body, resp = result
            if resp.status >= 400:
                return None
            return body

        return _fetch

    async def _convert_html_with_images(
        self,
        html: str,
        base_url: str | None = None,
    ) -> str:
        """Inline images as base64 then convert HTML to markdown."""
        resolved_base = base_url or getattr(self._scraper, "base_url", "")
        if resolved_base:
            fetcher = await self._create_image_fetcher()
            html = await inline_images_in_html(html, resolved_base, fetcher)
        return await self._convert_html_to_md(html)

    async def _pymupdf4llm_convert(self, body: bytes) -> str:
        """Convert PDF bytes to markdown via pymupdf4llm.

        Uses the ``pymupdf_rag`` backend directly to avoid pymupdf4llm 1.27+'s
        layout engine, which requires Tesseract even for digital PDFs. Scanned
        PDFs are handled upstream via ``LLMOCRService``.

        On failure (e.g. ``extractRAWDICT`` errors from table parsing), retries
        with ``table_strategy=None`` to skip table detection entirely.

        Runs the (synchronous) conversion off the event loop.
        """
        image_size_limit = getattr(self._scraper, "_pymupdf_image_size_limit", 0.1)

        def _convert() -> str:
            doc = fitz.open(stream=body, filetype="pdf")
            try:
                try:
                    return pymupdf4llm.helpers.pymupdf_rag.to_markdown(
                        doc,
                        embed_images=True,
                        image_size_limit=image_size_limit,
                    )
                except Exception:
                    # Retry without table detection — works around
                    # extractRAWDICT / min() errors on malformed tables.
                    return pymupdf4llm.helpers.pymupdf_rag.to_markdown(
                        doc,
                        embed_images=True,
                        table_strategy=None,
                        image_size_limit=image_size_limit,
                    )
            finally:
                doc.close()

        try:
            result = await run_in_thread(_convert)
            return (result or "").strip()
        except Exception as e:
            logger.debug(f"pymupdf4llm conversion failed: {e}")
            return ""

    async def _markitdown_convert(self, body: bytes) -> str:
        """Convert PDF bytes to markdown via Microsoft's markitdown library.

        Used as an intermediate fallback when pymupdf4llm produces only
        embedded images instead of extracting text.  markitdown is
        synchronous, so the call is offloaded via ``run_in_thread()``.
        """

        def _convert() -> str:
            md = MarkItDown()
            result = md.convert_stream(BytesIO(body), file_extension=".pdf")
            return result.markdown

        try:
            result = await run_in_thread(_convert)
            return (result or "").strip()
        except Exception as e:
            logger.debug(f"markitdown conversion failed: {e}")
            return ""

    async def bytes_to_markdown(
        self,
        body: bytes,
        filename: str = "document.pdf",
        content_type: str = "",
        base_url: str | None = None,
    ) -> str:
        """Convert raw bytes to markdown.

        For PDFs the conversion priority is:
        **pymupdf4llm → markitdown → LLM OCR**.

        ``is_pdf_scanned`` decides the initial strategy: digital PDFs try
        pymupdf4llm first, while scanned PDFs skip straight to markitdown
        and then LLM OCR.  Non-PDF HTML goes through ``html-to-markdown``
        with image inlining.
        """
        is_pdf_ = is_pdf(body, content_type)

        # ---- Non-PDF: convert via html-to-markdown ----
        if not is_pdf_:
            try:
                html_str = body.decode("utf-8", errors="replace")
                text_markdown = await self._convert_html_with_images(html_str, base_url)
                check_text = _WATERMARK_CHECK_RE.sub("", text_markdown).strip()
                if valid_markdown(check_text, min_length=50)[0]:
                    return text_markdown.strip()
            except Exception as e:
                logger.debug(f"HTML-to-markdown conversion failed: {e}")
            return ""

        # ---- PDF pipeline: pymupdf4llm → markitdown → LLM OCR ----
        min_length = 50
        try:
            page_count = await run_in_thread(_pdf_page_count, body)
            min_length = max(50, page_count * 100)
        except Exception:
            pass

        ocr_service = getattr(self._scraper, "ocr_service", None)

        # 1. Determine if the PDF is scanned
        try:
            scanned, confidence = await run_in_thread(is_pdf_scanned, body)
        except Exception as e:
            logger.debug(f"Scan detection failed, assuming scanned: {e}")
            scanned, confidence = True, 0.5

        use_ocr = scanned or confidence < 0.7

        # 2a. Digital PDF → pymupdf4llm first
        if not use_ocr:
            text_markdown = await self._pymupdf4llm_convert(body)
            # Strip base64 images before validation — pymupdf4llm sometimes
            # returns only embedded page images with no extracted text (common
            # with older PDF generators like Acrobat Distiller 6).
            text_without_images = _MD_BASE64_IMAGE_RE.sub("", text_markdown)
            check_text = _WATERMARK_CHECK_RE.sub("", text_without_images).strip()
            if valid_markdown(check_text, min_length=min_length)[0]:
                return text_markdown.strip()

            # pymupdf4llm output failed validation — try markitdown
            text_markdown = await self._markitdown_convert(body)
            check_text = _WATERMARK_CHECK_RE.sub("", text_markdown).strip()
            if valid_markdown(check_text, min_length=min_length)[0]:
                return text_markdown.strip()

            # markitdown also failed — fall back to LLM OCR
            if ocr_service:
                logger.debug(
                    "PDF appears digital but text extraction failed — trying LLM OCR."
                )
                return await ocr_service.pdf_to_markdown(body)
            logger.warning(
                "pymupdf4llm and markitdown output invalid and no OCR service configured."
            )
            return text_markdown.strip() if text_markdown else ""

        # 2b. Scanned / low-confidence → markitdown first (free), then LLM OCR
        text_markdown = await self._markitdown_convert(body)
        check_text = _WATERMARK_CHECK_RE.sub("", text_markdown).strip()
        if valid_markdown(check_text, min_length=min_length)[0]:
            return text_markdown.strip()

        if ocr_service:
            return await ocr_service.pdf_to_markdown(body)

        # No OCR service — try pymupdf4llm as degraded fallback
        logger.warning(
            "PDF appears scanned but no OCR service configured — "
            "trying pymupdf4llm as fallback."
        )
        text_markdown = await self._pymupdf4llm_convert(body)
        if text_markdown:
            return text_markdown.strip()

        logger.warning(
            "PDF extraction yielded little text and no OCR service configured."
        )
        return ""

    async def stream_to_markdown(
        self, stream: BytesIO, filename: str | None = None
    ) -> str:
        """Convert a raw byte stream (PDF or image) to markdown."""
        raw = stream.read()

        if filename:
            ext = Path(filename).suffix.lower()
            is_image = ext in [".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".gif"]
        else:
            is_image = _is_image_bytes(raw)

        if is_image and self._scraper.ocr_service:
            return await self._scraper.ocr_service.images_to_markdown([raw])
        if is_image:
            logger.warning("No LLM OCR service configured; cannot process image.")
            return ""
        return await self.bytes_to_markdown(raw, filename=filename or "document.pdf")

    async def html_to_markdown(self, html_content: str) -> str:
        """Convert an HTML fragment to cleaned markdown in one step."""
        md = await self._convert_html_to_md(html_content)
        return clean_markdown(md)

    async def response_to_markdown(
        self,
        body: bytes,
        filename: str | None = None,
        content_type: str = "",
        base_url: str | None = None,
    ) -> str:
        """Convert raw response bytes to markdown via the standard pipeline."""
        used_filename = filename or (
            "document.pdf" if is_pdf(body, content_type) else "document.html"
        )
        return await self.bytes_to_markdown(
            body,
            filename=used_filename,
            content_type=content_type,
            base_url=base_url,
        )

    async def response_or_url_to_markdown(
        self,
        response: aiohttp.ClientResponse | None,
        url: str | None,
        filename: str | None = None,
        base_url: str | None = None,
    ) -> str:
        """Fetch (if needed) and convert an HTTP response to markdown."""
        request_service = self._scraper.request_service
        resp = response
        if not resp and url:
            resp = await request_service.make_request(url)
        if not resp:
            return ""
        client_resp = cast(aiohttp.ClientResponse, resp)
        try:
            body = await client_resp.read()
        except aiohttp.ClientPayloadError:
            return ""
        if not body:
            return ""
        resp_filename, content_type = request_service.detect_content_info(client_resp)
        used_filename = (
            filename
            or resp_filename
            or ("document.pdf" if is_pdf(body, content_type) else "document.html")
        )
        return await self.bytes_to_markdown(
            body,
            filename=used_filename,
            content_type=content_type,
            base_url=base_url,
        )

    async def get_markdown(
        self,
        url: str | None = None,
        response: aiohttp.ClientResponse | None = None,
        stream: BytesIO | None = None,
        html_content: str | None = None,
        filename: str | None = None,
        base_url: str | None = None,
    ) -> str:
        """Get markdown from various input sources.

        Uses pymupdf4llm for PDFs and html-to-markdown for HTML.
        Priority: stream > html_content > response > url
        """
        try:
            if stream is not None:
                result = await self.stream_to_markdown(stream, filename)
            elif html_content is not None:
                result = await self._convert_html_with_images(html_content, base_url)
            else:
                result = await self.response_or_url_to_markdown(
                    response, url, filename, base_url
                )
        except Exception as e:
            logger.error(f"Error converting to markdown: {e}")
            return ""

        return clean_markdown(result) if result else ""

    async def download_and_convert(
        self,
        url: str,
    ) -> tuple[str, bytes, str]:
        """Download content from URL, convert to markdown, and return raw bytes.

        Returns:
            Tuple of ``(markdown, raw_bytes, file_extension)``.
        """
        request_service = self._scraper.request_service
        result = await request_service.fetch_bytes(url)
        if not result:
            return "", b"", ""
        if not isinstance(result, tuple):
            return "", b"", ""

        body, client_resp = result
        if client_resp.status >= 400:
            logger.warning(f"HTTP {client_resp.status} fetching {url} — skipping")
            return "", b"", ""
        filename, content_type = request_service.detect_content_info(client_resp)
        ext = detect_extension(content_type, filename)

        if body and _expects_pdf(ext, content_type, url) and body[:4] != b"%PDF":
            snippet = body[:200].decode("utf-8", errors="replace")
            logger.warning(
                f"Expected PDF but received non-PDF content from {url} "
                f"(content_type={content_type!r}, ext={ext!r}, "
                f"first_bytes={snippet!r})"
            )
            return "", b"", ""

        markdown = await self.response_to_markdown(
            body, filename, content_type, base_url=url
        )
        markdown = clean_markdown(markdown) if markdown else ""

        if is_pdf(body, content_type) and (not ext or ext == ".bin"):
            ext = ".pdf"

        return markdown, body, ext
