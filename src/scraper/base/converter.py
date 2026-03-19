"""Markdown conversion, HTML cleaning, and content validation.

All markdown/conversion logic extracted from BaseScraper lives here.
Access via ``self._converter`` on any BaseScraper subclass.
"""

from __future__ import annotations

import re
import string
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, cast

import aiohttp
import fitz
import pymupdf4llm
from bs4 import BeautifulSoup, Tag
from html_to_markdown import ConversionOptions
from html_to_markdown import convert as html_to_md
from loguru import logger

from src.utils import clean_md_tag, inline_images_in_html, run_in_thread

if TYPE_CHECKING:
    from src.scraper.base.scraper import BaseScraper

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_SERVER_ERROR_PATTERNS: list[str] = [
    "the requested url was not found on this server",
    "was not found on this server",
    "file or directory not found",
    "failed to open stream",
    "http request failed",
    "service unavailable",
    "doesn't work properly without javascript enabled",
    "object reference not set to an instance of an object",
]

# Matches runs of 15+ consecutive lines with ≤2 characters — the digital-authentication
# sidebar watermarks that PyMuPDF extracts as individual single-character lines when text
# is rotated. Used only for pre-validation length checks (not applied to returned text).
_WATERMARK_CHECK_RE = re.compile(r"(?:\n[^\n]{0,2}){15,}")

# Matches markdown base64 image syntax produced by pymupdf4llm when it embeds
# full-page images instead of extracting text (common with older PDF generators).
_MD_BASE64_IMAGE_RE = re.compile(r"!\[[^\]]*\]\(data:image/[^)]+\)")

_DISCLAIMER_RE = re.compile(
    r"(Est[ea]\s+(texto|conte[uú]do)|Ess[ea]\s+texto)\s+n[aã]o\s+substitui",
    re.IGNORECASE,
)

_CLEAN_NORM_EMPTY_TAGS = ["h1", "h2", "h3", "h4", "h5", "h6", "div", "span", "b"]


# ---------------------------------------------------------------------------
# Pure helper functions (no service access needed)
# ---------------------------------------------------------------------------


def is_pdf(body: bytes, content_type: str = "") -> bool:
    """Check if content is a PDF based on content type or magic bytes."""
    return "pdf" in content_type.lower() or body[:4] == b"%PDF"


def _expects_pdf(ext: str, content_type: str = "", url: str = "") -> bool:
    """Return True if the extension, content-type, or URL indicates PDF was expected."""
    return (
        ext.lower() == ".pdf"
        or "pdf" in (content_type or "").lower()
        or url.lower().endswith(".pdf")
    )


_IMAGE_MAGIC_BYTES: list[bytes] = [
    b"\x89PNG",  # PNG
    b"\xff\xd8",  # JPEG
    b"GIF8",  # GIF
    b"II*\x00",  # TIFF little-endian
    b"MM\x00*",  # TIFF big-endian
    b"BM",  # BMP
    b"RIFF",  # WebP (further check not needed for our purposes)
]


def _is_image_bytes(raw: bytes) -> bool:
    """Return True if *raw* starts with a known image-format magic sequence."""
    for magic in _IMAGE_MAGIC_BYTES:
        if raw[: len(magic)] == magic:
            return True
    return False


def _pdf_page_count(body: bytes) -> int:
    """Return the number of pages in a PDF, or 1 on any error."""
    try:
        doc = fitz.open(stream=body, filetype="pdf")
        count = doc.page_count
        doc.close()
        return max(1, count)
    except Exception:
        return 1


_OCR_METADATA_KEYWORDS: list[str] = [
    "tesseract",
    "ocr",
    "abbyy",
    "omnipage",
    "paper capture",
    "scanner",
    "scanned",
    "clearscan",
    "readiris",
    "iris",
]

# Regex to detect the invisible-text rendering operator (``3 Tr``) in raw PDF
# page streams.  Presence strongly indicates an OCR text layer overlaid on a
# scanned image.
_INVISIBLE_TEXT_RE = re.compile(rb"(?:\s|^)3\s+Tr(?:\s|$)")


def is_pdf_scanned(
    content: bytes,
    max_pages_to_check: int = 10,
) -> tuple[bool, float]:
    """Determine whether a PDF is primarily a scanned/image document.

    Analyses metadata, image coverage, text density and invisible-text
    operators on a sample of pages.

    Returns:
        ``(is_scanned, confidence)`` where *confidence* represents how
        certain the heuristic is about the result (0–1).
    """
    try:
        doc = fitz.open(stream=content, filetype="pdf")
    except Exception as exc:
        raise ValueError(f"Could not open PDF: {exc}") from exc

    total_pages = len(doc)
    if total_pages == 0:
        doc.close()
        return False, 0.0

    # 1. Metadata check for obvious OCR engines
    producer = doc.metadata.get("producer", "").lower() if doc.metadata else ""
    creator = doc.metadata.get("creator", "").lower() if doc.metadata else ""
    metadata_text = producer + " " + creator
    has_ocr_metadata = any(kw in metadata_text for kw in _OCR_METADATA_KEYWORDS)

    # 2. Sample pages (distributed evenly)
    if total_pages <= max_pages_to_check:
        pages_to_check = list(range(total_pages))
    else:
        step = total_pages / max_pages_to_check
        pages_to_check = sorted(
            list(set(int(i * step) for i in range(max_pages_to_check)))
        )

    page_scores: list[float] = []
    for page_num in pages_to_check:
        page = doc[page_num]
        page_area = page.rect.width * page.rect.height
        if page_area <= 0:
            continue

        # Image coverage
        images = page.get_image_info()
        total_img_area = sum(
            max(0, img["bbox"][2] - img["bbox"][0])
            * max(0, img["bbox"][3] - img["bbox"][1])
            for img in images
        )
        coverage = min(1.0, total_img_area / page_area)

        # Text density
        text = page.get_text().strip()
        text_length = len(text)

        # Invisible text operator (``3 Tr``) in raw page stream
        contents = page.read_contents()
        has_invisible_text = bool(_INVISIBLE_TEXT_RE.search(contents))

        # --- Scoring heuristic ---
        score = 0.5
        is_blank = False

        if coverage > 0.8:
            if has_invisible_text or has_ocr_metadata:
                score = 1.0
            elif text_length < 50:
                score = 1.0
            elif text_length > 200:
                score = 0.9
            else:
                if page.rect.width > page.rect.height:
                    score = 0.2  # landscape → likely a presentation slide
                else:
                    score = 0.8
        elif coverage > 0.1:
            if text_length < 50:
                score = 0.8
            else:
                score = 0.1
        else:
            if text_length > 10:
                score = 0.0
            else:
                is_blank = True

        if not is_blank:
            page_scores.append(score)

    doc.close()

    if not page_scores:
        return False, 1.0

    avg_score = sum(page_scores) / len(page_scores)
    is_scanned = avg_score > 0.5
    confidence = avg_score if is_scanned else (1.0 - avg_score)
    return is_scanned, round(confidence, 4)


def detect_extension(content_type: str, filename: str | None = None) -> str:
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


def wrap_html(content: str) -> str:
    """Wrap HTML fragment in ``<html><body>`` tags for conversion."""
    return f"<html><body>{content}</body></html>"


# Shared options for html-to-markdown conversion.
_HTML_TO_MD_OPTIONS = ConversionOptions(
    heading_style="atx",
    strip_tags=["script", "style", "svg"],
)


def clean_markdown(
    text: str,
    replace: list[tuple[str, str]] | None = None,
) -> str:
    """Clean markdown text by removing links and applying custom replacements."""
    text = clean_md_tag(text)
    # Negative lookbehind for '!' preserves image syntax ![alt](data:...)
    text = re.sub(r"(?<!!)\[([^\]]*)\]\([^)]*\)", r"\1", text)
    if replace:
        for find, replacement in replace:
            text = re.sub(find, replacement, text)
    return text.strip()


def strip_html_chrome(
    soup: BeautifulSoup | Tag,
    extra_selectors: list[dict] | None = None,
) -> BeautifulSoup | Tag:
    """Remove standard chrome tags (script, style, nav, header, footer, aside) from soup.

    Optionally remove elements matching *extra_selectors* (each a dict of
    ``find_all`` kwargs, e.g. ``{"class_": "rodapeTexto"}``).
    """
    for el in soup.find_all(["script", "style", "nav", "header", "footer", "aside"]):
        el.decompose()
    if extra_selectors:
        for selector in extra_selectors:
            for el in soup.find_all(True, **selector):
                el.decompose()
    return soup


def calc_pages(total: int, per_page: int) -> int:
    """Number of pages needed for *total* items at *per_page* each."""
    if total <= 0 or per_page <= 0:
        return 0
    return (total + per_page - 1) // per_page


def clean_norm_soup(
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
    """Content-level cleaning for norm text HTML.

    Complements ``strip_html_chrome()`` (structural cleanup) with
    content-level artifact removal common to Brazilian legislation pages.
    All options are independently toggleable.
    """
    # --- Pass 1: decompose simple leaf-level tags ---
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

    # --- Pass 2: combined content-level cleanup ---
    disclaimer_tags = {"p", "span", "div"} if remove_disclaimers else set()
    empty_tag_names = set(_CLEAN_NORM_EMPTY_TAGS) if remove_empty_tags else set()
    needs_combined = (
        remove_disclaimers
        or unwrap_links
        or unwrap_fonts
        or remove_empty_tags
        or strip_styles
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
                    continue
            if strip_styles and tag.get("style"):
                del tag["style"]

    return soup


def infer_type_from_title(title: str, types: dict | list) -> str | None:
    """Return the first type whose name appears at the start of *title*."""
    type_names = list(types.keys()) if isinstance(types, dict) else list(types)
    for name in sorted(type_names, key=len, reverse=True):
        if title.lower().startswith(name.lower()):
            return name
    return None


def valid_markdown(
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

        Runs the (synchronous) conversion off the event loop.
        """

        def _convert() -> str:
            doc = fitz.open(stream=body, filetype="pdf")
            try:
                return pymupdf4llm.helpers.pymupdf_rag.to_markdown(
                    doc, embed_images=True
                )
            finally:
                doc.close()

        try:
            result = await run_in_thread(_convert)
            return (result or "").strip()
        except Exception as e:
            logger.debug(f"pymupdf4llm conversion failed: {e}")
            return ""

    async def bytes_to_markdown(
        self,
        body: bytes,
        filename: str = "document.pdf",
        content_type: str = "",
        base_url: str | None = None,
    ) -> str:
        """Convert raw bytes to markdown.

        For PDFs, uses ``is_pdf_scanned`` to decide between pymupdf4llm
        (digital documents) and LLM OCR (scanned / low-confidence).
        Non-PDF HTML goes through ``html-to-markdown`` with image inlining.
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

        # ---- PDF pipeline ----
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

        # 2a. Digital PDF → pymupdf4llm (with OCR fallback on validation failure)
        if not use_ocr:
            text_markdown = await self._pymupdf4llm_convert(body)
            # Strip base64 images before validation — pymupdf4llm sometimes
            # returns only embedded page images with no extracted text (common
            # with older PDF generators like Acrobat Distiller 6).
            text_without_images = _MD_BASE64_IMAGE_RE.sub("", text_markdown)
            check_text = _WATERMARK_CHECK_RE.sub("", text_without_images).strip()
            if valid_markdown(check_text, min_length=min_length)[0]:
                return text_markdown.strip()

            # pymupdf4llm output failed validation — falling back to LLM OCR
            if ocr_service:
                return await ocr_service.pdf_to_markdown(body)
            logger.warning("pymupdf4llm output invalid and no OCR service configured.")
            return text_markdown.strip() if text_markdown else ""

        # 2b. Scanned / low-confidence → LLM OCR (with pymupdf4llm last resort)
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
