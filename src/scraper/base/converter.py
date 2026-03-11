"""Markdown conversion, HTML cleaning, and content validation.

All markdown/conversion logic extracted from BaseScraper lives here.
Access via ``self._converter`` on any BaseScraper subclass.
"""

from __future__ import annotations

import os
import re
import string

import aiohttp
import fitz

from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, cast

from bs4 import BeautifulSoup, Tag
from loguru import logger
from markitdown import MarkItDown
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.utils import clean_md_tag, run_in_thread

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
]

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
    """Wrap HTML fragment in <html><body> tags for markitdown conversion."""
    return f"<html><body>{content}</body></html>"


def clean_markdown(
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
    remove_images: bool = True,
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
    (``request_service``, ``ocr_service``) and ``_markitdown`` are
    always read from the scraper — compatible with ``object.__new__()``
    test instantiation.
    """

    _markitdown = MarkItDown()  # class-level singleton

    def __init__(self, scraper: BaseScraper):
        self._scraper = scraper

    # ------------------------------------------------------------------
    # Async methods — require services from the back-referenced scraper
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((OSError, RuntimeError)),
        reraise=True,
    )
    async def convert_to_md(
        self,
        source: BytesIO,
        filename: str = "document.html",
    ) -> str:
        """Convert a BytesIO stream to markdown via markitdown (with retry)."""
        _, ext = os.path.splitext(filename)
        if not ext:
            ext = ".html"

        markitdown = getattr(self._scraper, "_markitdown", self._markitdown)

        try:
            source.seek(0)
            result = await run_in_thread(
                markitdown.convert_stream,
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

    async def pdf_bytes_to_text(self, body: bytes) -> str:
        """Extract plain text from PDF bytes via PyMuPDF."""

        def _extract() -> str:
            doc = fitz.open(stream=body, filetype="pdf")
            try:
                pages: list[str] = []
                for page in doc:
                    text = str(page.get_text("text") or "").strip()
                    if text:
                        pages.append(text)
                return "\n\n".join(pages)
            finally:
                doc.close()

        try:
            return cast(str, await run_in_thread(_extract)).strip()
        except (OSError, ValueError, RuntimeError, TypeError) as e:
            logger.warning(f"PyMuPDF extraction failed: {e}")
            return ""

    async def bytes_to_markdown(
        self,
        body: bytes,
        filename: str = "document.pdf",
        content_type: str = "",
    ) -> str:
        """Convert raw bytes to markdown with markitdown, falling back to OCR for PDFs."""
        is_pdf_ = is_pdf(body, content_type)
        ocr_service = getattr(self._scraper, "ocr_service", None)

        try:
            text_markdown = await self.convert_to_md(
                BytesIO(body),
                filename=filename or ("document.pdf" if is_pdf_ else "document.html"),
            )
            if valid_markdown(text_markdown, min_length=50)[0]:
                return text_markdown.strip()
        except (OSError, ValueError, RuntimeError, TypeError) as e:
            logger.warning(f"markitdown extraction failed: {e}")

        if is_pdf_:
            fitz_text = await self.pdf_bytes_to_text(body)
            if valid_markdown(fitz_text, min_length=50)[0]:
                return fitz_text.strip()

        if is_pdf_ and ocr_service:
            return await ocr_service.pdf_to_markdown(body)

        if is_pdf_:
            logger.warning(
                "PDF extraction yielded little text and no OCR service configured."
            )
        return ""

    async def stream_to_markdown(
        self, stream: BytesIO, filename: str | None = None
    ) -> str:
        """Convert a raw byte stream (PDF or image) to markdown."""
        raw = stream.read()
        ocr_service = getattr(self._scraper, "ocr_service", None)

        if filename:
            ext = Path(filename).suffix.lower()
            is_image = ext in [".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".gif"]
        else:
            is_image = _is_image_bytes(raw)

        if is_image and ocr_service:
            return await ocr_service.images_to_markdown([raw])
        if is_image:
            logger.warning("No LLM OCR service configured; cannot process image.")
            return ""
        return await self.bytes_to_markdown(raw, filename=filename or "document.pdf")

    async def html_to_markdown(self, html_content: str) -> str:
        """Wrap an HTML fragment and convert it to cleaned markdown in one step."""
        wrapped = wrap_html(html_content)
        md = await self.convert_to_md(
            BytesIO(wrapped.encode("utf-8")),
            filename="document.html",
        )
        return clean_markdown(md)

    async def response_to_markdown(
        self,
        body: bytes,
        filename: str | None = None,
        content_type: str = "",
    ) -> str:
        """Convert raw response bytes to markdown via the standard pipeline."""
        used_filename = filename or (
            "document.pdf" if is_pdf(body, content_type) else "document.html"
        )
        return await self.bytes_to_markdown(
            body, filename=used_filename, content_type=content_type
        )

    async def response_or_url_to_markdown(
        self,
        response: aiohttp.ClientResponse | None,
        url: str | None,
        filename: str | None = None,
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
            body, filename=used_filename, content_type=content_type
        )

    async def get_markdown(
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
        try:
            if stream is not None:
                result = await self.stream_to_markdown(stream, filename)
            elif html_content is not None:
                buffer = BytesIO(html_content.encode("utf-8"))
                try:
                    result = await self.convert_to_md(buffer, filename="document.html")
                except (OSError, ValueError, RuntimeError, TypeError):
                    result = ""
            else:
                result = await self.response_or_url_to_markdown(response, url, filename)
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

        markdown = await self.response_to_markdown(body, filename, content_type)
        markdown = clean_markdown(markdown) if markdown else ""

        if is_pdf(body, content_type) and (not ext or ext == ".bin"):
            ext = ".pdf"

        return markdown, body, ext
