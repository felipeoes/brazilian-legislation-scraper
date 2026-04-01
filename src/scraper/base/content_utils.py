"""Pure helper functions for content detection, cleaning, and validation.

These module-level utilities were extracted from ``converter.py`` so that
``MarkdownConverter`` (the async class that depends on scraper services)
lives in its own module while stateless helpers can be imported without
pulling in heavy async dependencies.
"""

from __future__ import annotations

import re
import string
from pathlib import Path

import fitz
from bs4 import BeautifulSoup, Tag

from src.utils import clean_md_tag

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

_DISCLAIMER_RE = re.compile(
    r"(Est[ea]\s+(texto|conte[uú]do)|Ess[ea]\s+texto)\s+n[aã]o\s+substitui",
    re.IGNORECASE,
)

_CLEAN_NORM_EMPTY_TAGS = ["h1", "h2", "h3", "h4", "h5", "h6", "div", "span", "b"]

_IMAGE_MAGIC_BYTES: list[bytes] = [
    b"\x89PNG",  # PNG
    b"\xff\xd8",  # JPEG
    b"GIF8",  # GIF
    b"II*\x00",  # TIFF little-endian
    b"MM\x00*",  # TIFF big-endian
    b"BM",  # BMP
    b"RIFF",  # WebP (further check not needed for our purposes)
]

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
