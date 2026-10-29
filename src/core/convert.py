"""Content detection, cleaning, and conversion with explicit dependencies."""

import asyncio
import re
import string
from collections.abc import Awaitable, Callable
from pathlib import Path

import fitz
from bs4 import BeautifulSoup, Tag
from html_to_markdown import ConversionOptions
from html_to_markdown import convert as html_to_md
from loguru import logger

from src.core.http import HttpClient, Response
from src.core.images import inline_images_in_html
from src.core.ocr import OCR, clean_md_tag

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
    "(Est[ea]\\s+(texto|conte[uú]do)|Ess[ea]\\s+texto)\\s+n[aã]o\\s+substitui",
    re.IGNORECASE,
)
_CLEAN_NORM_EMPTY_TAGS = ["h1", "h2", "h3", "h4", "h5", "h6", "div", "span", "b"]
_IMAGE_MAGIC_BYTES: list[bytes] = [
    b"\x89PNG",
    b"\xff\xd8",
    b"GIF8",
    b"II*\x00",
    b"MM\x00*",
    b"BM",
    b"RIFF",
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
_INVISIBLE_TEXT_RE = re.compile(b"(?:\\s|^)3\\s+Tr(?:\\s|$)")


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


def is_pdf_scanned(content: bytes, max_pages_to_check: int = 10) -> tuple[bool, float]:
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
        return (False, 0.0)
    producer = doc.metadata.get("producer", "").lower() if doc.metadata else ""
    creator = doc.metadata.get("creator", "").lower() if doc.metadata else ""
    metadata_text = producer + " " + creator
    has_ocr_metadata = any((kw in metadata_text for kw in _OCR_METADATA_KEYWORDS))
    if total_pages <= max_pages_to_check:
        pages_to_check = list(range(total_pages))
    else:
        step = total_pages / max_pages_to_check
        pages_to_check = sorted(
            list(set((int(i * step) for i in range(max_pages_to_check))))
        )
    page_scores: list[float] = []
    for page_num in pages_to_check:
        page = doc[page_num]
        page_area = page.rect.width * page.rect.height
        if page_area <= 0:
            continue
        images = page.get_image_info()
        total_img_area = sum(
            (
                max(0, img["bbox"][2] - img["bbox"][0])
                * max(0, img["bbox"][3] - img["bbox"][1])
                for img in images
            )
        )
        coverage = min(1.0, total_img_area / page_area)
        text = page.get_text().strip()
        text_length = len(text)
        contents = page.read_contents()
        has_invisible_text = bool(_INVISIBLE_TEXT_RE.search(contents))
        score = 0.5
        is_blank = False
        if coverage > 0.8:
            if has_invisible_text or has_ocr_metadata:
                score = 1.0
            elif text_length < 50:
                score = 1.0
            elif text_length > 200:
                score = 0.9
            elif page.rect.width > page.rect.height:
                score = 0.2
            else:
                score = 0.8
        elif coverage > 0.1:
            if text_length < 50:
                score = 0.8
            else:
                score = 0.1
        elif text_length > 10:
            score = 0.0
        else:
            is_blank = True
        if not is_blank:
            page_scores.append(score)
    doc.close()
    if not page_scores:
        return (False, 1.0)
    avg_score = sum(page_scores) / len(page_scores)
    is_scanned = avg_score > 0.5
    confidence = avg_score if is_scanned else 1.0 - avg_score
    return (is_scanned, round(confidence, 4))


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


def clean_markdown(text: str, replace: list[tuple[str, str]] | None = None) -> str:
    """Clean markdown text by removing links and applying custom replacements."""
    text = clean_md_tag(text)
    text = re.sub("(?<!!)\\[([^\\]]*)\\]\\([^)]*\\)", "\\1", text)
    if replace:
        for find, replacement in replace:
            text = re.sub(find, replacement, text)
    return text.strip()


def strip_html_chrome(
    soup: BeautifulSoup | Tag, extra_selectors: list[dict] | None = None
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


def _remove_unwanted_tags(soup: BeautifulSoup | Tag, tag_names: list[str]) -> None:
    """Decompose all occurrences of the given tag names (img, style, script, etc.)."""
    if tag_names:
        for tag in soup.find_all(tag_names):
            tag.decompose()


def _strip_boilerplate(soup: BeautifulSoup | Tag) -> None:
    """Remove disclaimer paragraphs common in Brazilian legislation pages."""
    for tag in list(soup.find_all(["p", "span", "div"])):
        if tag.decomposed:
            continue
        txt = tag.get_text(strip=True)
        if _DISCLAIMER_RE.search(txt) and len(txt) < 300:
            tag.decompose()


def _clean_links(soup: BeautifulSoup | Tag) -> None:
    """Remove empty anchor tags and unwrap anchors that contain text."""
    for tag in list(soup.find_all("a")):
        if tag.decomposed:
            continue
        if not tag.get_text(strip=True) and (not tag.find("img")):
            tag.decompose()
        else:
            tag.unwrap()


def _unwrap_font_tags(soup: BeautifulSoup | Tag) -> None:
    """Unwrap ``<font>`` tags, keeping their inner content."""
    for tag in list(soup.find_all("font")):
        if not tag.decomposed:
            tag.unwrap()


def _remove_empty_elements(soup: BeautifulSoup | Tag) -> None:
    """Remove empty heading, div, span, and bold tags."""
    for tag in list(soup.find_all(_CLEAN_NORM_EMPTY_TAGS)):
        if tag.decomposed:
            continue
        if not tag.get_text(strip=True) and (not tag.find("img")):
            tag.decompose()


def _clean_attributes(soup: BeautifulSoup | Tag) -> None:
    """Strip inline ``style`` attributes from all remaining tags."""
    for tag in soup.find_all(style=True):
        del tag["style"]


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
    tags_to_remove: list[str] = []
    if remove_images:
        tags_to_remove.append("img")
    if remove_style_tags:
        tags_to_remove.append("style")
    if remove_script_tags:
        tags_to_remove.append("script")
    _remove_unwanted_tags(soup, tags_to_remove)
    if remove_disclaimers:
        _strip_boilerplate(soup)
    if unwrap_links:
        _clean_links(soup)
    if unwrap_fonts:
        _unwrap_font_tags(soup)
    if remove_empty_tags:
        _remove_empty_elements(soup)
    if strip_styles:
        _clean_attributes(soup)
    return soup


def infer_type_from_title(title: str, types: dict | list) -> str | None:
    """Return the first type whose name appears at the start of *title*."""
    type_names = list(types.keys()) if isinstance(types, dict) else list(types)
    for name in sorted(type_names, key=len, reverse=True):
        if title.lower().startswith(name.lower()):
            return name
    return None


def valid_markdown(text_markdown: str | None, min_length: int = 50) -> tuple[bool, str]:
    """Validate markdown text using common patterns found across all scrapers."""
    if not text_markdown:
        return (False, "text_markdown is None or empty")
    stripped = text_markdown.strip()
    if not stripped:
        return (False, "text_markdown is empty after strip")
    cleaned = stripped.translate(
        str.maketrans("", "", string.punctuation + string.whitespace)
    )
    if not cleaned:
        return (False, "text_markdown contains only punctuation/whitespace")
    lower = stripped.lower()
    for pattern in _SERVER_ERROR_PATTERNS:
        if pattern in lower:
            return (False, f"text_markdown contains server error: {pattern}")
    if len(stripped) < min_length:
        return (
            False,
            f"text_markdown too short ({len(stripped)} < {min_length} chars)",
        )
    return (True, "")


async def html_to_markdown(
    html: str, base_url: str, fetch_image: Callable[[str], Awaitable[bytes | None]]
) -> str:
    html = await inline_images_in_html(html, base_url, fetch_image)
    result = await asyncio.to_thread(
        html_to_md,
        html,
        ConversionOptions(heading_style="atx", strip_tags=["script", "style", "svg"]),
    )
    return clean_markdown(result or "")


_WATERMARK_CHECK_RE = re.compile("(?:\\n[^\\n]{0,2}){15,}")
_MD_BASE64_IMAGE_RE = re.compile("!\\[[^\\]]*\\]\\(data:image/[^)]+\\)")


def _digital_pdf(data: bytes, image_size_limit: float) -> str:
    import pymupdf4llm.helpers.pymupdf_rag

    with fitz.open(stream=data, filetype="pdf") as doc:
        try:
            return pymupdf4llm.helpers.pymupdf_rag.to_markdown(
                doc, embed_images=True, image_size_limit=image_size_limit
            )
        except Exception:
            return pymupdf4llm.helpers.pymupdf_rag.to_markdown(
                doc,
                embed_images=True,
                table_strategy=None,
                image_size_limit=image_size_limit,
            )


def _plain_pdf(data: bytes) -> str:
    with fitz.open(stream=data, filetype="pdf") as doc:
        return "\n\n".join((page.get_text() for page in doc))


async def pdf_to_markdown(
    data: bytes, ocr: OCR | None = None, *, image_size_limit: float = 0.1
) -> str:
    scanned, confidence = await asyncio.to_thread(is_pdf_scanned, data)
    count = await asyncio.to_thread(_pdf_page_count, data)
    minimum = max(50, count * 100)

    def usable(text: str) -> bool:
        check = _WATERMARK_CHECK_RE.sub("", _MD_BASE64_IMAGE_RE.sub("", text)).strip()
        return valid_markdown(check, min_length=minimum)[0]

    if not scanned and confidence >= 0.7:
        try:
            markdown = await asyncio.to_thread(_digital_pdf, data, image_size_limit)
            if usable(markdown):
                return clean_markdown(markdown)
        except Exception as exc:
            logger.debug(f"PDF markdown extraction failed: {exc}")
    plain = await asyncio.to_thread(_plain_pdf, data)
    if usable(plain):
        return clean_markdown(plain)
    if ocr is not None:
        return clean_markdown(await ocr.pdf(data))
    raise ValueError("PDF needs OCR but no OCR model is configured")


async def bytes_to_markdown(
    data: bytes, content_type: str, url: str, http: HttpClient, ocr: OCR | None = None
) -> str:
    if is_pdf(data, content_type):
        if not data.startswith(b"%PDF"):
            raise ValueError(f"Expected PDF content from {url}")
        return await pdf_to_markdown(data, ocr)
    if _is_image_bytes(data):
        if ocr is None:
            raise ValueError("Image needs OCR but no OCR model is configured")
        return clean_markdown(await ocr.images([data]))

    async def fetch_image(image_url: str) -> bytes:
        return (await http.request(image_url)).content

    response = Response(url, 200, {"Content-Type": content_type}, data)
    return await html_to_markdown(response.text, url, fetch_image)


def normalize_type(
    raw_type: str,
    known_types: dict | list,
    aliases: dict | None = None,
    fallback: str = "",
) -> str:
    cleaned = re.sub("\\s+", " ", str(raw_type or "")).strip()
    if not cleaned:
        return fallback
    lower = cleaned.casefold()
    if aliases and lower in aliases:
        return aliases[lower]
    return next(
        (key for key in known_types if lower == key.casefold()), fallback or cleaned
    )


def remove_summary_from_markdown(md: str, summary: str) -> str:
    """Remove the summary paragraph from text_markdown to avoid duplication.

    The Bahia document body (``field--name-body``) includes the ementa/summary
    as its second paragraph. Since the summary is already stored separately, it
    is stripped here to avoid repeating it verbatim inside ``text_markdown``.
    """
    if not summary:
        return md
    summary_norm = re.sub("\\s+", " ", summary).strip()
    paragraphs = md.split("\n\n")
    filtered = [
        p
        for p in paragraphs
        if re.sub("\\s+", " ", re.sub("[*_`]+", "", p)).strip() != summary_norm
    ]
    if len(filtered) == len(paragraphs):
        return md
    return "\n\n".join(filtered)


def remove_summary_element(container: BeautifulSoup | Tag, summary: str) -> None:
    """Remove the smallest summary container without deleting articles."""
    normalized = " ".join(summary.split()).casefold()
    if not normalized:
        return
    candidates = []
    for tag in container.find_all(["p", "td", "div", "span", "font", "table"]):
        text = " ".join(tag.get_text(" ", strip=True).split()).casefold()
        if normalized in text and "art." not in text:
            candidates.append((len(text), tag))
    if candidates:
        min(candidates, key=lambda item: item[0])[1].decompose()


def infer_type_by_prefix(title: str, types: dict | list) -> str:
    """Match the longest type prefix; mappings supply canonical labels."""
    normalized = " ".join(title.split()).casefold()
    for name in sorted(types, key=len, reverse=True):
        if normalized.startswith(name.casefold()):
            value = types[name] if isinstance(types, dict) else name
            return value if isinstance(value, str) else name
    return ""
