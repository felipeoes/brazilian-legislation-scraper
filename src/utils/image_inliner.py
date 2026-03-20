"""Async utility for inlining ``<img>`` sources as base64 data URIs."""

from __future__ import annotations

import asyncio
import base64
import mimetypes
from collections.abc import Awaitable, Callable
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from loguru import logger


async def inline_images_in_html(
    html: str,
    base_url: str,
    fetch_fn: Callable[[str], Awaitable[bytes | None]],
    *,
    max_concurrent: int = 10,
) -> str:
    """Replace every ``<img src>`` in *html* with an inline base64 data URI.

    Parameters
    ----------
    html:
        Raw HTML string.
    base_url:
        Used to resolve relative ``src`` attributes.
    fetch_fn:
        ``async (url) -> bytes | None`` — fetches image bytes.
        Return ``None`` to skip the image gracefully.
    max_concurrent:
        Maximum number of concurrent image fetches.

    Returns
    -------
    str
        HTML with images inlined. Failed fetches are left unchanged.
    """
    soup = BeautifulSoup(html, "html.parser")
    imgs = soup.find_all("img")
    if not imgs:
        return html

    sem = asyncio.Semaphore(max_concurrent)

    async def _fetch_one(img_tag: Tag) -> None:
        src = _get_image_source(img_tag)
        if not src:
            return

        full_url = src if src.startswith("http") else urljoin(base_url, src)

        try:
            async with sem:
                data = await fetch_fn(full_url)
        except Exception as exc:
            logger.debug(f"Image fetch failed for '{full_url[:80]}': {exc}")
            return

        if not data:
            return

        content_type = _guess_mime(full_url, data)
        b64 = base64.b64encode(data).decode("ascii")
        img_tag["src"] = f"data:{content_type};base64,{b64}"
        for attr in ("data-src", "srcset", "data-srcset"):
            img_tag.attrs.pop(attr, None)

    await asyncio.gather(*(_fetch_one(img) for img in imgs))
    return str(soup)


def _get_image_source(img_tag: Tag) -> str | None:
    """Return the best fetchable image source from an ``<img>`` or its picture."""
    direct_source = _first_fetchable_attr(img_tag, ("src", "data-src"))
    if direct_source:
        return direct_source

    srcset_source = _first_srcset_candidate(
        img_tag.get("srcset") or img_tag.get("data-srcset")
    )
    if srcset_source:
        return srcset_source

    parent = img_tag.parent
    if isinstance(parent, Tag) and parent.name == "picture":
        for source_tag in parent.find_all("source", recursive=False):
            direct_source = _first_fetchable_attr(source_tag, ("src", "data-src"))
            if direct_source:
                return direct_source
            srcset_source = _first_srcset_candidate(
                source_tag.get("srcset") or source_tag.get("data-srcset")
            )
            if srcset_source:
                return srcset_source

    return None


def _first_fetchable_attr(tag: Tag, attrs: tuple[str, ...]) -> str | None:
    for attr in attrs:
        value = tag.get(attr)
        if isinstance(value, list):
            value = " ".join(value)
        if not isinstance(value, str):
            continue
        value = value.strip()
        if value and _is_fetchable_source(value):
            return value
    return None


def _first_srcset_candidate(srcset: str | None) -> str | None:
    if not isinstance(srcset, str):
        return None

    for candidate in srcset.split(","):
        candidate = candidate.strip()
        if not candidate:
            continue
        source = candidate.split()[0].strip()
        if source and _is_fetchable_source(source):
            return source

    return None


def _is_fetchable_source(source: str) -> bool:
    lower_source = source.lower()
    return not lower_source.startswith(
        ("data:", "cid:", "javascript:", "about:", "file:")
    )


def _guess_mime(url: str, data: bytes) -> str:
    """Best-effort MIME type from URL extension or magic bytes."""
    mime, _ = mimetypes.guess_type(url.split("?")[0])
    if mime:
        return mime

    if data[:8].startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data[:2] == b"\xff\xd8":
        return "image/jpeg"
    if data[:4] == b"GIF8":
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"

    return "image/png"
