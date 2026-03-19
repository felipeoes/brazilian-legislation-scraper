"""Async utility for inlining ``<img>`` sources as base64 data URIs."""

from __future__ import annotations

import asyncio
import base64
import mimetypes
from collections.abc import Awaitable, Callable
from urllib.parse import urljoin

from bs4 import BeautifulSoup
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

    async def _fetch_one(img_tag: BeautifulSoup) -> None:
        src = img_tag.get("src")
        if not src or src.startswith("data:"):
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

    await asyncio.gather(*(_fetch_one(img) for img in imgs))
    return str(soup)


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
