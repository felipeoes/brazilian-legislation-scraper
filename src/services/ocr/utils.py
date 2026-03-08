"""Shared utilities for OCR service backends."""

from __future__ import annotations

import re

_DATA_URI_RE = re.compile(r"data:image/([a-zA-Z0-9]+);base64,(.+)", re.DOTALL)


def parse_base64_data_uri(data_url: str) -> tuple[str, str]:
    """Parse a ``data:image/…;base64,…`` URI.

    Returns:
        A ``(format, base64_data)`` tuple.  If the URI does not match the
        expected pattern the whole string is returned as base64 data with
        ``"png"`` as the assumed format.
    """
    match = _DATA_URI_RE.match(data_url)
    if match:
        return match.group(1).lower(), match.group(2)
    return "png", data_url
