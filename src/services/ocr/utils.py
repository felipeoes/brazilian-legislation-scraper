"""Shared utilities for OCR service backends."""

from __future__ import annotations

import re

_DATA_URI_RE = re.compile(r"data:image/([a-zA-Z0-9]+);base64,(.*)", re.DOTALL)


def parse_base64_data_uri(data_url: str) -> tuple[str, str]:
    """Parse a ``data:image/…;base64,…`` URI.

    Returns:
        A ``(format, base64_data)`` tuple where ``base64_data`` may be empty
        for zero-byte images.

    Raises:
        ValueError: If *data_url* is not a valid data URI.
    """
    match = _DATA_URI_RE.match(data_url)
    if not match:
        raise ValueError(f"Not a valid data URI: {data_url[:60]!r}")
    return match.group(1).lower(), match.group(2).strip()
