"""Shared test infrastructure for the Brazilian legislation scraper test suite."""

import sys
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

# Add the project root to sys.path so 'src' can be imported
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def make_base_scraper(
    cls,
    base_url: str,
    name: str,
    types: dict | list,
    situations: dict | None = None,
    **overrides: Any,
):
    """Instantiate any scraper class bypassing ``__init__`` (no network, no I/O).

    Sets all attributes that every scraper needs so per-scraper helpers only
    have to supply the few scraper-specific fields.

    Usage::

        def _make_scraper(**kwargs):
            return make_base_scraper(
                BahiaLegislaScraper,
                "https://...",
                "BAHIA",
                TYPES,
                **kwargs,
            )
    """
    scraper = object.__new__(cls)
    scraper.base_url = base_url
    scraper.name = name
    scraper.types = types
    scraper.situations = situations if situations is not None else {}
    scraper.verbose = False
    scraper.overwrite = False
    scraper._scraped_keys = set()
    scraper.count = 0
    scraper.error_count = 0
    scraper._types_summary = {}
    scraper.saver = None
    scraper.ocr_service = None
    scraper.request_service = MagicMock()
    scraper._markitdown = MagicMock()
    for key, value in overrides.items():
        setattr(scraper, key, value)
    return scraper


def make_failed_request(
    url: str = "https://example.com/failed",
    reason: str = "Connection error",
    status: int | None = None,
):
    """Return a falsy ``FailedRequest``-like mock sentinel.

    Matches the behaviour of ``src.services.request.service.FailedRequest``:
    the object is falsy (``bool(obj)`` returns ``False``) and exposes
    ``url``, ``status``, and ``reason`` attributes.
    """
    fr = MagicMock()
    fr.__bool__ = MagicMock(return_value=False)
    fr.url = url
    fr.status = status
    fr.reason = reason
    return fr


def make_mock_response(
    body: str | bytes = b"",
    status: int = 200,
    content_type: str = "text/html",
) -> MagicMock:
    """Return a mock ``aiohttp.ClientResponse``-like object.

    The mock supports ``read()`` (returns bytes), ``text()`` (returns str),
    ``status``, and basic ``headers`` / ``content_type``.
    """
    import asyncio

    raw = body.encode("utf-8") if isinstance(body, str) else body

    resp = MagicMock()
    resp.status = status
    resp.content_type = content_type
    resp.headers = {"Content-Type": content_type}

    read_fut: asyncio.Future[bytes] = asyncio.Future()
    read_fut.set_result(raw)
    resp.read = MagicMock(return_value=read_fut)

    text_fut: asyncio.Future[str] = asyncio.Future()
    text_fut.set_result(raw.decode("utf-8", errors="replace"))
    resp.text = MagicMock(return_value=text_fut)

    return resp


def make_mock_json_response(
    payload: dict | list,
    status: int = 200,
) -> MagicMock:
    """Return a mock response whose ``.json()`` resolves to *payload*."""
    import asyncio
    import json

    resp = make_mock_response(
        body=json.dumps(payload).encode(),
        status=status,
        content_type="application/json",
    )
    json_fut: asyncio.Future[dict | list] = asyncio.Future()
    json_fut.set_result(payload)
    resp.json = MagicMock(return_value=json_fut)
    return resp


async def assert_resume_skips(scraper, doc_info: dict) -> None:
    """Assert that ``_get_doc_data`` returns ``None`` and skips fetch for already-scraped docs."""
    scraper._is_already_scraped = MagicMock(return_value=True)
    result = await scraper._get_doc_data(doc_info)
    assert result is None
    scraper.request_service.make_request.assert_not_called()


async def assert_fetch_failure_saves_error(scraper, doc_info: dict) -> None:
    """Assert that ``_get_doc_data`` handles fetch failure correctly."""
    scraper._is_already_scraped = MagicMock(return_value=False)
    scraper._save_doc_error = AsyncMock()
    scraper.request_service.make_request = AsyncMock(return_value=make_failed_request())
    result = await scraper._get_doc_data(doc_info)
    assert result is None
    scraper._save_doc_error.assert_called_once()


@pytest.fixture
def failed_request():
    """Pytest fixture that returns a falsy FailedRequest-like mock."""
    return make_failed_request()


@pytest.fixture
def integration_scraper_factory():
    """Factory fixture for integration tests.

    Yields a callable that creates a scraper in a temp directory and cleans
    it up afterwards.  Typical usage::

        async def test_something(integration_scraper_factory):
            async with integration_scraper_factory(MyScraper, verbose=False) as scraper:
                result = await scraper.some_method()
                assert result
    """
    import contextlib

    @contextlib.asynccontextmanager
    async def _factory(scraper_cls, **kwargs):
        with tempfile.TemporaryDirectory() as tmp:
            kwargs.setdefault("verbose", False)
            scraper = scraper_cls(docs_save_dir=tmp, **kwargs)
            try:
                yield scraper
            finally:
                try:
                    await scraper.cleanup()
                except Exception:
                    pass

    return _factory
