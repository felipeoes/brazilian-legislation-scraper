"""Shared test infrastructure for the Brazilian legislation scraper test suite."""

import sys
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

# Add tests/ to sys.path so subdir test files can import from conftest/base_tests
sys.path.insert(0, str(Path(__file__).resolve().parent))
# Add the project root to sys.path so 'src' can be imported
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from src.scraper.base.converter import MarkdownConverter
from src.scraper.base.persistence import PersistenceManager


@pytest.fixture
def sample_markdown_content():
    """Sample markdown content that resembles Brazilian legislation."""
    return """# DECRETO Nº 123/2022

**Ementa**: Dispõe sobre a organização administrativa do Poder Executivo.

**Art. 1º** - Ficam criadas as seguintes secretarias:
I - Secretaria de Administração;
II - Secretaria de Finanças;
III - Secretaria de Planejamento.

**Art. 2º** - As secretarias mencionadas no artigo anterior terão suas competências definidas em regulamento.

**Art. 3º** - Este decreto entra em vigor na data de sua publicação.

*Publicado no Diário Oficial em 15/12/2022*
"""


@pytest.fixture
def sample_html_content():
    """Sample HTML content that resembles Brazilian legislation pages."""
    return """<!DOCTYPE html>
<html>
<head>
    <title>Decreto nº 123/2022</title>
</head>
<body>
    <div class="documento">
        <h1>DECRETO Nº 123/2022</h1>
        <p class="ementa">Dispõe sobre a organização administrativa do Poder Executivo.</p>
        <div class="texto">
            <p><strong>Art. 1º</strong> - Ficam criadas as seguintes secretarias:</p>
            <ol>
                <li>Secretaria de Administração;</li>
                <li>Secretaria de Finanças;</li>
                <li>Secretaria de Planejamento.</li>
            </ol>
            <p><strong>Art. 2º</strong> - As secretarias mencionadas no artigo anterior terão suas competências definidas em regulamento.</p>
            <p><strong>Art. 3º</strong> - Este decreto entra em vigor na data de sua publicação.</p>
        </div>
        <p class="publicacao">Publicado no Diário Oficial em 15/12/2022</p>
    </div>
</body>
</html>"""


@pytest.fixture
def sample_pdf_bytes():
    """Sample PDF bytes for testing PDF processing."""
    # Minimal PDF header that looks like a real PDF
    return b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n/Pages 2 0 R\n>>\nendobj\n2 0 obj\n<<\n/Type /Pages\n/Kids [3 0 R]\n/Count 1\n>>\nendobj\n3 0 obj\n<<\n/Type /Page\n/Parent 2 0 R\n/MediaBox [0 0 612 792]\n>>\nendobj\nxref\n0 4\n0000000000 65535 f\n0000000009 00000 n\n0000000058 00000 n\n0000000115 00000 n\ntrailer\n<<\n/Size 4\n/Root 1 0 R\n>>\nstartxref\n174\n%%EOF"


@pytest.fixture
def sample_doc_info():
    """Sample document info dictionary used across scraper tests."""
    return {
        "id": "123",
        "number": "123",
        "year": "2022",
        "type": "Decreto",
        "title": "Decreto nº 123/2022",
        "summary": "Dispõe sobre a organização administrativa",
        "category": "Executivo",
        "publication_date": "2022-12-15",
        "document_url": "https://example.com/doc/123",
    }


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
    scraper.browser_service = None
    scraper.request_service = MagicMock()
    scraper.request_service.cleanup = AsyncMock()
    scraper._markitdown = MagicMock()
    scraper._converter = MarkdownConverter(scraper)
    scraper._persister = PersistenceManager(scraper)
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
    scraper._fetch_soup_and_mhtml = AsyncMock(side_effect=Exception("fetch failed"))
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
