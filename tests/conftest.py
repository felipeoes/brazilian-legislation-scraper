"""Fakes use the normal scraper constructor and buffered HTTP responses."""

import json
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from src.core.browser import BrowserPool
from src.core.http import HttpClient, Response
from src.core.store import Store

sys.path.insert(0, str(Path(__file__).parent))


def make_scraper(cls, **overrides):
    http = HttpClient(rps=1000, max_retries=1)
    http.request = AsyncMock(
        side_effect=AssertionError("Unexpected network request in unit test")
    )
    browser = BrowserPool(limiter=http.limiter)
    browser.snapshot = AsyncMock(
        side_effect=AssertionError("Unexpected browser request in unit test")
    )
    scraper = cls(
        store=Store(":memory:"), http=http, browser=browser, years=range(2024, 2025)
    )
    for key, value in overrides.items():
        setattr(scraper, key, value)
    return scraper


def make_mock_response(body=b"", status=200, content_type="text/html"):
    return Response(
        "https://example.org",
        status,
        {"Content-Type": content_type},
        body.encode() if isinstance(body, str) else body,
    )


def make_mock_json_response(payload, status=200):
    return make_mock_response(json.dumps(payload), status, "application/json")


@pytest.fixture
def integration_scraper_factory(tmp_path):
    @asynccontextmanager
    async def factory(cls, year=2024):
        http = HttpClient(rps=1, max_retries=2)
        async with cls(
            store=Store(tmp_path / cls.name / "documents.db"),
            http=http,
            years=range(year, year + 1),
            workers=2,
        ) as scraper:
            yield scraper

    return factory


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
