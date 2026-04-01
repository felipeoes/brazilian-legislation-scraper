"""Safety-net tests for SAPLBaseScraper (src/scraper/base/sapl_scraper.py).

Tests cover:
- _find_content_start(): title-matching heuristics and edge cases (CC=26)
- _get_docs_links(): SAPL API JSON parsing into doc-info dicts
- _get_doc_data(): successful fetch, failed PDF, already-scraped skip
- _scrape_year(): paginated API stream → grouped processing

Run with:
    uv run pytest tests/scrapers/base/test_sapl_scraper.py -v --tb=short -q
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import make_base_scraper, make_failed_request, make_mock_json_response

from src.scraper.base.sapl_scraper import SAPLBaseScraper

# ---------------------------------------------------------------------------
# Minimal concrete subclass (SAPLBaseScraper is not directly instantiable
# because it relies on __init__ wiring from StateScraper).
# ---------------------------------------------------------------------------

TYPES: dict[str, int] = {
    "Lei Ordinária": 2,
    "Lei Complementar": 1,
    "Decreto": 3,
}


def _make_scraper(**kwargs) -> SAPLBaseScraper:
    """Create a SAPLBaseScraper instance bypassing __init__."""
    kwargs.setdefault("subjects", {})
    kwargs.setdefault("_id_to_type", {v: k for k, v in TYPES.items()})
    kwargs.setdefault("_page_size", 100)
    return make_base_scraper(
        SAPLBaseScraper,
        "https://sapl.example.leg.br",
        "TEST_SAPL",
        TYPES,
        **kwargs,
    )


def _make_api_item(
    doc_id: int = 1,
    texto_integral: str | None = "https://sapl.example.leg.br/media/lei.pdf",
    tipo: int = 2,
    title_str: str = "Lei Ordinária 1/2023",
    ano: int = 2023,
    **kwargs,
) -> dict:
    """Build a single SAPL API result item."""
    return {
        "id": doc_id,
        "numero": str(doc_id),
        "__str__": title_str,
        "link_detail_backend": f"/norma/{doc_id}",
        "ementa": "Ementa da norma.",
        "assuntos": [],
        "data": "2023-01-01",
        "ano": ano,
        "esfera_federacao": None,
        "veiculo_publicacao": None,
        "texto_integral": texto_integral,
        "data_vigencia": None,
        "tipo": tipo,
        **kwargs,
    }


# ===================================================================
# _find_content_start()
# ===================================================================


class TestFindContentStart:
    """Tests for _find_content_start (CC=26, ~83 lines)."""

    def _find(self, lines: list[str], expected_title: str | None = None) -> int:
        scraper = _make_scraper()
        return scraper._find_content_start(lines, expected_title)

    def test_exact_title_match(self):
        lines = [
            "GOVERNO DO ESTADO",
            "GABINETE DO GOVERNADOR",
            "Lei Ordinária nº 123/2023",
            "Art. 1º Fica criado...",
        ]
        idx = self._find(lines, expected_title="Lei Ordinária nº 123/2023")
        assert idx == 2

    def test_title_match_case_insensitive(self):
        lines = [
            "Cabeçalho qualquer",
            "LEI ORDINÁRIA Nº 123/2023",
            "Art. 1º Algo",
        ]
        idx = self._find(lines, expected_title="Lei Ordinária nº 123/2023")
        assert idx == 1

    def test_fuzzy_title_score_match(self):
        """When exact match fails, token scoring picks the best candidate."""
        lines = [
            "Página do estado",
            "LEI COMPLEMENTAR 456 de 2023",
            "Art. 1º Texto.",
        ]
        idx = self._find(lines, expected_title="Lei Complementar nº 456/2023")
        assert idx == 1

    def test_fallback_to_numbered_sapl_title(self):
        """Without an expected title, picks the last numbered SAPL title line."""
        lines = [
            "GOVERNO DO ESTADO",
            "DECRETO 99/2022",
            "Art. 1º Algo qualquer.",
        ]
        idx = self._find(lines, expected_title=None)
        assert idx == 1

    def test_returns_zero_for_empty_lines(self):
        assert self._find([], expected_title="Decreto 1/2023") == 0

    def test_returns_zero_when_no_markers(self):
        """Lines with no SAPL title patterns should return 0."""
        lines = ["Algum texto qualquer", "Mais texto sem padrão"]
        assert self._find(lines, expected_title=None) == 0

    def test_prefers_candidate_before_first_article(self):
        """When multiple title-like lines exist, picks one before Art. 1º."""
        lines = [
            "LEI 10/2020",
            "Art. 1º Primeiro artigo.",
            "LEI 20/2020",
            "Art. 2º Segundo artigo.",
        ]
        idx = self._find(lines, expected_title=None)
        assert idx == 0

    def test_title_starts_with_expected(self):
        """A line that starts with the expected title should be matched."""
        lines = [
            "Preâmbulo",
            "Decreto nº 5 - Regulamenta a Lei X",
            "Art. 1º Texto.",
        ]
        idx = self._find(lines, expected_title="Decreto nº 5")
        assert idx == 1


# ===================================================================
# _get_docs_links()
# ===================================================================


class TestGetDocsLinks:
    """Tests for _get_docs_links — JSON parsing from SAPL API."""

    @pytest.mark.asyncio
    async def test_parses_single_item(self):
        scraper = _make_scraper()
        item = _make_api_item(doc_id=10, tipo=1)
        data = {"results": [item]}

        result = await scraper._get_docs_links("https://api.example.com/", data=data)

        assert len(result) == 1
        assert result[0]["id"] == 10
        assert result[0]["tipo_id"] == 1
        assert result[0]["pdf_link"] == item["texto_integral"]
        assert result[0]["title"] == "Lei Ordinária 1/2023"

    @pytest.mark.asyncio
    async def test_skips_item_without_texto_integral(self):
        scraper = _make_scraper()
        scraper._save_doc_error = AsyncMock()
        item = _make_api_item(texto_integral=None)
        data = {"results": [item]}

        result = await scraper._get_docs_links("https://api.example.com/", data=data)

        assert result == []
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_fetches_from_url_when_no_data(self):
        scraper = _make_scraper()
        payload = {"results": [_make_api_item(doc_id=5)]}
        scraper.request_service.make_request = AsyncMock(
            return_value=make_mock_json_response(payload)
        )

        result = await scraper._get_docs_links("https://api.example.com/norms")

        scraper.request_service.make_request.assert_awaited_once()
        assert len(result) == 1
        assert result[0]["id"] == 5

    @pytest.mark.asyncio
    async def test_returns_empty_on_failed_request(self):
        scraper = _make_scraper()
        scraper.request_service.make_request = AsyncMock(
            return_value=make_failed_request()
        )

        result = await scraper._get_docs_links("https://api.example.com/norms")
        assert result == []

    @pytest.mark.asyncio
    async def test_infers_valid_situation_when_no_data_vigencia(self):
        scraper = _make_scraper()
        item = _make_api_item(data_vigencia=None)
        data = {"results": [item]}

        result = await scraper._get_docs_links("https://api.example.com/", data=data)
        assert result[0]["situation"] == "Não consta revogação expressa"

    @pytest.mark.asyncio
    async def test_infers_invalid_situation_when_data_vigencia_set(self):
        scraper = _make_scraper()
        item = _make_api_item(data_vigencia="2024-01-01")
        data = {"results": [item]}

        result = await scraper._get_docs_links("https://api.example.com/", data=data)
        assert result[0]["situation"] == "Revogada"


# ===================================================================
# _get_doc_data()
# ===================================================================


class TestGetDocData:
    """Tests for _get_doc_data — document fetch, skip, and error paths."""

    def _make_doc_info(self, **overrides) -> dict:
        return {
            "id": 1,
            "norm_number": "1",
            "title": "Lei Ordinária 1/2023",
            "type": "Lei Ordinária",
            "situation": "Não consta revogação expressa",
            "summary": "Ementa",
            "subject": [],
            "date": "2023-01-01",
            "origin": None,
            "publication": None,
            "tipo_id": 2,
            "pdf_link": "https://sapl.example.leg.br/media/lei.pdf",
            **overrides,
        }

    @pytest.mark.asyncio
    async def test_returns_none_when_already_scraped(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=True)
        scraper._process_pdf = AsyncMock()

        result = await scraper._get_doc_data(self._make_doc_info(), year=2023)

        assert result is None
        scraper._process_pdf.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_none_and_saves_error_on_pdf_failure(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._process_pdf = AsyncMock(return_value=None)
        scraper._save_doc_error = AsyncMock()

        result = await scraper._get_doc_data(self._make_doc_info(), year=2023)

        assert result is None
        scraper._save_doc_error.assert_awaited_once()
        call_kwargs = scraper._save_doc_error.call_args.kwargs
        assert "PDF processing failed" in call_kwargs["error_message"]

    @pytest.mark.asyncio
    async def test_returns_scraped_document_on_success(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._process_pdf = AsyncMock(
            return_value={
                "text_markdown": "Art. 1º Texto da lei.",
                "document_url": "https://sapl.example.leg.br/media/lei.pdf",
                "raw_content": b"%PDF",
                "content_extension": ".pdf",
            }
        )

        result = await scraper._get_doc_data(self._make_doc_info(), year=2023)

        assert result is not None
        assert result.year == 2023
        assert result.title == "Lei Ordinária 1/2023"
        assert result.text_markdown == "Art. 1º Texto da lei."
        assert result.document_url == "https://sapl.example.leg.br/media/lei.pdf"


# ===================================================================
# _scrape_year()
# ===================================================================


class TestScrapeYear:
    """Tests for _scrape_year — paginated API stream + grouped processing."""

    @pytest.mark.asyncio
    async def test_returns_empty_on_failed_response(self):
        scraper = _make_scraper()
        scraper.request_service.make_request = AsyncMock(
            return_value=make_failed_request()
        )

        result = await scraper._scrape_year(2023)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_results(self):
        scraper = _make_scraper()
        scraper.request_service.make_request = AsyncMock(
            return_value=make_mock_json_response(
                {"results": [], "pagination": {"total_pages": 1}},
                status=200,
            )
        )

        result = await scraper._scrape_year(2023)
        assert result == []

    @pytest.mark.asyncio
    async def test_processes_single_page_of_results(self):
        scraper = _make_scraper()
        api_items = [
            _make_api_item(doc_id=1, tipo=2, ano=2023),
            _make_api_item(
                doc_id=2, tipo=1, ano=2023, title_str="Lei Complementar 2/2023"
            ),
        ]
        api_response = make_mock_json_response(
            {"results": api_items, "pagination": {"total_pages": 1}},
            status=200,
        )
        scraper.request_service.make_request = AsyncMock(return_value=api_response)

        fake_doc = MagicMock()
        fake_doc.model_dump.return_value = {"title": "test"}
        scraper._process_documents = AsyncMock(return_value=[fake_doc])

        with patch.object(
            type(scraper), "_gather_results", new_callable=AsyncMock
        ) as mock_gather:
            mock_gather.side_effect = [
                # First call: _gather_results for link_tasks
                [
                    [
                        {
                            "id": 1,
                            "norm_number": "1",
                            "title": "Lei Ordinária 1/2023",
                            "situation": "Não consta revogação expressa",
                            "summary": "Ementa",
                            "subject": [],
                            "date": "2023-01-01",
                            "origin": None,
                            "publication": None,
                            "pdf_link": "https://sapl.example.leg.br/media/1.pdf",
                        },
                    ]
                ],
                # Second call: _gather_results for type processing
                [[fake_doc]],
            ]

            result = await scraper._scrape_year(2023)

        assert len(result) >= 1
