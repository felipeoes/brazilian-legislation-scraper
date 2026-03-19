"""Tests for ParaibaAlpbScraper.

Covers:
- TYPES constant: 11 types present, integer IDs
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set (SAPLBaseScraper passes situations={})
- _id_to_type reverse map built correctly
- _get_docs_links includes tipo_id field

Run with:
    .venv/bin/pytest tests/test_paraiba_scraper.py -v
"""

from unittest.mock import AsyncMock

import pytest
from base_tests import ScraperClassTests, TypesConstantTests
from conftest import make_base_scraper

from src.scraper.state_legislation.paraiba import TYPES, ParaibaAlpbScraper

# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> ParaibaAlpbScraper:
    """Instantiate ParaibaAlpbScraper bypassing __init__ (no network, no I/O)."""
    kwargs.setdefault("subjects", {})
    kwargs.setdefault("_id_to_type", {v: k for k, v in TYPES.items()})
    kwargs.setdefault("_page_size", 100)
    return make_base_scraper(
        ParaibaAlpbScraper,
        "https://sapl3.al.pb.leg.br",
        "PARAIBA",
        TYPES,
        **kwargs,
    )


def _make_api_item(
    doc_id: int = 1,
    texto_integral: str | None = "https://sapl3.al.pb.leg.br/media/lei.pdf",
    tipo: int = 2,
    **kwargs,
) -> dict:
    return {
        "id": doc_id,
        "numero": str(doc_id),
        "__str__": f"Lei Ordinária {doc_id}/2023",
        "link_detail_backend": f"/norma/{doc_id}",
        "ementa": "Ementa da lei.",
        "assuntos": [],
        "data": "2023-01-01",
        "ano": 2023,
        "esfera_federacao": None,
        "veiculo_publicacao": None,
        "texto_integral": texto_integral,
        "data_vigencia": None,
        "tipo": tipo,
        **kwargs,
    }


# ---------------------------------------------------------------------------
# TYPES constant
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 11
    REQUIRED_KEYS = {"Lei Ordinária", "Lei Complementar", "Constituição Estadual"}


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = ParaibaAlpbScraper
    STATE_NAME = "Paraíba"

    def test_situations_empty(self):
        scraper = _make_scraper()
        assert scraper.situations == {}

    def test_id_to_type_built_correctly(self):
        scraper = _make_scraper()
        assert scraper._id_to_type[2] == "Lei Ordinária"
        assert scraper._id_to_type[1] == "Lei Complementar"
        assert len(scraper._id_to_type) == len(TYPES)


# ---------------------------------------------------------------------------
# _get_docs_links — tipo_id field
# ---------------------------------------------------------------------------


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_tipo_id_present_in_result(self):
        scraper = _make_scraper()
        item = _make_api_item(doc_id=1, tipo=2)
        data = {"results": [item]}
        result = await scraper._get_docs_links("https://example.com/api/", data=data)
        assert len(result) == 1
        assert result[0]["tipo_id"] == 2

    @pytest.mark.asyncio
    async def test_tipo_id_matches_api_tipo(self):
        scraper = _make_scraper()
        items = [
            _make_api_item(doc_id=1, tipo=2),
            _make_api_item(doc_id=2, tipo=9),
        ]
        data = {"results": items}
        result = await scraper._get_docs_links("https://example.com/api/", data=data)
        assert result[0]["tipo_id"] == 2
        assert result[1]["tipo_id"] == 9

    @pytest.mark.asyncio
    async def test_item_without_texto_integral_skipped(self):
        scraper = _make_scraper()
        scraper._save_doc_error = AsyncMock()
        item = _make_api_item(texto_integral=None)
        data = {"results": [item]}
        result = await scraper._get_docs_links("https://example.com/api/", data=data)
        assert result == []
        scraper._save_doc_error.assert_awaited_once_with(
            title="Lei Ordinária 1/2023",
            year=2023,
            situation="Não consta revogação expressa",
            norm_type="Lei Ordinária",
            html_link="https://sapl3.al.pb.leg.br/norma/1",
            error_message="SAPL API returned no texto_integral attachment",
            norma_id=1,
            materia_id=None,
            api_detail_url="https://sapl3.al.pb.leg.br/api/norma/normajuridica/1",
        )


class TestProcessPdf:
    def test_build_pdf_fetch_urls_prefers_https_for_same_host(self):
        scraper = _make_scraper()

        urls = scraper._build_pdf_fetch_urls(
            "http://sapl3.al.pb.leg.br/media/sapl/public/normajuridica/2025/1/lei.pdf"
        )

        assert urls == [
            "https://sapl3.al.pb.leg.br/media/sapl/public/normajuridica/2025/1/lei.pdf",
            "http://sapl3.al.pb.leg.br/media/sapl/public/normajuridica/2025/1/lei.pdf",
        ]

    @pytest.mark.asyncio
    async def test_process_pdf_retries_with_original_url_after_https_candidate(self):
        scraper = _make_scraper()
        original_url = (
            "http://sapl3.al.pb.leg.br/media/sapl/public/normajuridica/2025/1/lei.pdf"
        )
        scraper._download_and_convert = AsyncMock(
            side_effect=[
                ("", b"", ""),
                ("Lei Ordinária nº 1\n\n" + "Art. 1º Texto. " * 10, b"%PDF", ".pdf"),
            ]
        )

        result = await scraper._process_pdf(
            original_url,
            2025,
            title="Lei Ordinária nº 1",
        )

        assert result is not None
        assert result["document_url"] == original_url
        assert scraper._download_and_convert.await_args_list[0].args == (
            "https://sapl3.al.pb.leg.br/media/sapl/public/normajuridica/2025/1/lei.pdf",
        )
        assert scraper._download_and_convert.await_args_list[1].args == (original_url,)
