"Parser and metadata regression tests for sapl_scraper."

import pytest
from conftest import make_scraper

from src.core.sapl import SAPLBaseScraper

TYPES: dict[str, int] = {"Lei Ordinária": 2, "Lei Complementar": 1, "Decreto": 3}


def _make_scraper(**kwargs) -> SAPLBaseScraper:
    """Create a SAPLBaseScraper instance bypassing __init__."""
    kwargs.setdefault("subjects", {})
    kwargs.setdefault("_id_to_type", {v: k for k, v in TYPES.items()})
    kwargs.setdefault("_page_size", 100)
    return make_scraper(SAPLBaseScraper, **kwargs)


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
        lines = ["Cabeçalho qualquer", "LEI ORDINÁRIA Nº 123/2023", "Art. 1º Algo"]
        idx = self._find(lines, expected_title="Lei Ordinária nº 123/2023")
        assert idx == 1

    def test_fuzzy_title_score_match(self):
        """When exact match fails, token scoring picks the best candidate."""
        lines = ["Página do estado", "LEI COMPLEMENTAR 456 de 2023", "Art. 1º Texto."]
        idx = self._find(lines, expected_title="Lei Complementar nº 456/2023")
        assert idx == 1

    def test_fallback_to_numbered_sapl_title(self):
        """Without an expected title, picks the last numbered SAPL title line."""
        lines = ["GOVERNO DO ESTADO", "DECRETO 99/2022", "Art. 1º Algo qualquer."]
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
        lines = ["Preâmbulo", "Decreto nº 5 - Regulamenta a Lei X", "Art. 1º Texto."]
        idx = self._find(lines, expected_title="Decreto nº 5")
        assert idx == 1


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
        assert result[0]["type"] == scraper._resolve_type_name(1)
        assert result[0]["pdf_link"] == item["texto_integral"]
        assert result[0]["title"] == "Lei Ordinária 1/2023"

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
