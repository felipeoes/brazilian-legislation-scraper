"Parser and metadata regression tests for sergipe_scraper."

from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.se import (
    INVALID_SITUATIONS,
    SITUATIONS,
    TYPES,
    VALID_SITUATIONS,
    SergipeLegsonScraper,
)


def _make_scraper(**kwargs) -> SergipeLegsonScraper:
    """Instantiate SergipeLegsonScraper with injected dependencies."""
    return make_scraper(
        SergipeLegsonScraper,
        search_url="https://legison.pge.se.gov.br/Public/Consulta",
        doc_content_url="https://legison.pge.se.gov.br/Public/GetConteudoAto",
        **kwargs,
    )


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 4
    REQUIRED_KEYS = {
        "Lei Ordinária",
        "Lei Complementar",
        "Decreto",
        "Emenda Constitucional",
    }
    REQUIRE_INT_VALUES = True


class TestSituationsConstants(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_em_vigor_in_valid(self):
        assert "Em Vigor" in VALID_SITUATIONS.values()

    def test_revogado_in_invalid(self):
        assert "Revogado" in INVALID_SITUATIONS.values()

    def test_situations_merges_both(self):
        for k, v in VALID_SITUATIONS.items():
            assert SITUATIONS[k] == v
        for k, v in INVALID_SITUATIONS.items():
            assert SITUATIONS[k] == v


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = SergipeLegsonScraper
    STATE_NAME = "Sergipe"


class TestFormatSearchPayload:
    def test_includes_type_id(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload(3, 2023)
        assert payload["IdTipo"] == "3"

    def test_includes_year(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload(3, 2023)
        assert payload["ano"] == "2023"

    def test_default_page_is_1(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload(3, 2023)
        assert payload["Page"] == 1

    def test_custom_page(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload(3, 2023, page=5)
        assert payload["Page"] == 5

    def test_required_keys_present(self):
        scraper = _make_scraper()
        payload = scraper._format_search_payload(3, 2023)
        for key in ("Ementa", "IdTipo", "ano", "Page", "Order"):
            assert key in payload


class TestCleanLegisonMarkdown:
    def test_removes_portal_footer(self):
        scraper = _make_scraper()
        text = "Texto da lei.\n\nExtraído do Portal de Legislação do Governo de Sergipe - LegisOn https://legislacao.se.gov.br/\n"
        result = scraper._clean_legison_markdown(text)
        assert "LegisOn" not in result
        assert "Texto da lei." in result

    def test_removes_nao_substitui_disclaimer(self):
        scraper = _make_scraper()
        text = "Texto.\n\nEste texto não substitui o publicado no Diário Oficial do Estado."
        result = scraper._clean_legison_markdown(text)
        assert "não substitui" not in result

    def test_collapses_multiple_newlines(self):
        scraper = _make_scraper()
        text = "Linha 1.\n\n\n\n\nLinha 2."
        result = scraper._clean_legison_markdown(text)
        assert "\n\n\n" not in result

    def test_strips_result(self):
        scraper = _make_scraper()
        text = "  \n\nTexto.\n\n  "
        result = scraper._clean_legison_markdown(text)
        assert result == result.strip()


class TestExtractContentMarkdown:
    def test_no_content_key_returns_empty(self):
        scraper = _make_scraper()
        result = scraper._extract_content_markdown({})
        assert result == ""

    def test_non_list_content_returns_empty(self):
        scraper = _make_scraper()
        result = scraper._extract_content_markdown({"content": "not a list"})
        assert result == ""

    def test_extracts_text_from_conteudo(self):
        scraper = _make_scraper()
        data = {"content": [{"conteudo": "Texto da lei."}, {"conteudo": "Mais texto."}]}
        result = scraper._extract_content_markdown(data)
        assert "Texto da lei." in result
        assert "Mais texto." in result

    def test_skips_items_without_conteudo(self):
        scraper = _make_scraper()
        data = {"content": [{"other": "value"}, {"conteudo": "Texto válido."}]}
        result = scraper._extract_content_markdown(data)
        assert "Texto válido." in result

    def test_empty_content_list_returns_empty(self):
        scraper = _make_scraper()
        result = scraper._extract_content_markdown({"content": []})
        assert result == ""


def _make_api_item(
    doc_id: int = 1,
    numero: str = "42",
    tipo_descricao: str = "Lei Ordinária",
    ementa: str = "Dispõe sobre algo.",
    data_ato: str = "2023-06-01T00:00:00",
    situacao_id: int = 1,
) -> dict:
    return {
        "id": doc_id,
        "numero": numero,
        "dataAto": data_ato,
        "ementa": ementa,
        "tipoAto": {"descricao": tipo_descricao},
        "situacao": {"id": situacao_id},
    }
