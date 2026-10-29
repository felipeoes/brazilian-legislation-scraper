"Parser and metadata regression tests for goias_scraper."

from unittest.mock import AsyncMock, MagicMock

from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.go import (
    _TYPE_ID_TO_SUFFIX,
    SITUATIONS,
    TYPES,
    LegislaGoias,
    remove_summary_from_markdown,
)


def _make_scraper(**kwargs) -> LegislaGoias:
    """Instantiate LegislaGoias with injected dependencies."""
    return make_scraper(LegislaGoias, **kwargs)


def _make_valid_md() -> str:
    return (
        "# Lei Estadual\n\nO governador do estado de Goiás decreta a presente lei. "
        * 30
    )


def _make_doc_detail(
    doc_id: int = 1,
    numero: str = "001",
    ano: int = 2020,
    tipo_nome: str = "Lei Ordinária",
    tipo_id: int = 2,
    conteudo: str = "",
    ementa: str = "Ementa da lei",
) -> dict:
    return {
        "id": doc_id,
        "numero": numero,
        "ano": ano,
        "data_legislacao": f"{ano}-01-01",
        "tipo_legislacao": {"nome": tipo_nome, "id": tipo_id},
        "ementa": ementa,
        "estado_legislacao": {"nome": "Vigente"},
        "conteudo": conteudo,
    }


def _make_search_result(
    doc_id: int = 1,
    numero: str = "001",
    ano: int = 2020,
    tipo_nome: str = "Lei Ordinária",
    tipo_id: int = 2,
) -> dict:
    """Make a search result item (as returned by the list API)."""
    return {
        "id": doc_id,
        "numero": numero,
        "ano": ano,
        "tipo_legislacao": {"nome": tipo_nome, "id": tipo_id},
        "estado_legislacao": {"nome": "Vigente"},
    }


def _make_response(data: dict) -> MagicMock:
    resp = MagicMock()
    resp.__bool__ = MagicMock(return_value=True)
    resp.json = AsyncMock(return_value=data)
    resp.read = AsyncMock(return_value=b"%PDF fake content")
    return resp


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 10
    REQUIRED_KEYS = {"Lei Ordinária", "Lei Complementar", "Constituição Estadual"}
    REQUIRE_INT_VALUES = False

    def test_each_type_has_id_and_url_suffix(self):
        for name, data in TYPES.items():
            assert "id" in data, f"{name} missing 'id'"
            assert "url_suffix" in data, f"{name} missing 'url_suffix'"

    def test_ids_are_integers(self):
        for name, data in TYPES.items():
            assert isinstance(data["id"], int), f"{name} id is not int"


class TestTypeIdToSuffix:
    def test_maps_all_type_ids(self):
        for name, data in TYPES.items():
            assert data["id"] in _TYPE_ID_TO_SUFFIX, f"{name} id not in mapping"

    def test_correct_suffix_for_lei_ordinaria(self):
        assert _TYPE_ID_TO_SUFFIX[2] == "lei"

    def test_correct_suffix_for_constituicao(self):
        assert _TYPE_ID_TO_SUFFIX[12] == "constituicao-estadual"


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = True


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = LegislaGoias
    STATE_NAME = "Goias"

    def test_situations_empty_by_default(self):
        scraper = _make_scraper()
        assert scraper.situations == {}


class TestBuildSearchUrl:
    def test_contains_ano_param(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020, 1)
        assert "ano=2020" in url

    def test_contains_page_param(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020, 3)
        assert "page=3" in url

    def test_no_tipo_legislacao_by_default(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020, 1)
        assert "tipo_legislacao" not in url

    def test_includes_tipo_legislacao_when_provided(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020, 1, norm_type_id=2)
        assert "tipo_legislacao=2" in url

    def test_no_shared_state_mutation(self):
        scraper = _make_scraper()
        url1 = scraper._build_search_url(2020, 1, norm_type_id=2)
        url2 = scraper._build_search_url(2019, 2, norm_type_id=7)
        assert "tipo_legislacao=2" in url1
        assert "tipo_legislacao=7" in url2
        assert "ano=2020" in url1
        assert "ano=2019" in url2

    def test_base_url_included(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020, 1)
        assert "legisla.casacivil.go.gov.br" in url

    def test_page_starts_at_1_by_default(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020)
        assert "page=1" in url


class TestCleanMarkdown:
    def test_removes_javascript_print(self):
        scraper = _make_scraper()
        result = scraper._clean_markdown("text javascript:print() more")
        assert "javascript:print()" not in result

    def test_strips_whitespace(self):
        scraper = _make_scraper()
        result = scraper._clean_markdown("  hello  ")
        assert result == "hello"

    def test_passthrough_clean_text(self):
        scraper = _make_scraper()
        text = "# Lei\n\nConteúdo da lei."
        assert scraper._clean_markdown(text) == text


class TestRemoveSummaryFromMarkdown:
    def test_removes_matching_standalone_paragraph(self):
        summary = "Altera o Decreto nº 10.218 de 2023."
        md = f"# DECRETO\n\n{summary}\n\nO GOVERNADOR decreta:"
        result = remove_summary_from_markdown(md, summary)
        assert summary not in result
        assert "# DECRETO" in result
        assert "O GOVERNADOR decreta:" in result

    def test_noop_when_summary_embedded_in_article(self):
        summary = "Fica aberto crédito suplementar."
        md = "# Portaria\n\nO SECRETÁRIO resolve:\n\nArt. 1º Fica aberto crédito suplementar.\n\nArt. 2º Esta portaria entra em vigor nesta data."
        result = remove_summary_from_markdown(md, summary)
        assert result == md

    def test_noop_when_summary_empty(self):
        md = "# Lei\n\nConteúdo da lei."
        assert remove_summary_from_markdown(md, "") == md

    def test_noop_when_summary_not_found(self):
        md = "# Lei\n\nConteúdo completamente diferente."
        result = remove_summary_from_markdown(md, "Ementa que não existe no texto.")
        assert result == md

    def test_normalizes_extra_whitespace_in_summary(self):
        summary = "Dispõe sobre  a reorganização   administrativa."
        md = "# Lei\n\nDispõe sobre a reorganização administrativa.\n\nArt. 1º ..."
        result = remove_summary_from_markdown(md, summary)
        assert "Dispõe sobre a reorganização administrativa." not in result

    def test_strips_markdown_formatting_chars_from_paragraph(self):
        summary = "Dispõe sobre a reorganização administrativa."
        md = "# Lei\n\n**Dispõe sobre a reorganização administrativa.**\n\nArt. 1º ..."
        result = remove_summary_from_markdown(md, summary)
        assert "Dispõe sobre a reorganização administrativa." not in result

    def test_removes_only_first_occurrence_if_duplicate_paragraphs(self):
        summary = "Ementa da lei."
        md = f"# Lei\n\n{summary}\n\nConteúdo\n\n{summary}"
        result = remove_summary_from_markdown(md, summary)
        assert result.count(summary) == 0
