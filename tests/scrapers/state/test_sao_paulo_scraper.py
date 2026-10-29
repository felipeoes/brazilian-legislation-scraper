"Parser and metadata regression tests for sao_paulo_scraper."

from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.sp import (
    INVALID_SITUATIONS,
    SITUATIONS,
    TYPES,
    VALID_SITUATIONS,
    SaoPauloAlespScraper,
)


def _make_scraper(**kwargs) -> SaoPauloAlespScraper:
    """Instantiate SaoPauloAlespScraper with injected dependencies."""
    return make_scraper(
        SaoPauloAlespScraper,
        _page_size=500,
        _site_base="https://www.al.sp.gov.br",
        headers={"User-Agent": "test"},
        **kwargs,
    )


def _make_results_html(total: int = 3, pages: int = 1) -> str:
    rows = ""
    for i in range(1, total + 1):
        rows += f'\n        <tr>\n          <td>\n            <span>Lei {i:04d}/2020</span>\n            <a href="/norma/{i}" class="link_norma">link</a>\n            <a href="/norma/{i}/texto">texto</a>\n          </td>\n          <td><span>Ementa da Lei {i}</span></td>\n        </tr>'
    return f"<html><body>\n        <b>Resultado: {total} normas em {pages}</b>\n        <table>{rows}</table>\n    </body></html>"


def _make_no_results_html() -> str:
    return "<html><body><div class='card cinza text-center'>Nenhuma norma encontrada como os parâmetros informados</div></body></html>"


def _make_valid_md() -> str:
    return "# Lei\n\nTexto da lei. " * 30


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 14
    REQUIRED_KEYS = {"Decreto", "Lei", "Constituição Estadual"}
    REQUIRE_INT_VALUES = True


class TestSituationsConstants(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_situations_is_union(self):
        assert len(SITUATIONS) == len(VALID_SITUATIONS) + len(INVALID_SITUATIONS)

    def test_valid_situations_contains_sem_revogacao(self):
        assert "Sem revogação expressa" in VALID_SITUATIONS

    def test_invalid_situations_contains_revogada(self):
        assert "Revogada" in INVALID_SITUATIONS

    def test_all_values_are_integers(self):
        for name, val in SITUATIONS.items():
            assert isinstance(val, int), f"{name!r} should have int ID"


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = SaoPauloAlespScraper
    STATE_NAME = "São Paulo"

    def test_situations_non_empty_in_instance(self):
        scraper = _make_scraper()
        assert len(scraper.situations) > 0


class TestBuildSearchUrl:
    def test_contains_base_url(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020)
        assert "al.sp.gov.br/norma/resultados" in url

    def test_contains_year(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2020)
        assert "ano=2020" in url

    def test_all_situations(self):
        """idTipoSituacao=0 means all situations."""
        scraper = _make_scraper()
        url = scraper._build_search_url(2020)
        assert "idTipoSituacao=0" in url

    def test_no_type_filter(self):
        """No idsTipoNorma= selection param — _idsTipoNorma=1 is only the checkbox sentinel."""
        scraper = _make_scraper()
        url = scraper._build_search_url(2020)
        assert "_idsTipoNorma=1" in url
        assert "&idsTipoNorma=" not in url

    def test_page_param(self):
        scraper = _make_scraper()
        url0 = scraper._build_search_url(2020, page=0)
        url2 = scraper._build_search_url(2020, page=2)
        assert "page=0" in url0
        assert "page=2" in url2

    def test_no_shared_state_mutation(self):
        scraper = _make_scraper()
        url1 = scraper._build_search_url(2020)
        url2 = scraper._build_search_url(2019)
        assert "ano=2020" in url1
        assert "ano=2019" in url2


class TestExtractResultCounts:
    def test_parses_standard_format(self):
        html = "<html><body><b>Resultado: 42 normas em 1</b></body></html>"
        soup = BeautifulSoup(html, "lxml")
        result = SaoPauloAlespScraper._extract_result_counts(soup)
        assert result == (42, 1)

    def test_parses_multipage(self):
        html = "<html><body><b>Resultado: 1500 normas em 3</b></body></html>"
        soup = BeautifulSoup(html, "lxml")
        result = SaoPauloAlespScraper._extract_result_counts(soup)
        assert result == (1500, 3)

    def test_returns_none_on_no_match(self):
        html = "<html><body><b>No results here</b></body></html>"
        soup = BeautifulSoup(html, "lxml")
        result = SaoPauloAlespScraper._extract_result_counts(soup)
        assert result is None

    def test_returns_none_on_empty_soup(self):
        soup = BeautifulSoup("<html><body></body></html>", "lxml")
        result = SaoPauloAlespScraper._extract_result_counts(soup)
        assert result is None


class TestParseDocs:
    def test_extracts_docs(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_results_html(3), "lxml")
        docs = scraper._parse_docs_from_soup(soup)
        assert len(docs) == 3

    def test_doc_has_required_keys(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_results_html(1), "lxml")
        docs = scraper._parse_docs_from_soup(soup)
        assert "title" in docs[0]
        assert "summary" in docs[0]
        assert "html_link" in docs[0]
        assert "norm_link" in docs[0]

    def test_html_link_is_absolute(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_results_html(1), "lxml")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs[0]["html_link"].startswith("http")

    def test_skips_mostrando_row(self):
        html = '<html><body>\n            <table>\n                <tr><td>Mostrando 1-10 de 50</td><td></td></tr>\n                <tr>\n                    <td><span>Lei 001/2020</span>\n                        <a href="/norma/1" class="link_norma">link</a>\n                        <a href="/norma/1/texto">texto</a>\n                    </td>\n                    <td><span>Ementa</span></td>\n                </tr>\n            </table>\n        </body></html>'
        scraper = _make_scraper()
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._parse_docs_from_soup(soup)
        assert len(docs) == 1

    def test_empty_table_returns_empty(self):
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body><table></table></body></html>", "lxml")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs == []


class TestInferType:
    def test_lei_complementar_matched_before_lei(self):
        """Longest match wins: 'Lei Complementar' before 'Lei'."""
        result = SaoPauloAlespScraper._infer_type("Lei Complementar 42/2020")
        assert result == "Lei Complementar"

    def test_lei_matched(self):
        result = SaoPauloAlespScraper._infer_type("Lei 9.394/1996")
        assert result == "Lei"

    def test_decreto_matched(self):
        result = SaoPauloAlespScraper._infer_type("Decreto 64.921/2020")
        assert result == "Decreto"

    def test_decisao_da_mesa_matched(self):
        result = SaoPauloAlespScraper._infer_type("Decisão da Mesa 1311/2005")
        assert result == "Decisão da Mesa"

    def test_unknown_falls_back_to_title_prefix(self):
        result = SaoPauloAlespScraper._infer_type("Norma Desconhecida 1/2020")
        assert result == "Norma Desconhecida"

    def test_case_insensitive(self):
        result = SaoPauloAlespScraper._infer_type("LEI 1/2020")
        assert result == "Lei"
