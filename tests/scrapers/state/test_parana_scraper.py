"Parser and metadata regression tests for parana_scraper."

from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.core.scraper import DEFAULT_INVALID_SITUATION, DEFAULT_VALID_SITUATION
from src.scrapers.states.pr import SITUATIONS, TYPES, ParanaCVScraper


def _make_scraper(**kwargs) -> ParanaCVScraper:
    """Instantiate ParanaCVScraper with injected dependencies."""
    return make_scraper(
        ParanaCVScraper,
        _base_form_data={
            "pesquisou": "true",
            "opcaoAno": "2",
            "opcaoNro": "1",
            "optPesquisa": "tm",
            "tiposAtoStr": "",
            "site": "1",
            "codigoTipoAto": "",
            "tipoOrdenacao": "",
            "ordAsc": "false",
            "optTexto": "2",
            "texto": "",
            "anoInicialAto": "",
            "anoFinalAto": "",
            "nroInicialAto": "",
            "nroFinalAto": "",
            "tipoAto": "",
            "nroAto": "",
            "anoAto": "",
            "tema": "0",
            "anoInicialAtoTema": "",
            "anoFinalAtoTema": "",
            "nroInicialAtoTema": "",
            "nroFinalAtoTema": "",
        },
        **kwargs,
    )


def _make_results_html(count: int = 2, revogado: bool = False) -> str:
    revogado_text = " Revogado pelo Decreto 999/2010" if revogado else ""
    rows = ""
    for i in range(1, count + 1):
        rows += f"""\n        <tr class="list_cor_sim">\n          <td><a href="javascript:exibirAto('{100 + i}')">Lei {i}</a></td>\n          <td>Lei Estadual {i}/2020{revogado_text}</td>\n          <td>Dispõe sobre assunto {i}.</td>\n          <td>01/01/2020</td>\n        </tr>"""
    return f'<html><body>\n        <p>Página 1 de 1</p><p>Total de {count} registros</p>\n        <table id="list_tabela"><tbody>{rows}</tbody></table>\n    </body></html>'


def _make_doc_html(revogado: bool = False) -> str:
    revogado_text = "Revogado pelo Decreto 999/2010" if revogado else ""
    return f'<html><body>\n        <form name="pesquisarAtoForm">\n          <div>\n            <p>LEI ESTADUAL N.º 1/2020</p>\n            <p>Dispõe sobre assunto importante.</p>\n            {revogado_text}\n          </div>\n        </form>\n    </body></html>'


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 7
    REQUIRED_KEYS = {"Lei", "Decreto"}
    REQUIRE_INT_VALUES = True


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = True


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = ParanaCVScraper
    STATE_NAME = "Paraná"


class TestBuildFormData:
    def test_sets_tipos_ato_str(self):
        scraper = _make_scraper()
        data = scraper._build_form_data(2020, 1)
        assert data["tiposAtoStr"] == "1"

    def test_sets_ano_inicial_ato_tema(self):
        scraper = _make_scraper()
        data = scraper._build_form_data(2020, 1)
        assert data["anoInicialAtoTema"] == "2020"

    def test_sets_ano_final_ato_tema(self):
        scraper = _make_scraper()
        data = scraper._build_form_data(2020, 1)
        assert data["anoFinalAtoTema"] == "2020"

    def test_no_mutation_of_base_form_data(self):
        scraper = _make_scraper()
        original = scraper._base_form_data.copy()
        scraper._build_form_data(2020, 1)
        assert scraper._base_form_data == original

    def test_different_years_produce_different_data(self):
        scraper = _make_scraper()
        d1 = scraper._build_form_data(2020, 1)
        d2 = scraper._build_form_data(2021, 1)
        assert d1["anoInicialAtoTema"] != d2["anoInicialAtoTema"]


class TestSearchUrl:
    def test_without_total_records(self):
        scraper = _make_scraper()
        url = scraper._search_url(page=1)
        assert "indice=1" in url
        assert "site=1" in url
        assert "totalRegistros" not in url

    def test_with_total_records(self):
        scraper = _make_scraper()
        url = scraper._search_url(page=2, total_records=50)
        assert "indice=2" in url
        assert "totalRegistros=50" in url

    def test_contains_base_url(self):
        scraper = _make_scraper()
        url = scraper._search_url()
        assert scraper.base_url in url


class TestParseResultsTable:
    def test_happy_path_returns_docs(self):
        soup = BeautifulSoup(_make_results_html(2), "lxml")
        docs = ParanaCVScraper._parse_results_table(soup)
        assert len(docs) == 2
        assert "id" in docs[0]
        assert "title" in docs[0]
        assert "summary" in docs[0]
        assert "date" in docs[0]

    def test_missing_table_returns_empty(self):
        soup = BeautifulSoup("<html><body><p>Nothing</p></body></html>", "lxml")
        docs = ParanaCVScraper._parse_results_table(soup)
        assert docs == []

    def test_extracts_cod_ato_from_href(self):
        soup = BeautifulSoup(_make_results_html(1), "lxml")
        docs = ParanaCVScraper._parse_results_table(soup)
        assert docs[0]["id"] == "101"

    def test_skips_rows_without_link(self):
        html = '<html><body>\n            <table id="list_tabela"><tbody>\n              <tr class="list_cor_sim">\n                <td>No link here</td>\n                <td>Title</td><td>Summary</td><td>01/01/2020</td>\n              </tr>\n            </tbody></table>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        docs = ParanaCVScraper._parse_results_table(soup)
        assert docs == []

    def test_skips_rows_with_fewer_than_4_tds(self):
        html = '<html><body>\n            <table id="list_tabela"><tbody>\n              <tr class="list_cor_sim">\n                <td><a href="javascript:exibirAto(\'99\')">X</a></td>\n                <td>Title</td>\n              </tr>\n            </tbody></table>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        docs = ParanaCVScraper._parse_results_table(soup)
        assert docs == []


class TestInferSituation:
    def test_revogado_pelo_returns_invalid(self):
        html = (
            "<html><body><p>Situação: Revogado pelo Decreto 999/2010.</p></body></html>"
        )
        soup = BeautifulSoup(html, "lxml")
        assert ParanaCVScraper._infer_situation(soup) == DEFAULT_INVALID_SITUATION

    def test_revogada_pela_returns_invalid(self):
        html = "<html><body><p>Situação: Revogada pela Lei 888/2008.</p></body></html>"
        soup = BeautifulSoup(html, "lxml")
        assert ParanaCVScraper._infer_situation(soup) == DEFAULT_INVALID_SITUATION

    def test_no_revogado_returns_valid(self):
        html = "<html><body><p>Dispõe sobre normas administrativas.</p></body></html>"
        soup = BeautifulSoup(html, "lxml")
        assert ParanaCVScraper._infer_situation(soup) == DEFAULT_VALID_SITUATION

    def test_revogado_por_returns_invalid(self):
        html = (
            "<html><body><p>Situação: Revogado por norma posterior.</p></body></html>"
        )
        soup = BeautifulSoup(html, "lxml")
        assert ParanaCVScraper._infer_situation(soup) == DEFAULT_INVALID_SITUATION
