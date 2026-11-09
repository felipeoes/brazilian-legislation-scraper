"Parser and metadata regression tests for parana_scraper."

import pytest
from bs4 import BeautifulSoup
from conftest import make_mock_response, make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.core.scraper import DEFAULT_INVALID_SITUATION, DEFAULT_VALID_SITUATION
from src.scrapers.states.pr import SITUATIONS, TYPES, ParanaCVScraper


def _make_scraper(**kwargs) -> ParanaCVScraper:
    """Instantiate ParanaCVScraper with injected dependencies."""
    return make_scraper(ParanaCVScraper, **kwargs)


def _make_results_html(codes: list[int], total: int | None = None) -> str:
    """The unpaginated year listing (``listarAtosAno.do?action=listarAtos``)."""
    rows = "".join(
        f"""<tr class="list_cor_sim">
          <td><a href="JavaScript:exibir('/legislacao/listarAtosAno.do?action=exibir&amp;codAto={code}&amp;indice=1&amp;totalRegistros={len(codes)}');"><img src="/legislacao/images/icon_exibir.png"/></a></td>
          <td><div id="{code}_epigrafeAto">Lei {code} - 01 de janeiro de 2020</div></td>
          <td><div id="{code}_descricaoItemAto">Dispõe sobre assunto {code}.</div></td>
          <td><div id="{code}_dataPublicacao">01/01/2020</div></td>
        </tr>"""
        for code in codes
    )
    total = len(codes) if total is None else total
    return f"""<html><body><div><i>{len(codes)} registro(s) listado(s)</i></div>
        <table id="barra_paginacao"><tr><td>P&aacute;gina 1 de 1 : (Total de {total} registros)</td></tr></table>
        <table id="list_tabela">{rows}</table></body></html>"""


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


class TestParseResultsTable:
    def test_happy_path_returns_docs(self):
        soup = BeautifulSoup(_make_results_html([101, 102]), "lxml")
        docs = ParanaCVScraper._parse_results_table(soup)
        assert docs[0] == {
            "id": "101",
            "title": "Lei 101 - 01 de janeiro de 2020",
            "summary": "Dispõe sobre assunto 101.",
            "date": "01/01/2020",
        }
        assert len(docs) == 2

    def test_missing_table_returns_empty(self):
        soup = BeautifulSoup("<html><body><p>Nothing</p></body></html>", "lxml")
        docs = ParanaCVScraper._parse_results_table(soup)
        assert docs == []

    def test_skips_rows_without_link(self):
        html = '<html><body>\n            <table id="list_tabela"><tbody>\n              <tr class="list_cor_sim">\n                <td>No link here</td>\n                <td>Title</td><td>Summary</td><td>01/01/2020</td>\n              </tr>\n            </tbody></table>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        docs = ParanaCVScraper._parse_results_table(soup)
        assert docs == []

    def test_skips_rows_with_fewer_than_4_tds(self):
        html = '<html><body>\n            <table id="list_tabela"><tbody>\n              <tr class="list_cor_sim">\n                <td><a href="JavaScript:exibir(\'/legislacao/listarAtosAno.do?action=exibir&amp;codAto=99\');">X</a></td>\n                <td>Title</td>\n              </tr>\n            </tbody></table>\n        </body></html>'
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


class TestFetch:
    REF = {
        "id": "101",
        "title": "Lei 1 - 01 de janeiro de 2020",
        "year": 2020,
        "type": "Lei",
        "situation": DEFAULT_VALID_SITUATION,
        "document_url": "https://www.legislacao.pr.gov.br/legislacao/pesquisarAto.do?action=exibir&codAto=101",
    }

    async def test_fetches_over_http_and_stores_html_body(self):
        """Chromium hangs on a never-finishing sub-resource; the HTML itself is complete."""
        scraper = _make_scraper()
        body = _make_doc_html()
        scraper.http.request.side_effect = [make_mock_response(body)]
        doc = await scraper.fetch(dict(self.REF))
        assert doc.raw == body.encode()
        assert doc.raw_ext == ".html"
        assert "Dispõe sobre assunto importante" in doc.text_markdown
        assert doc.situation == DEFAULT_VALID_SITUATION
        scraper.browser.snapshot.assert_not_called()

    async def test_revoked_situation(self):
        scraper = _make_scraper()
        html = _make_doc_html().replace(
            "<div>", "<div><p>Situação: Revogado pelo Decreto 999/2010</p>", 1
        )
        scraper.http.request.side_effect = [make_mock_response(html)]
        doc = await scraper.fetch(dict(self.REF))
        assert doc.situation == DEFAULT_INVALID_SITUATION

    async def test_missing_form_raises(self):
        scraper = _make_scraper()
        scraper.http.request.side_effect = [make_mock_response("<html></html>")]
        with pytest.raises(ValueError, match="pesquisarAtoForm"):
            await scraper.fetch(dict(self.REF))


# Trimmed from live responses: an empty year, and the site's error page, which also
# shows "Nenhum registro encontrado".
_NO_RECORDS = """<script>var auxiliar = document.getElementById(globalAjaxDivName);</script>
<br><div width="100%" align="center"><i>Nenhum registro encontrado.</i></div>"""
_SITE_ERROR = """<script>globalAjaxMsgError += '<td class="msg_aviso">gov.pr.celepar.framework.exception.ApplicationException: Sistema temporariamente indisponível. Aguarde alguns instantes e tente novamente.</td>';</script>
<br><div width="100%" align="center"><i>Nenhum registro encontrado.</i></div>"""


class TestListType:
    async def test_lists_every_act_of_the_year_in_one_request(self):
        scraper = _make_scraper()
        scraper.http.request.side_effect = [
            make_mock_response(_make_results_html([101, 102]))
        ]
        refs = [ref async for ref in scraper.list_type(2020, "Resolução", 13)]
        assert [ref["id"] for ref in refs] == ["101", "102"]
        assert refs[0]["document_url"] == (
            "https://www.legislacao.pr.gov.br/legislacao/pesquisarAto.do?action=exibir&codAto=101"
        )
        assert (refs[0]["year"], refs[0]["type"]) == (2020, "Resolução")
        call = scraper.http.request.await_args
        assert "action=listarAtos&anoAto=2020&isPaginado=false" in call.args[0]
        # Every agency at once: resoluções and portarias come from ~200 of them.
        assert call.kwargs["data"]["codTipoAto"] == "13"
        assert call.kwargs["data"]["codOrgaoUnidade"] == ""
        # The site needs ~2 minutes to list a year of resoluções.
        assert call.kwargs["timeout"] >= 300

    async def test_repeated_rows_are_listed_once(self):
        """The site repeats a few rows and counts them (2025 Decretos: 3,860 rows, 3,858 acts)."""
        scraper = _make_scraper()
        scraper.http.request.side_effect = [
            make_mock_response(_make_results_html([101, 102, 101]))
        ]
        refs = [ref async for ref in scraper.list_type(2025, "Decreto", 11)]
        assert [ref["id"] for ref in refs] == ["101", "102"]

    async def test_fewer_rows_than_the_total_fail_visibly(self):
        scraper = _make_scraper()
        scraper.http.request.side_effect = [
            make_mock_response(_make_results_html([101, 102], total=3))
        ]
        with pytest.raises(ValueError, match="listed 2 of 3"):
            [ref async for ref in scraper.list_type(2025, "Lei", 1)]

    async def test_no_results_page_yields_nothing(self):
        scraper = _make_scraper()
        scraper.http.request.side_effect = [make_mock_response(_NO_RECORDS)]
        refs = [
            ref async for ref in scraper.list_type(2025, "Constituição Estadual", 10)
        ]
        assert refs == []

    async def test_site_error_is_not_an_empty_year(self):
        scraper = _make_scraper()
        scraper.http.request.side_effect = [make_mock_response(_SITE_ERROR)]
        with pytest.raises(ValueError, match="temporariamente indisponível"):
            [ref async for ref in scraper.list_type(2025, "Lei", 1)]

    async def test_unrecognized_page_raises(self):
        scraper = _make_scraper()
        scraper.http.request.side_effect = [make_mock_response("<html></html>")]
        with pytest.raises(ValueError, match="Unrecognized Paraná listing"):
            [ref async for ref in scraper.list_type(2025, "Lei", 1)]
