"Parser and metadata regression tests for parana_scraper."

from math import ceil
from urllib.parse import parse_qs, urlsplit

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


class TestListType:
    async def test_no_results_page_yields_nothing(self):
        """The site reports zero hits only inside a script string, not in visible text."""
        scraper = _make_scraper()
        html = """<html><head><script>
            globalMsgError += '<td class="msg_sucesso">Nenhum registro encontrado.</td>';
        </script></head><body><form name="pesquisarAtoForm"></form></body></html>"""
        scraper.http.request.side_effect = [make_mock_response(html)]
        assert [ref async for ref in scraper.list_type(2025, "Lei", 1)] == []

    async def test_unrecognized_page_raises(self):
        scraper = _make_scraper()
        scraper.http.request.side_effect = [make_mock_response("<html></html>")]
        with pytest.raises(ValueError, match="pagination"):
            [ref async for ref in scraper.list_type(2025, "Lei", 1)]

    async def test_lists_results(self):
        scraper = _make_scraper()
        scraper.http.request.side_effect = [make_mock_response(_make_results_html(2))]
        refs = [ref async for ref in scraper.list_type(2020, "Lei", 1)]
        assert [ref["id"] for ref in refs] == ["101", "102"]
        assert refs[0]["document_url"].endswith("codAto=101")


_NO_RECORDS = "<html><script>m = 'Nenhum registro encontrado.';</script></html>"


def _serve_acts(acts: list[tuple[str | None, int | None]]):
    """Fake search form: 20 rows per page; like the live site, page 2+ repeats 4 rows
    of the previous page (date ties are ordered per query), so plain paging skips acts.
    A ``None`` code is counted in the total but never shown (Lei 22.530/2025 counts twice).
    """

    def serve(url: str, method: str, data: dict, timeout: float):
        low, high = data["nroInicialAtoTema"], data["nroFinalAtoTema"]
        hits = [
            act
            for act in acts
            if not low or (act[1] is not None and int(low) <= act[1] <= int(high))
        ]
        if not hits:
            return make_mock_response(_NO_RECORDS)
        page = int(parse_qs(urlsplit(url).query)["indice"][0])
        start = max(0, (page - 1) * 20 - 4)
        shown = [act for act in hits if act[0] is not None]
        rows = "".join(
            f'<tr class="list_cor_sim"><td><a href="javascript:exibir(\'{code}\');">x</a></td>'
            f"<td>{f'Lei {number} - 1 de janeiro de 2025' if number else 'Lei - sem número'}</td>"
            "<td>Súmula.</td><td>01/01/2025</td></tr>"
            for code, number in shown[start : start + 20]
        )
        return make_mock_response(
            f"<html><body><p>Página {page} de {ceil(len(hits) / 20)}</p>"
            f"<p>Total de {len(hits)} registros</p>"
            f'<table id="list_tabela"><tbody>{rows}</tbody></table></body></html>'
        )

    return serve


class TestListTypeSplitsByNumber:
    ACTS = [(str(1000 + n), n) for n in range(101, 151)]

    async def test_lists_every_act_once_despite_overlapping_pages(self):
        scraper = _make_scraper()
        scraper.http.request.side_effect = _serve_acts(self.ACTS)
        refs = [ref async for ref in scraper.list_type(2025, "Lei", 1)]
        assert sorted(ref["id"] for ref in refs) == sorted(
            code for code, _ in self.ACTS
        )

    async def test_acts_without_number_fail_visibly(self):
        scraper = _make_scraper()
        # Only on a middle page, which number-range queries replace.
        acts = [*self.ACTS[:30], ("9999", None), *self.ACTS[30:]]
        scraper.http.request.side_effect = _serve_acts(acts)
        refs = []
        with pytest.raises(ValueError, match="1 Paraná acts have no number"):
            async for ref in scraper.list_type(2025, "Lei", 1):
                refs.append(ref)
        assert len(refs) == 50

    async def test_counted_but_hidden_act_is_not_an_error(self):
        scraper = _make_scraper()
        scraper.http.request.side_effect = _serve_acts([*self.ACTS, (None, 125)])
        refs = [ref async for ref in scraper.list_type(2025, "Lei", 1)]
        assert len(refs) == 50
