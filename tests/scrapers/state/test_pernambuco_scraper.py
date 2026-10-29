"""Tests for PernambucoAlepeScraper."""

import ast
import inspect

from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import ScraperClassTests, TypesConstantTests

from src.scrapers.states.pe import TYPES, PernambucoAlepeScraper


def _make_scraper(**kwargs) -> PernambucoAlepeScraper:
    """Instantiate PernambucoAlepeScraper with injected dependencies."""
    _search_url = "https://legis.alepe.pe.gov.br/Paginas/pesquisaAvancada.aspx"
    return make_scraper(
        PernambucoAlepeScraper,
        search_url=_search_url,
        _known_types=tuple(sorted(TYPES.keys(), key=len, reverse=True)),
        params={
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": "",
            "__VIEWSTATEGENERATOR": "",
            "__EVENTVALIDATION": "",
            "ctl00$hfUrl": _search_url,
            "ctl00$tbxLogin": "",
            "ctl00$tbxSenha": "",
            "ctl00$conteudo$tbxNumero": "",
            "ctl00$conteudo$tbxAno": "",
            "ctl00$conteudo$tbxTextoPesquisa": "",
            "ctl00$conteudo$tbxTextoPesquisaNeg": "",
            "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_0": "CONTTXTORIGINAL",
            "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_1": "EMENTA",
            "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_2": "APELIDO",
            "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_3": "NOME",
            "ctl00$conteudo$tbxThesaurus": "",
            "ctl00$conteudo$rbOpThesaurus": "Todos",
            "ctl00$conteudo$tbxAssuntoGeral": "",
            "ctl00$conteudo$rbOpAssuntoGeral": "Todos",
            "ctl00$conteudo$tbxDataInicialNorma": "",
            "ctl00$conteudo$tbxDataFinalNorma": "",
            "ctl00$conteudo$ddlPublicacao": "",
            "ctl00$conteudo$tbxDataInicialPublicacao": "",
            "ctl00$conteudo$tbxDataFinalPublicacao": "",
            "ctl00$conteudo$tbxIniciativa": "",
            "ctl00$conteudo$tbxNumeroProjeto": "",
            "ctl00$conteudo$tbxAnoProjeto": "",
            "ctl00$conteudo$btnPesquisar": "Pesquisar",
            "ctl00$tbxNomeErro": "",
            "ctl00$tbxEmailErro": "",
            "ctl00$tbxMensagemErro": "",
            "ctl00$tbxLoginMob": "",
            "ctl00$tbxSenhaMob": "",
        },
        **kwargs,
    )


def _make_initial_search_html() -> str:
    return '\n    <html><body>\n      <form>\n        <input name="__VIEWSTATE" value="state-1" />\n        <input name="__VIEWSTATEGENERATOR" value="gen-1" />\n        <input name="__EVENTVALIDATION" value="ev-1" />\n      </form>\n    </body></html>\n    '


def _make_results_row(
    *,
    doc_id: int,
    title: str,
    summary: str,
    publication: str = "Publicada no DOE 01/01/2025",
    include_updated: bool = True,
    include_original: bool = True,
) -> str:
    links = []
    if include_original:
        links.append(
            f'<li><a href="texto.aspx?id={doc_id}&tipo=TEXTOORIGINAL"><div></div></a></li>'
        )
    if include_updated:
        links.append(
            f'<li><a href="texto.aspx?id={doc_id}&tipo=TEXTOATUALIZADO"><div></div></a></li>'
        )
    return f"""\n    <tr>\n      <td class="td-nome-norma col-md-4">\n        <span class="nome-norma">\n          <a href="texto.aspx?id={doc_id}&tipo=">{title}</a>\n        </span>\n        <span class="publicacao">{publication}</span>\n      </td>\n      <td class="textos col-md-2" id="textos">\n        <ul class="txt-desktop">\n          {"".join(links)}\n        </ul>\n      </td>\n      <td class="ementa-norma col-md-6">\n        <div class="fLeft">{summary}</div>\n        <a class="btn btn-info btn-xs fRight" href="dadosReferenciais.aspx?id={doc_id}">Dados Referenciais</a>\n      </td>\n    </tr>\n    """


def _make_results_page(rows: list[str], pager: str = "") -> BeautifulSoup:
    html = f"""\n    <html><body>\n      <div id="divResultado">\n        <table><tbody>{"".join(rows)}</tbody></table>\n      </div>\n      {pager}\n      <input name="__VIEWSTATE" value="state-next" />\n      <input name="__VIEWSTATEGENERATOR" value="gen-next" />\n      <input name="__EVENTVALIDATION" value="ev-next" />\n    </body></html>\n    """
    return BeautifulSoup(html, "lxml")


def _make_pager(*items: tuple[str, str, bool], include_prox: bool = False) -> str:
    parts = []
    for slot_id, label, active in items:
        class_attr = ' class="active"' if active else ""
        parts.append(f'<a id="{slot_id}"{class_attr}>{label}</a>')
    if include_prox:
        parts.append('<a id="lbtnProx">›</a>')
    return "<div class='pager'>" + "".join(parts) + "</div>"


def _make_doc_html(revogada: bool = False) -> bytes:
    revogada_div = '<div id="divRevogada">Revogada</div>' if revogada else ""
    html = f'\n    <html><body>\n      {revogada_div}\n      <div class="WordSection1">\n        <p><b>LEI ORDINARIA N.o 1/2020</b></p>\n        <p>Dispõe sobre matéria importante.</p>\n        <p>Este texto não substitui o publicado no Diário Oficial.</p>\n        <p><a href="/?lei">Lei relacionada</a></p>\n        <p><img src="foo.jpg" /></p>\n      </div>\n    </body></html>\n    '
    return html.encode("utf-8")


def _make_reference_html(revogada: bool = False) -> BeautifulSoup:
    revogada_div = '<div id="divRevogada">Revogada</div>' if revogada else ""
    html = f"\n    <html><body>\n      {revogada_div}\n      <table>\n        <tr><th>Data</th><td>22/12/2020</td></tr>\n        <tr><th>Ementa</th><td>Resumo de referência.</td></tr>\n        <tr><th>Iniciativa</th><td>Poder Executivo</td></tr>\n        <tr><th>Publicação</th><td>Publicação feita no DOE.</td></tr>\n        <tr><th>Assunto Geral</th><td>MEIO AMBIENTE.</td></tr>\n        <tr><th>Atualizações</th><td>Não consta atualização.</td></tr>\n        <tr><th>Indexação</th><td>INDICE 1.</td></tr>\n      </table>\n    </body></html>\n    "
    return BeautifulSoup(html, "lxml")


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 15
    REQUIRED_KEYS = {"Resolução Conjunta"}
    REQUIRE_INT_VALUES = True


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = PernambucoAlepeScraper
    STATE_NAME = "Pernambuco"

    def test_uses_string_not_text_in_bs4_calls(self):
        import src.scrapers.states.pe as mod

        source = inspect.getsource(mod)
        tree = ast.parse(source)

        class Visitor(ast.NodeVisitor):
            found_text_kw = False

            def visit_Call(self, node):
                for kw in node.keywords:
                    if kw.arg == "text":
                        self.found_text_kw = True
                self.generic_visit(node)

        visitor = Visitor()
        visitor.visit(tree)
        assert not visitor.found_text_kw


class TestSearchHelpers:
    def test_build_search_payload_is_year_only(self):
        scraper = _make_scraper()
        payload = scraper._build_search_payload(2025, {"__VIEWSTATE": "abc"})
        assert payload["ctl00$conteudo$tbxAno"] == "2025"
        assert payload["__VIEWSTATE"] == "abc"
        assert not any(("cblTipoNorma" in key for key in payload))

    def test_extract_norm_type_matches_known_prefixes(self):
        scraper = _make_scraper()
        assert scraper._extract_norm_type("Lei Ordinária n° 17.139") == "Lei Ordinária"
        assert (
            scraper._extract_norm_type("Constituição Estadual de 1989")
            == "Constituição Estadual"
        )

    def test_extract_documents_prefers_texto_atualizado(self):
        scraper = _make_scraper()
        row = _make_results_row(
            doc_id=1,
            title="Lei Ordinária n° 17.139",
            summary="Resumo 1",
            include_updated=True,
            include_original=True,
        )
        soup = _make_results_page([row])
        docs = scraper._extract_documents(soup, year=2025)
        assert len(docs) == 1
        assert docs[0]["type"] == "Lei Ordinária"
        assert docs[0]["document_url"].endswith("tipo=TEXTOATUALIZADO")
        assert docs[0]["_candidate_document_urls"][0].endswith("TEXTOATUALIZADO")
        assert docs[0]["_candidate_document_urls"][1].endswith("TEXTOORIGINAL")
        assert docs[0]["additional_data_url"].endswith("dadosReferenciais.aspx?id=1")

    def test_parse_pager_slots_reads_moving_window(self):
        scraper = _make_scraper()
        soup = _make_results_page(
            [],
            pager=_make_pager(
                ("lbtn1", "6", True), ("lbtn2", "7", False), ("lbtn3", "8", False)
            ),
        )
        slots = scraper._parse_pager_slots(soup)
        assert [(slot.slot_id, slot.page_number, slot.is_active) for slot in slots] == [
            ("lbtn1", 6, True),
            ("lbtn2", 7, False),
            ("lbtn3", 8, False),
        ]
