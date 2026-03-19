"""Tests for PernambucoAlepeScraper."""

import ast
import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest
from base_tests import ScraperClassTests, TypesConstantTests
from bs4 import BeautifulSoup
from conftest import make_base_scraper

from src.scraper.state_legislation.pernambuco import (
    SITUATIONS,
    TYPES,
    PernambucoAlepeScraper,
)
from src.services.request.service import FailedRequest


def _make_scraper(**kwargs) -> PernambucoAlepeScraper:
    """Instantiate PernambucoAlepeScraper bypassing __init__ (no network, no I/O)."""
    _search_url = "https://legis.alepe.pe.gov.br/Paginas/pesquisaAvancada.aspx"
    return make_base_scraper(
        PernambucoAlepeScraper,
        "https://legis.alepe.pe.gov.br",
        "PERNAMBUCO",
        TYPES,
        SITUATIONS,
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
    return """
    <html><body>
      <form>
        <input name="__VIEWSTATE" value="state-1" />
        <input name="__VIEWSTATEGENERATOR" value="gen-1" />
        <input name="__EVENTVALIDATION" value="ev-1" />
      </form>
    </body></html>
    """


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
    return f"""
    <tr>
      <td class="td-nome-norma col-md-4">
        <span class="nome-norma">
          <a href="texto.aspx?id={doc_id}&tipo=">{title}</a>
        </span>
        <span class="publicacao">{publication}</span>
      </td>
      <td class="textos col-md-2" id="textos">
        <ul class="txt-desktop">
          {"".join(links)}
        </ul>
      </td>
      <td class="ementa-norma col-md-6">
        <div class="fLeft">{summary}</div>
        <a class="btn btn-info btn-xs fRight" href="dadosReferenciais.aspx?id={doc_id}">Dados Referenciais</a>
      </td>
    </tr>
    """


def _make_results_page(rows: list[str], pager: str = "") -> BeautifulSoup:
    html = f"""
    <html><body>
      <div id="divResultado">
        <table><tbody>{"".join(rows)}</tbody></table>
      </div>
      {pager}
      <input name="__VIEWSTATE" value="state-next" />
      <input name="__VIEWSTATEGENERATOR" value="gen-next" />
      <input name="__EVENTVALIDATION" value="ev-next" />
    </body></html>
    """
    return BeautifulSoup(html, "html.parser")


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
    html = f"""
    <html><body>
      {revogada_div}
      <div class="WordSection1">
        <p><b>LEI ORDINARIA N.o 1/2020</b></p>
        <p>Dispõe sobre matéria importante.</p>
        <p>Este texto não substitui o publicado no Diário Oficial.</p>
        <p><a href="/?lei">Lei relacionada</a></p>
        <p><img src="foo.jpg" /></p>
      </div>
    </body></html>
    """
    return html.encode("utf-8")


def _make_reference_html(revogada: bool = False) -> BeautifulSoup:
    revogada_div = '<div id="divRevogada">Revogada</div>' if revogada else ""
    html = f"""
    <html><body>
      {revogada_div}
      <table>
        <tr><th>Data</th><td>22/12/2020</td></tr>
        <tr><th>Ementa</th><td>Resumo de referência.</td></tr>
        <tr><th>Iniciativa</th><td>Poder Executivo</td></tr>
        <tr><th>Publicação</th><td>Publicação feita no DOE.</td></tr>
        <tr><th>Assunto Geral</th><td>MEIO AMBIENTE.</td></tr>
        <tr><th>Atualizações</th><td>Não consta atualização.</td></tr>
        <tr><th>Indexação</th><td>INDICE 1.</td></tr>
      </table>
    </body></html>
    """
    return BeautifulSoup(html, "html.parser")


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 15
    REQUIRED_KEYS = {"Resolução Conjunta"}
    REQUIRE_INT_VALUES = True


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = PernambucoAlepeScraper
    STATE_NAME = "Pernambuco"

    def test_uses_string_not_text_in_bs4_calls(self):
        import src.scraper.state_legislation.pernambuco as mod

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
        assert not any("cblTipoNorma" in key for key in payload)

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
                ("lbtn1", "6", True),
                ("lbtn2", "7", False),
                ("lbtn3", "8", False),
            ),
        )
        slots = scraper._parse_pager_slots(soup)
        assert [(slot.slot_id, slot.page_number, slot.is_active) for slot in slots] == [
            ("lbtn1", 6, True),
            ("lbtn2", 7, False),
            ("lbtn3", 8, False),
        ]


class TestPagination:
    @pytest.mark.asyncio
    async def test_get_docs_links_walks_moving_page_windows(self):
        scraper = _make_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(_make_initial_search_html(), "html.parser")
        )

        page1 = _make_results_page(
            [_make_results_row(doc_id=1, title="Lei Ordinária n° 1", summary="R1")],
            pager=_make_pager(
                ("lbtn1", "1", True), ("lbtn2", "2", False), include_prox=True
            ),
        )
        page2 = _make_results_page(
            [_make_results_row(doc_id=2, title="Lei Ordinária n° 2", summary="R2")]
        )
        page3 = _make_results_page(
            [_make_results_row(doc_id=3, title="Lei Ordinária n° 3", summary="R3")],
            pager=_make_pager(("lbtn1", "3", True), ("lbtn2", "4", False)),
        )
        page4 = _make_results_page(
            [_make_results_row(doc_id=4, title="Lei Ordinária n° 4", summary="R4")]
        )

        scraper._post_results_page = AsyncMock(side_effect=[page1, page2, page3, page4])

        docs = await scraper._get_docs_links(2025)

        assert [doc["title"] for doc in docs] == [
            "Lei Ordinária n° 1",
            "Lei Ordinária n° 2",
            "Lei Ordinária n° 3",
            "Lei Ordinária n° 4",
        ]
        assert scraper._post_results_page.call_count == 4
        second_payload = scraper._post_results_page.call_args_list[1].args[0]
        third_payload = scraper._post_results_page.call_args_list[2].args[0]
        fourth_payload = scraper._post_results_page.call_args_list[3].args[0]
        assert second_payload["__EVENTTARGET"] == "ctl00$conteudo$lbtn2"
        assert third_payload["__EVENTTARGET"] == "ctl00$conteudo$lbtnProx"
        assert fourth_payload["__EVENTTARGET"] == "ctl00$conteudo$lbtn2"

    @pytest.mark.asyncio
    async def test_get_docs_links_skips_overlapping_pages_after_prox(self):
        scraper = _make_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(_make_initial_search_html(), "html.parser")
        )

        page16 = _make_results_page(
            [_make_results_row(doc_id=16, title="Lei Ordinária n° 16", summary="R16")],
            pager=_make_pager(
                ("lbtn1", "16", True),
                ("lbtn2", "17", False),
                ("lbtn3", "18", False),
                include_prox=True,
            ),
        )
        page17 = _make_results_page(
            [_make_results_row(doc_id=17, title="Lei Ordinária n° 17", summary="R17")]
        )
        page18 = _make_results_page(
            [_make_results_row(doc_id=18, title="Lei Ordinária n° 18", summary="R18")]
        )
        page18_overlap = _make_results_page(
            [_make_results_row(doc_id=18, title="Lei Ordinária n° 18", summary="R18")],
            pager=_make_pager(
                ("lbtn1", "18", True),
                ("lbtn2", "19", False),
                ("lbtn3", "20", False),
            ),
        )
        page19 = _make_results_page(
            [_make_results_row(doc_id=19, title="Lei Ordinária n° 19", summary="R19")]
        )
        page20 = _make_results_page(
            [_make_results_row(doc_id=20, title="Lei Ordinária n° 20", summary="R20")]
        )

        scraper._post_results_page = AsyncMock(
            side_effect=[page16, page17, page18, page18_overlap, page19, page20]
        )

        docs = await scraper._get_docs_links(2025)

        assert [doc["title"] for doc in docs] == [
            "Lei Ordinária n° 16",
            "Lei Ordinária n° 17",
            "Lei Ordinária n° 18",
            "Lei Ordinária n° 19",
            "Lei Ordinária n° 20",
        ]


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_resume_skip_checks_all_candidate_urls(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(
            side_effect=lambda url, title: url.endswith("TEXTOORIGINAL")
        )
        doc = {
            "title": "Lei 1/2020",
            "type": "Lei Ordinária",
            "document_url": "https://legis.alepe.pe.gov.br/texto.aspx?id=1&tipo=TEXTOATUALIZADO",
            "_candidate_document_urls": [
                "https://legis.alepe.pe.gov.br/texto.aspx?id=1&tipo=TEXTOATUALIZADO",
                "https://legis.alepe.pe.gov.br/texto.aspx?id=1&tipo=TEXTOORIGINAL",
            ],
        }
        result = await scraper._get_doc_data(doc, year=2020)
        assert result is None

    @pytest.mark.asyncio
    async def test_failed_fetch_saves_error_with_year_and_type(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper.request_service.fetch_bytes = AsyncMock(
            return_value=FailedRequest(
                url="https://legis.alepe.pe.gov.br/texto.aspx?id=1&tipo=TEXTOATUALIZADO",
                reason="boom",
            )
        )
        scraper._save_doc_error = AsyncMock()
        doc = {
            "title": "Lei 1/2020",
            "type": "Lei Ordinária",
            "document_url": "https://legis.alepe.pe.gov.br/texto.aspx?id=1&tipo=TEXTOATUALIZADO",
            "_candidate_document_urls": [
                "https://legis.alepe.pe.gov.br/texto.aspx?id=1&tipo=TEXTOATUALIZADO"
            ],
        }
        result = await scraper._get_doc_data(doc, year=2020)
        assert result is None
        scraper._save_doc_error.assert_called_once()
        kwargs = scraper._save_doc_error.call_args.kwargs
        assert kwargs["year"] == 2020
        assert kwargs["norm_type"] == "Lei Ordinária"
        assert kwargs["error_message"] == "boom"

    @pytest.mark.asyncio
    async def test_falls_back_from_updated_to_original(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper.request_service.fetch_bytes = AsyncMock(
            side_effect=[
                (b"<html><body><p>missing</p></body></html>", MagicMock()),
                (_make_doc_html(), MagicMock()),
            ]
        )
        scraper._capture_mhtml = AsyncMock(return_value=b"fake-mhtml")
        scraper._fetch_reference_page = AsyncMock(return_value=_make_reference_html())
        scraper._get_markdown = AsyncMock(return_value="# Lei\n\n" + "Texto. " * 30)
        doc = {
            "title": "Lei 1/2020",
            "type": "Lei Ordinária",
            "summary": "Resumo do resultado",
            "document_url": "https://legis.alepe.pe.gov.br/texto.aspx?id=1&tipo=TEXTOATUALIZADO",
            "additional_data_url": "https://legis.alepe.pe.gov.br/dadosReferenciais.aspx?id=1",
            "_candidate_document_urls": [
                "https://legis.alepe.pe.gov.br/texto.aspx?id=1&tipo=TEXTOATUALIZADO",
                "https://legis.alepe.pe.gov.br/texto.aspx?id=1&tipo=TEXTOORIGINAL",
            ],
        }

        result = await scraper._get_doc_data(doc, year=2020)

        assert result is not None
        assert result["document_url"].endswith("TEXTOORIGINAL")
        assert result["text_version"] == "TEXTOORIGINAL"
        assert result["summary"] == "Resumo de referência."
        assert result["publication"] == "Publicação feita no DOE."
        assert result["situation"] == "Não consta revogação expressa"

    @pytest.mark.asyncio
    async def test_valid_doc_returns_mhtml_source_metadata(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper.request_service.fetch_bytes = AsyncMock(
            return_value=(
                _make_doc_html(),
                MagicMock(),
            )
        )
        scraper._capture_mhtml = AsyncMock(return_value=b"fake-mhtml")
        scraper._fetch_reference_page = AsyncMock(return_value=_make_reference_html())
        valid_md = "# Lei Ordinária 1/2020\n\n" + "Texto da lei. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = {
            "title": "Lei 1/2020",
            "type": "Lei Ordinária",
            "document_url": "https://legis.alepe.pe.gov.br/texto.aspx?id=1&tipo=TEXTOATUALIZADO",
            "additional_data_url": "https://legis.alepe.pe.gov.br/dadosReferenciais.aspx?id=1",
            "_candidate_document_urls": [
                "https://legis.alepe.pe.gov.br/texto.aspx?id=1&tipo=TEXTOATUALIZADO"
            ],
        }

        result = await scraper._get_doc_data(doc, year=2020)

        assert result is not None
        assert result["year"] == 2020
        assert result["type"] == "Lei Ordinária"
        assert result["text_version"] == "TEXTOATUALIZADO"
        assert result["_content_extension"] == ".mhtml"
        assert result["_raw_content"] == b"fake-mhtml"
        assert result["initiative"] == "Poder Executivo"
        assert result["date"] == "22/12/2020"
        assert result["indexation"] == "INDICE 1."

    @pytest.mark.asyncio
    async def test_revogada_doc_sets_situation(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper.request_service.fetch_bytes = AsyncMock(
            return_value=(
                _make_doc_html(revogada=True),
                MagicMock(),
            )
        )
        scraper._capture_mhtml = AsyncMock(return_value=b"fake-mhtml")
        scraper._fetch_reference_page = AsyncMock(return_value=_make_reference_html())
        scraper._get_markdown = AsyncMock(return_value="# Lei\n\n" + "Texto. " * 30)
        doc = {
            "title": "Lei 1/2020",
            "type": "Lei Ordinária",
            "document_url": "https://legis.alepe.pe.gov.br/texto.aspx?id=1&tipo=TEXTOATUALIZADO",
            "_candidate_document_urls": [
                "https://legis.alepe.pe.gov.br/texto.aspx?id=1&tipo=TEXTOATUALIZADO"
            ],
        }
        result = await scraper._get_doc_data(doc, year=2020)
        assert result is not None
        assert result["situation"] == "Revogada"
