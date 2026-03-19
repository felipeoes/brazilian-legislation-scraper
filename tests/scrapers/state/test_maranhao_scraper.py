"""Tests for MaranhaoAlemaScraper.

Run with:
    .venv/bin/pytest tests/test_maranhao_scraper.py -v
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from bs4 import BeautifulSoup

from src.scraper.state_legislation.maranhao import (
    JSFFormState,
    SITUATIONS,
    TYPES,
    MaranhaoAlemaScraper,
)
from base_tests import TypesConstantTests
from conftest import make_base_scraper, assert_resume_skips


class FakeResponse:
    def __init__(self, text: str):
        self._text = text

    async def text(self, errors: str = "replace") -> str:
        return self._text


def _make_scraper(**kwargs) -> MaranhaoAlemaScraper:
    defaults = {"_rows_per_page": 10, "_scraped_constitution": False}
    return make_base_scraper(
        MaranhaoAlemaScraper,
        "https://legislacao.al.ma.leg.br",
        "MARANHAO",
        TYPES,
        situations=SITUATIONS,
        **{**defaults, **kwargs},
    )


def _build_live_scraper(save_dir) -> MaranhaoAlemaScraper:
    return MaranhaoAlemaScraper(
        year_start=2025,
        year_end=2025,
        docs_save_dir=save_dir,
        overwrite=True,
        rps=5,
    )


def _make_valid_md() -> str:
    return "# Decreto\n\nTexto do decreto. " * 30


def _make_result_rows_html(
    *, count: int = 3, norm_label: str = "Lei Ordinária", start: int = 1
) -> str:
    rows = ""
    for idx in range(start, start + count):
        rows += f"""
        <tr class="ui-widget-content">
            <td class="col-botao-acao-toogler"></td>
            <td><label class="ui-outputlabel ui-widget">{11000 + idx}/2020</label></td>
            <td><label class="ui-outputlabel ui-widget">{norm_label}</label></td>
            <td><label class="ui-outputlabel ui-widget">PL {idx:03d}/2020</label></td>
            <td><label class="ui-outputlabel ui-widget">{idx:02d}/01/2020</label></td>
            <td><label class="ui-outputlabel ui-widget ementa">Ementa do doc {idx}</label></td>
            <td><a href="/pdf/doc{idx}.pdf">PDF</a></td>
        </tr>"""
    return rows


def _make_search_page_html(
    *,
    total: int | None = None,
    rows_html: str = "",
    action: str = "/ged/busca.html?dswid=905",
    viewstate: str = "vs-1",
    clientwindow: str = "905",
    dispatcher: str = "dispatch-1",
) -> str:
    total_html = ""
    table_html = ""
    row_state_html = ""
    if total is not None:
        total_html = (
            '<div class="ui-datatable-header ui-widget-header ui-corner-top">'
            f"Consulta de Documentos Eletrônicos - {total} registro(s) encontrado(s)"
            "</div>"
        )
        table_html = f'<table id="table_resultados"><tbody>{rows_html}</tbody></table>'
        row_state_html = (
            '<input type="hidden" name="table_resultados_rowExpansionState" value="" />'
        )

    return f"""
    <html><body>
        <form id="j_idt44" name="j_idt44" method="post" action="{action}">
            <input type="hidden" name="j_idt44" value="j_idt44" />
            <input type="hidden" name="j_idt46_dsprt" value="{dispatcher}" />
            <input type="hidden" name="javax.faces.ViewState" value="{viewstate}" />
            <input type="hidden" name="javax.faces.ClientWindow" value="{clientwindow}" />
            {row_state_html}
            <input id="in_tipo_doc_focus" name="in_tipo_doc_focus" type="text" value="" />
            <select id="in_tipo_doc_input" name="in_tipo_doc_input">
                <option value="1">Lei</option>
                <option value="5">Emenda Constitucional</option>
            </select>
            <input id="in_nro_doc" name="in_nro_doc" type="text" value="" />
            <input id="in_ano_doc" name="in_ano_doc" type="text" value="" />
            <textarea id="ementa" name="ementa"></textarea>
            <input id="in_nro_proj_lei" name="in_nro_proj_lei" type="text" value="" />
            <input id="in_ano_proj_lei" name="in_ano_proj_lei" type="text" value="" />
            <input id="in_ini_public_input" name="in_ini_public_input" type="text" value="" />
            <input id="in_fim_public_input" name="in_fim_public_input" type="text" value="" />
            <button id="j_idt72" name="j_idt72" type="submit"><span>Consultar</span></button>
        </form>
        {total_html}
        {table_html}
    </body></html>"""


def _make_subtype_panel_html(field_name: str = "j_idt54") -> str:
    return f"""
    <div id="painel_tipo_doc">
        <table id="{field_name}" role="presentation" class="ui-selectmanycheckbox ui-widget">
            <tr>
                <td>
                    <input id="{field_name}:0" name="{field_name}" type="checkbox" value="2" checked="checked" />
                    <label for="{field_name}:0">Lei Ordinária</label>
                </td>
            </tr>
            <tr>
                <td>
                    <input id="{field_name}:1" name="{field_name}" type="checkbox" value="3" checked="checked" />
                    <label for="{field_name}:1">Lei Complementar</label>
                </td>
            </tr>
        </table>
    </div>"""


def _make_partial_xml(*updates: tuple[str, str]) -> str:
    inner = "".join(
        f'<update id="{update_id}"><![CDATA[{value}]]></update>'
        for update_id, value in updates
    )
    return f"<?xml version='1.0' encoding='UTF-8'?><partial-response><changes>{inner}</changes></partial-response>"


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 6
    REQUIRED_KEYS = {"Constituição Estadual", "Lei"}
    REQUIRE_INT_VALUES = False

    def test_lei_is_dict_with_subtypes(self):
        assert isinstance(TYPES["Lei"], dict)
        assert TYPES["Lei"]["subtypes"] == {
            "Lei Ordinária": 2,
            "Lei Complementar": 3,
        }


class TestFormParsing:
    def test_extract_form_state_reads_dynamic_fields(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_search_page_html(), "html.parser")

        state = scraper._extract_form_state(soup)

        assert state is not None
        assert (
            state.action_url
            == "https://legislacao.al.ma.leg.br/ged/busca.html?dswid=905"
        )
        assert state.fields["j_idt44"] == "j_idt44"
        assert state.fields["j_idt46_dsprt"] == "dispatch-1"
        assert state.fields["javax.faces.ViewState"] == "vs-1"
        assert state.search_button_name == "j_idt72"

    def test_extract_subtypes_reads_field_name_and_values(self):
        scraper = _make_scraper()

        field_name, options = scraper._extract_subtypes(_make_subtype_panel_html())

        assert field_name == "j_idt54"
        assert options == {"Lei Ordinária": "2", "Lei Complementar": "3"}

    def test_build_search_payload_includes_dynamic_subtype_and_tokens(self):
        scraper = _make_scraper()
        state = JSFFormState(
            action_url="https://legislacao.al.ma.leg.br/ged/busca.html?dswid=905",
            fields={
                "j_idt44": "j_idt44",
                "j_idt46_dsprt": "dispatch-1",
                "javax.faces.ViewState": "vs-1",
                "javax.faces.ClientWindow": "905",
                "in_tipo_doc_focus": "",
                "in_nro_doc": "",
                "in_ano_doc": "",
                "ementa": "",
                "in_nro_proj_lei": "",
                "in_ano_proj_lei": "",
                "in_ini_public_input": "",
                "in_fim_public_input": "",
            },
            search_button_name="j_idt72",
        )

        payload = scraper._build_search_payload(
            state,
            1,
            2020,
            subtype_field_name="j_idt54",
            subtype_values=("3",),
        )

        assert ("j_idt54", "3") in payload
        assert ("in_ano_doc", "2020") in payload
        assert ("j_idt72", "") in payload
        assert ("javax.faces.ViewState", "vs-1") in payload


class TestGetDocsLinks:
    def test_parses_docs_from_results_html(self):
        scraper = _make_scraper()
        html = _make_result_rows_html(count=2, norm_label="Lei Complementar")

        docs = scraper._get_docs_links(html, "Lei")

        assert docs is not None
        assert len(docs) == 2
        assert docs[0]["title"] == "Lei Complementar - 11001/2020"
        assert docs[0]["project"] == "PL 001/2020"
        assert docs[0]["publication"] == "01/01/2020"
        assert docs[0]["summary"] == "Ementa do doc 1"
        assert docs[0]["pdf_link"] == "https://legislacao.al.ma.leg.br/pdf/doc1.pdf"

    def test_empty_rows_return_empty_list(self):
        scraper = _make_scraper()

        docs = scraper._get_docs_links("<html><body></body></html>", "Lei")

        assert docs == []


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_resume_skip_returns_none(self):
        await assert_resume_skips(
            _make_scraper(),
            {"title": "Lei 001", "pdf_link": "/pdf/lei001.pdf", "year": 2020},
        )

    @pytest.mark.asyncio
    async def test_invalid_markdown_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._download_and_convert = AsyncMock(
            return_value=("short", b"raw", ".pdf")
        )
        scraper._save_doc_error = AsyncMock()

        result = await scraper._get_doc_data(
            {"title": "Lei 001", "pdf_link": "/pdf/lei001.pdf", "year": 2020}
        )

        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_valid_doc_returns_correct_shape(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._download_and_convert = AsyncMock(
            return_value=(_make_valid_md(), b"raw_bytes", ".pdf")
        )

        result = await scraper._get_doc_data(
            {
                "title": "Lei 001",
                "pdf_link": "/pdf/lei001.pdf",
                "year": 2020,
                "type": "Lei",
                "situation": "Vigente",
            }
        )

        assert result is not None
        assert result["document_url"] == "/pdf/lei001.pdf"
        assert result["_raw_content"] == b"raw_bytes"
        assert result["_content_extension"] == ".pdf"


class TestSearchFlow:
    @pytest.mark.asyncio
    async def test_search_norms_uses_ajax_subtype_then_full_search(self):
        scraper = _make_scraper()
        scraper.request_service.make_request = AsyncMock(
            side_effect=[
                FakeResponse(_make_search_page_html()),
                FakeResponse(
                    _make_partial_xml(
                        ("painel_tipo_doc", _make_subtype_panel_html()),
                        ("j_id1:javax.faces.ViewState:0", "vs-2"),
                        ("j_id1:javax.faces.ClientWindow:0", "905"),
                    )
                ),
                FakeResponse(
                    _make_search_page_html(
                        total=5,
                        rows_html=_make_result_rows_html(
                            count=1, norm_label="Lei Complementar"
                        ),
                        viewstate="vs-3",
                    )
                ),
            ]
        )

        state, soup, subtype_field_name, subtype_values = await scraper._search_norms(
            "Lei",
            "1",
            2020,
            subtype="Lei Complementar",
            subtype_id="3",
        )

        assert state is not None
        assert soup is not None
        assert subtype_field_name == "j_idt54"
        assert subtype_values == ("3",)
        assert state.fields["javax.faces.ViewState"] == "vs-3"

        ajax_payload = scraper.request_service.make_request.call_args_list[1].kwargs[
            "payload"
        ]
        search_payload = scraper.request_service.make_request.call_args_list[2].kwargs[
            "payload"
        ]

        assert ("javax.faces.source", "in_tipo_doc") in ajax_payload
        assert ("javax.faces.partial.render", "painel_tipo_doc") in ajax_payload
        assert ("j_idt54", "3") in search_payload
        assert ("j_idt72", "") in search_payload

    @pytest.mark.asyncio
    async def test_fetch_docs_page_parses_table_partial_response(self):
        scraper = _make_scraper()
        scraper.request_service.make_request = AsyncMock(
            return_value=FakeResponse(
                _make_partial_xml(
                    ("table_resultados", _make_result_rows_html(count=2, start=11)),
                )
            )
        )
        form_state = JSFFormState(
            action_url="https://legislacao.al.ma.leg.br/ged/busca.html?dswid=905",
            fields={
                "j_idt44": "j_idt44",
                "j_idt46_dsprt": "dispatch-2",
                "javax.faces.ViewState": "vs-3",
                "javax.faces.ClientWindow": "905",
                "in_tipo_doc_focus": "",
                "in_nro_doc": "",
                "in_ano_doc": "2020",
                "ementa": "",
                "in_nro_proj_lei": "",
                "in_ano_proj_lei": "",
                "in_ini_public_input": "",
                "in_fim_public_input": "",
                "table_resultados_rowExpansionState": "",
            },
            search_button_name="j_idt72",
        )

        docs = await scraper._fetch_docs_page(
            2,
            form_state=form_state,
            norm_type_id="5",
            year=2020,
            effective_type="Emenda Constitucional",
        )

        assert len(docs) == 2
        assert docs[0]["title"] == "Lei Ordinária - 11011/2020"
        assert docs[1]["pdf_link"] == "https://legislacao.al.ma.leg.br/pdf/doc12.pdf"

    @pytest.mark.asyncio
    async def test_scrape_norms_combines_page_one_with_paginated_results(self):
        scraper = _make_scraper()
        search_soup = BeautifulSoup(
            _make_search_page_html(
                total=12,
                rows_html=_make_result_rows_html(
                    count=10, norm_label="Emenda Constitucional"
                ),
                viewstate="vs-4",
                dispatcher="dispatch-4",
            ),
            "html.parser",
        )
        form_state = JSFFormState(
            action_url="https://legislacao.al.ma.leg.br/ged/busca.html?dswid=905",
            fields={
                "j_idt44": "j_idt44",
                "j_idt46_dsprt": "dispatch-4",
                "javax.faces.ViewState": "vs-4",
                "javax.faces.ClientWindow": "905",
                "table_resultados_rowExpansionState": "",
                "in_tipo_doc_focus": "",
                "in_nro_doc": "",
                "in_ano_doc": "2020",
                "ementa": "",
                "in_nro_proj_lei": "",
                "in_ano_proj_lei": "",
                "in_ini_public_input": "",
                "in_fim_public_input": "",
            },
            search_button_name="j_idt72",
        )
        scraper._search_norms = AsyncMock(
            return_value=(form_state, search_soup, "", ())
        )
        scraper._fetch_docs_page = AsyncMock(
            return_value=[
                {
                    "title": "Emenda Constitucional - 12001/2020",
                    "publication": "11/01/2020",
                    "project": "PL 011/2020",
                    "summary": "Extra",
                    "pdf_link": "https://legislacao.al.ma.leg.br/pdf/extra.pdf",
                }
            ]
        )
        scraper._process_documents = AsyncMock(return_value=[{"title": "saved"}])

        results = await scraper._scrape_norms(
            "Emenda Constitucional",
            "5",
            2020,
            "Não consta",
        )

        assert results == [{"title": "saved"}]
        scraper._fetch_docs_page.assert_awaited_once()
        await_args = scraper._process_documents.await_args
        assert await_args is not None
        documents = await_args.args[0]
        assert len(documents) == 11


class TestScrapeConstitution:
    @pytest.mark.asyncio
    async def test_already_scraped_sets_flag_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=True)
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(
                '<html><body><object class="view-pdf-constituicao" data="http://example.com/ce.pdf"></object></body></html>',
                "html.parser",
            )
        )

        result = await scraper._scrape_constitution(
            "Constituição Estadual", "constituicao-estadual/detalhe.html"
        )

        assert result is None
        assert scraper._scraped_constitution is True

    @pytest.mark.asyncio
    async def test_valid_md_saves_and_sets_flag(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(
                '<html><body><object class="view-pdf-constituicao" data="http://example.com/ce.pdf"></object></body></html>',
                "html.parser",
            )
        )
        scraper._download_and_convert = AsyncMock(
            return_value=(_make_valid_md(), b"raw", ".pdf")
        )
        scraper._save_doc_result = AsyncMock()

        result = await scraper._scrape_constitution(
            "Constituição Estadual", "constituicao-estadual/detalhe.html"
        )

        assert result is not None
        assert result["title"] == "Constituição Estadual do Maranhão"
        assert scraper._scraped_constitution is True
        scraper._save_doc_result.assert_awaited_once()


class TestScrapeSituationType:
    @pytest.mark.asyncio
    async def test_constitution_already_scraped_skips(self):
        scraper = _make_scraper(_scraped_constitution=True)
        scraper._scrape_constitution = AsyncMock()

        results = await scraper._scrape_situation_type(
            2020,
            "Não consta",
            "Não consta",
            "Constituição Estadual",
            "constituicao-estadual/detalhe.html",
        )

        scraper._scrape_constitution.assert_not_called()
        assert results == []

    @pytest.mark.asyncio
    async def test_nested_subtype_calls_scrape_norms_per_subtype(self):
        scraper = _make_scraper()
        scraper._scrape_norms = AsyncMock(return_value=[])

        await scraper._scrape_situation_type(
            2020, "Não consta", "Não consta", "Lei", TYPES["Lei"]
        )

        assert scraper._scrape_norms.await_count == 2

    @pytest.mark.asyncio
    async def test_plain_type_calls_scrape_norms_once(self):
        scraper = _make_scraper()
        scraper._scrape_norms = AsyncMock(return_value=[])

        await scraper._scrape_situation_type(
            2020, "Não consta", "Não consta", "Emenda Constitucional", 5
        )

        scraper._scrape_norms.assert_awaited_once()


class TestScrapeYear:
    @pytest.mark.asyncio
    async def test_collects_all_type_results(self):
        scraper = _make_scraper()
        scraper._scrape_situation_type = AsyncMock(return_value=[{"title": "doc"}])

        results = await scraper._scrape_year(2020)

        assert scraper._scrape_situation_type.await_count == 6
        assert len(results) == 6

    @pytest.mark.asyncio
    async def test_handles_per_type_exception(self):
        scraper = _make_scraper()

        async def side_effect(year, situation, situation_id, norm_type, norm_type_id):
            if norm_type == "Emenda Constitucional":
                raise RuntimeError("network error")
            return [{"title": f"doc_{norm_type}"}]

        scraper._scrape_situation_type = AsyncMock(side_effect=side_effect)

        results = await scraper._scrape_year(2020)

        assert len(results) == 5


@pytest.mark.integration
class TestMaranhaoIntegration:
    @pytest.mark.asyncio
    async def test_live_lei_complementar_search_2025_returns_results(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmpdir:
            scraper = _build_live_scraper(Path(tmpdir))
            try:
                await scraper._load_scraped_keys(2025)
                (
                    form_state,
                    soup,
                    subtype_field_name,
                    subtype_values,
                ) = await scraper._search_norms(
                    "Lei",
                    "1",
                    2025,
                    subtype="Lei Complementar",
                    subtype_id="3",
                )

                assert form_state is not None
                assert soup is not None
                assert subtype_field_name
                assert subtype_values == ("3",)

                total_docs = scraper._get_total_docs(soup)
                docs = await scraper._get_docs_links(soup, "Lei Complementar") or []

                assert total_docs > 0
                assert docs
                assert len(docs) <= total_docs
                assert all(
                    doc["title"].startswith("Lei Complementar -") for doc in docs
                )
            finally:
                await scraper.cleanup()

    @pytest.mark.asyncio
    async def test_live_lei_ordinaria_page_two_fetches_distinct_results(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmpdir:
            scraper = _build_live_scraper(Path(tmpdir))
            try:
                await scraper._load_scraped_keys(2025)
                (
                    form_state,
                    soup,
                    subtype_field_name,
                    subtype_values,
                ) = await scraper._search_norms(
                    "Lei",
                    "1",
                    2025,
                    subtype="Lei Ordinária",
                    subtype_id="2",
                )

                assert form_state is not None
                assert soup is not None

                total_docs = scraper._get_total_docs(soup)
                assert total_docs > scraper._rows_per_page

                page_one_docs = (
                    await scraper._get_docs_links(soup, "Lei Ordinária") or []
                )
                page_two_docs = await scraper._fetch_docs_page(
                    2,
                    form_state=form_state,
                    norm_type_id="1",
                    year=2025,
                    effective_type="Lei Ordinária",
                    subtype_field_name=subtype_field_name,
                    subtype_values=subtype_values,
                )

                assert len(page_one_docs) == scraper._rows_per_page
                assert page_two_docs
                assert page_one_docs[0]["title"] != page_two_docs[0]["title"]
            finally:
                await scraper.cleanup()
