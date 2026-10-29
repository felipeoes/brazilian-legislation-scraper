"""Tests for MaranhaoAlemaScraper.

Run with:
    .venv/bin/pytest tests/test_maranhao_scraper.py -v
"""

from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import TypesConstantTests

from src.scrapers.states.ma import TYPES, JSFFormState, MaranhaoAlemaScraper


class FakeResponse:
    def __init__(self, text: str):
        self._text = text

    async def text(self, errors: str = "replace") -> str:
        return self._text


def _make_scraper(**kwargs) -> MaranhaoAlemaScraper:
    defaults = {"_rows_per_page": 10}
    return make_scraper(MaranhaoAlemaScraper, **{**defaults, **kwargs})


def _make_valid_md() -> str:
    return "# Decreto\n\nTexto do decreto. " * 30


def _make_result_rows_html(
    *, count: int = 3, norm_label: str = "Lei Ordinária", start: int = 1
) -> str:
    rows = ""
    for idx in range(start, start + count):
        rows += f'\n        <tr class="ui-widget-content">\n            <td class="col-botao-acao-toogler"></td>\n            <td><label class="ui-outputlabel ui-widget">{11000 + idx}/2020</label></td>\n            <td><label class="ui-outputlabel ui-widget">{norm_label}</label></td>\n            <td><label class="ui-outputlabel ui-widget">PL {idx:03d}/2020</label></td>\n            <td><label class="ui-outputlabel ui-widget">{idx:02d}/01/2020</label></td>\n            <td><label class="ui-outputlabel ui-widget ementa">Ementa do doc {idx}</label></td>\n            <td><a href="/pdf/doc{idx}.pdf">PDF</a></td>\n        </tr>'
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
        total_html = f'<div class="ui-datatable-header ui-widget-header ui-corner-top">Consulta de Documentos Eletrônicos - {total} registro(s) encontrado(s)</div>'
        table_html = f'<table id="table_resultados"><tbody>{rows_html}</tbody></table>'
        row_state_html = (
            '<input type="hidden" name="table_resultados_rowExpansionState" value="" />'
        )
    return f'\n    <html><body>\n        <form id="j_idt44" name="j_idt44" method="post" action="{action}">\n            <input type="hidden" name="j_idt44" value="j_idt44" />\n            <input type="hidden" name="j_idt46_dsprt" value="{dispatcher}" />\n            <input type="hidden" name="javax.faces.ViewState" value="{viewstate}" />\n            <input type="hidden" name="javax.faces.ClientWindow" value="{clientwindow}" />\n            {row_state_html}\n            <input id="in_tipo_doc_focus" name="in_tipo_doc_focus" type="text" value="" />\n            <select id="in_tipo_doc_input" name="in_tipo_doc_input">\n                <option value="1">Lei</option>\n                <option value="5">Emenda Constitucional</option>\n            </select>\n            <input id="in_nro_doc" name="in_nro_doc" type="text" value="" />\n            <input id="in_ano_doc" name="in_ano_doc" type="text" value="" />\n            <textarea id="ementa" name="ementa"></textarea>\n            <input id="in_nro_proj_lei" name="in_nro_proj_lei" type="text" value="" />\n            <input id="in_ano_proj_lei" name="in_ano_proj_lei" type="text" value="" />\n            <input id="in_ini_public_input" name="in_ini_public_input" type="text" value="" />\n            <input id="in_fim_public_input" name="in_fim_public_input" type="text" value="" />\n            <button id="j_idt72" name="j_idt72" type="submit"><span>Consultar</span></button>\n        </form>\n        {total_html}\n        {table_html}\n    </body></html>'


def _make_subtype_panel_html(field_name: str = "j_idt54") -> str:
    return f'\n    <div id="painel_tipo_doc">\n        <table id="{field_name}" role="presentation" class="ui-selectmanycheckbox ui-widget">\n            <tr>\n                <td>\n                    <input id="{field_name}:0" name="{field_name}" type="checkbox" value="2" checked="checked" />\n                    <label for="{field_name}:0">Lei Ordinária</label>\n                </td>\n            </tr>\n            <tr>\n                <td>\n                    <input id="{field_name}:1" name="{field_name}" type="checkbox" value="3" checked="checked" />\n                    <label for="{field_name}:1">Lei Complementar</label>\n                </td>\n            </tr>\n        </table>\n    </div>'


def _make_partial_xml(*updates: tuple[str, str]) -> str:
    inner = "".join(
        (
            f'<update id="{update_id}"><![CDATA[{value}]]></update>'
            for update_id, value in updates
        )
    )
    return f"<?xml version='1.0' encoding='UTF-8'?><partial-response><changes>{inner}</changes></partial-response>"


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 6
    REQUIRED_KEYS = {"Constituição Estadual", "Lei"}
    REQUIRE_INT_VALUES = False

    def test_lei_is_dict_with_subtypes(self):
        assert isinstance(TYPES["Lei"], dict)
        assert TYPES["Lei"]["subtypes"] == {"Lei Ordinária": 2, "Lei Complementar": 3}


class TestFormParsing:
    def test_extract_form_state_reads_dynamic_fields(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_search_page_html(), "lxml")
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
            state, 1, 2020, subtype_field_name="j_idt54", subtype_values=("3",)
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
