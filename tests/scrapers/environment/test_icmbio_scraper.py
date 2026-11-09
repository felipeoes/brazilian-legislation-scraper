"Parser and metadata regression tests for icmbio_scraper."

from unittest.mock import AsyncMock

import fitz
import pytest
from conftest import make_mock_response, make_scraper

from src.scrapers.environment.icmbio import (
    COLUMNS,
    DATASET_ID,
    MODEL_ID,
    PAGE_SIZE,
    REPORT_ID,
    VISUAL_ID,
    ICMBioScraper,
)


class TestICMBioBuildQueryPayload:
    def test_first_page_no_restart_tokens(self):
        payload = ICMBioScraper._build_query_payload()
        binding = payload["queries"][0]["Query"]["Commands"][0][
            "SemanticQueryDataShapeCommand"
        ]["Binding"]
        window = binding["DataReduction"]["Primary"]["Window"]
        assert "RestartTokens" not in window
        assert window["Count"] == PAGE_SIZE
        assert payload["modelId"] == MODEL_ID

    def test_first_page_application_context(self):
        payload = ICMBioScraper._build_query_payload()
        ctx = payload["queries"][0]["ApplicationContext"]
        assert ctx["DatasetId"] == DATASET_ID
        assert ctx["Sources"][0]["ReportId"] == REPORT_ID
        assert ctx["Sources"][0]["VisualId"] == VISUAL_ID

    def test_pagination_page_includes_restart_tokens(self):
        tokens = [["tok_a", "tok_b"]]
        payload = ICMBioScraper._build_query_payload(restart_tokens=tokens)
        binding = payload["queries"][0]["Query"]["Commands"][0][
            "SemanticQueryDataShapeCommand"
        ]["Binding"]
        window = binding["DataReduction"]["Primary"]["Window"]
        assert window["RestartTokens"] == tokens

    def test_select_has_all_columns(self):
        payload = ICMBioScraper._build_query_payload()
        select = payload["queries"][0]["Query"]["Commands"][0][
            "SemanticQueryDataShapeCommand"
        ]["Query"]["Select"]
        col_names = [item["Column"]["Property"] for item in select]
        assert col_names == COLUMNS

    def test_version_field(self):
        payload = ICMBioScraper._build_query_payload()
        assert payload["version"] == "1.0.0"


class TestICMBioParseDsrRows:
    def _make_ds(self, ph_rows, value_dicts=None):
        ds = {"PH": [{"DM0": ph_rows}]}
        if value_dicts is not None:
            ds["ValueDicts"] = value_dicts
        return ds

    def _schema_row(self, dn_map: dict) -> dict:
        """Build a schema row where dn_map maps column-index to DN value."""
        schema = []
        for i in range(len(COLUMNS)):
            if i in dn_map:
                schema.append({"DN": dn_map[i]})
            else:
                schema.append({})
        return {"S": schema}

    def test_single_row_no_repeat(self):
        ds = self._make_ds(
            [
                {
                    "C": [
                        "em vigência",
                        1609459200000,
                        "DOU 1",
                        "Summary",
                        "https://in.gov.br/doc",
                        "IN 1",
                        "Fauna",
                    ]
                }
            ]
        )
        rows = ICMBioScraper._parse_dsr_rows(ds)
        assert len(rows) == 1
        r = rows[0]
        assert r["condicao"] == "em vigência"
        assert r["instrumento"] == "DOU 1"
        assert r["ementa"] == "Summary"
        assert r["link_dou"] == "https://in.gov.br/doc"
        assert r["ato"] == "IN 1"
        assert r["assunto"] == "Fauna"

    def test_epoch_ms_converted_to_date_string(self):
        ds = self._make_ds([{"C": ["em vigência", 1609459200000, "", "", "", "", ""]}])
        rows = ICMBioScraper._parse_dsr_rows(ds)
        assert rows[0]["publicacao"] == "01/01/2021"

    def test_repeat_bitmask_carries_previous_value(self):
        ds = self._make_ds(
            [
                {
                    "C": [
                        "em vigência",
                        1609459200000,
                        "DOU",
                        "Ementa A",
                        "https://in.gov.br/a",
                        "IN 1",
                        "Fauna",
                    ]
                },
                {
                    "C": [
                        1617235200000,
                        "DOU2",
                        "Ementa B",
                        "https://in.gov.br/b",
                        "IN 2",
                        "Flora",
                    ],
                    "R": 1,
                },
            ]
        )
        rows = ICMBioScraper._parse_dsr_rows(ds)
        assert rows[1]["condicao"] == "em vigência"
        assert rows[1]["ato"] == "IN 2"

    def test_value_dict_resolution(self):
        schema_row = self._schema_row({0: "D0"})
        ds = self._make_ds(
            [
                schema_row,
                {
                    "C": [
                        1,
                        1609459200000,
                        "DOU",
                        "Ementa",
                        "https://in.gov.br/x",
                        "IN 5",
                        "Fauna",
                    ]
                },
            ],
            value_dicts={"D0": ["revogado", "em vigência"]},
        )
        rows = ICMBioScraper._parse_dsr_rows(
            ds, accumulated_dicts={"D0": ["revogado", "em vigência"]}
        )
        assert rows[1]["condicao"] == "em vigência"

    def test_value_dict_out_of_bounds_returns_empty(self):
        schema_row = self._schema_row({0: "D0"})
        ds = self._make_ds(
            [
                schema_row,
                {
                    "C": [
                        99,
                        1609459200000,
                        "DOU",
                        "Ementa",
                        "https://in.gov.br/x",
                        "IN 5",
                        "Fauna",
                    ]
                },
            ]
        )
        rows = ICMBioScraper._parse_dsr_rows(ds, accumulated_dicts={"D0": ["revogado"]})
        assert rows[1]["condicao"] == ""

    def test_none_value_becomes_empty_string(self):
        ds = self._make_ds([{"C": ["status"]}])
        rows = ICMBioScraper._parse_dsr_rows(ds)
        assert rows[0]["publicacao"] == ""
        assert rows[0]["link_dou"] == ""

    def test_empty_accumulated_dicts_falls_back_to_page_value_dicts(self):
        schema_row = self._schema_row({0: "D0"})
        ds = self._make_ds(
            [
                schema_row,
                {"C": [0, 1609459200000, "", "", "https://in.gov.br/y", "IN 3", ""]},
            ],
            value_dicts={"D0": ["em vigência", "revogado"]},
        )
        rows = ICMBioScraper._parse_dsr_rows(ds, accumulated_dicts={})
        assert rows[1]["condicao"] == "em vigência"

    def test_cross_page_accumulated_dicts_take_priority(self):
        schema_row = self._schema_row({0: "D0"})
        ds = self._make_ds(
            [
                schema_row,
                {"C": [1, 1609459200000, "", "", "https://in.gov.br/z", "IN 9", ""]},
            ],
            value_dicts={"D0": ["old_value"]},
        )
        accumulated = {"D0": ["revogado", "em vigência com alteração"]}
        rows = ICMBioScraper._parse_dsr_rows(ds, accumulated_dicts=accumulated)
        assert rows[1]["condicao"] == "em vigência com alteração"


class TestICMBioClassifyType:
    def test_instrucao_normativa_full_name(self):
        assert (
            ICMBioScraper._classify_type("Instrução Normativa Nº 1")
            == "Instrução Normativa"
        )

    def test_instrucao_normativa_in_numeric_prefix(self):
        assert ICMBioScraper._classify_type("IN 42, de 2024") == "Instrução Normativa"

    def test_in_without_digit_not_matched(self):
        assert (
            ICMBioScraper._classify_type("in texto sem número") != "Instrução Normativa"
        )

    def test_portaria(self):
        assert ICMBioScraper._classify_type("Portaria Nº 5") == "Portaria"

    def test_portaria_uppercase(self):
        assert ICMBioScraper._classify_type("PORTARIA Nº 10") == "Portaria"

    def test_outros_atos_resolucao(self):
        assert ICMBioScraper._classify_type("Resolução Nº 3") == "Outros Atos"

    def test_outros_atos_unknown(self):
        assert ICMBioScraper._classify_type("Deliberação 7") == "Outros Atos"

    def test_case_insensitive_instrucao(self):
        assert (
            ICMBioScraper._classify_type("INSTRUÇÃO NORMATIVA 2")
            == "Instrução Normativa"
        )

    def test_in_digit_zero(self):
        assert ICMBioScraper._classify_type("in 0, de 2020") == "Instrução Normativa"

    def test_in_icmbio_prefix_maps_to_instrucao_normativa(self):
        assert (
            ICMBioScraper._classify_type("IN ICMBio 5 de 19/05/2016")
            == "Instrução Normativa"
        )

    def test_in_conjunta_prefix_maps_to_instrucao_normativa(self):
        assert (
            ICMBioScraper._classify_type("IN Conjunta MMA/IBAMA/ICMBio 1 de 12/04/2021")
            == "Instrução Normativa"
        )


class TestICMBioRowToDoc:
    def _make_scraper(self):
        scraper = make_scraper(ICMBioScraper)
        return scraper

    def _valid_row(self, **overrides):
        row = {
            "condicao": "em vigência",
            "publicacao": "01/15/2022",
            "instrumento": "DOU 1",
            "ementa": "Summary text",
            "link_dou": "https://www.in.gov.br/web/dou/-/resolucao-1",
            "ato": "Instrução Normativa Nº 1",
            "assunto": "Fauna",
        }
        row.update(overrides)
        return row

    def test_valid_row_returns_doc(self):
        scraper = self._make_scraper()
        doc = scraper._row_to_doc(self._valid_row())
        assert doc is not None
        assert doc["year"] == 2022
        assert doc["title"] == "Instrução Normativa Nº 1"
        assert doc["situation"] == "em vigência"
        assert doc["type"] == "Instrução Normativa"
        assert doc["document_url"] == "https://www.in.gov.br/web/dou/-/resolucao-1"
        assert doc["summary"] == "Summary text"
        assert doc["subject"] == "Fauna"
        assert doc["publication_info"] == "DOU 1"

    def test_missing_link_dou_returns_none(self):
        scraper = self._make_scraper()
        doc = scraper._row_to_doc(self._valid_row(link_dou=""))
        assert doc is None

    def test_non_in_gov_br_url_returns_none(self):
        scraper = self._make_scraper()
        doc = scraper._row_to_doc(self._valid_row(link_dou="https://example.com/doc"))
        assert doc is None

    def test_multiple_urls_only_first_used(self):
        scraper = self._make_scraper()
        doc = scraper._row_to_doc(
            self._valid_row(
                link_dou="https://www.in.gov.br/first https://www.in.gov.br/second"
            )
        )
        assert doc is not None
        assert doc["document_url"] == "https://www.in.gov.br/first"

    def test_blank_condicao_normalized(self):
        scraper = self._make_scraper()
        doc = scraper._row_to_doc(self._valid_row(condicao=""))
        assert doc is not None
        assert doc["situation"] == "não consta"

    def test_blank_marker_condicao_normalized(self):
        scraper = self._make_scraper()
        for blank in ("(Blank)", "(Em branco)"):
            doc = scraper._row_to_doc(self._valid_row(condicao=blank))
            assert doc is not None
            assert doc["situation"] == "não consta", f"Failed for condicao={blank!r}"

    def test_invalid_date_returns_none(self):
        scraper = self._make_scraper()
        doc = scraper._row_to_doc(self._valid_row(publicacao="not-a-date"))
        assert doc is None

    def test_empty_date_returns_none(self):
        scraper = self._make_scraper()
        doc = scraper._row_to_doc(self._valid_row(publicacao=""))
        assert doc is None

    def test_situation_lowercased_and_stripped(self):
        scraper = self._make_scraper()
        doc = scraper._row_to_doc(self._valid_row(condicao="  Em Vigência  "))
        assert doc is not None
        assert doc["situation"] == "em vigência"

    def test_int_publicacao_does_not_crash(self):
        """Bug 4: if epoch-ms conversion fails, publicacao stays as int.
        _row_to_doc must not raise AttributeError on int.strip()."""
        scraper = self._make_scraper()
        doc = scraper._row_to_doc(self._valid_row(publicacao=1609459200000))
        assert doc is not None or doc is None


class TestICMBioFetchPage:
    def _make_scraper(self, response):
        scraper = make_scraper(ICMBioScraper)
        scraper.http.request = AsyncMock(return_value=response)
        return scraper

    def _mock_response(self, status=200, body=None):
        import json

        from src.core.http import Response

        return Response("url", status, {}, json.dumps(body or {}).encode())

    @pytest.mark.asyncio
    async def test_http_failure_propagates(self):
        from src.core.http import HttpError

        scraper = self._make_scraper(None)
        scraper.http.request.side_effect = HttpError("url", 503, "unavailable")
        with pytest.raises(HttpError):
            await scraper._fetch_page()

    @pytest.mark.asyncio
    async def test_ic_true_means_complete(self):
        body = {
            "results": [
                {
                    "result": {
                        "data": {"dsr": {"DS": [{"IC": True, "PH": [{"DM0": []}]}]}}
                    }
                }
            ]
        }
        scraper = self._make_scraper(self._mock_response(200, body))
        ds, tokens, complete = await scraper._fetch_page()
        assert ds is not None
        assert complete is True
        assert tokens is None

    @pytest.mark.asyncio
    async def test_ic_false_with_rt_returns_tokens(self):
        restart_tokens = [["token_x"]]
        body = {
            "results": [
                {
                    "result": {
                        "data": {
                            "dsr": {
                                "DS": [
                                    {
                                        "IC": False,
                                        "RT": restart_tokens,
                                        "PH": [{"DM0": []}],
                                    }
                                ]
                            }
                        }
                    }
                }
            ]
        }
        scraper = self._make_scraper(self._mock_response(200, body))
        ds, tokens, complete = await scraper._fetch_page()
        assert complete is False
        assert tokens == restart_tokens

    @pytest.mark.asyncio
    async def test_invalid_json_raises(self):
        from src.core.http import Response

        scraper = self._make_scraper(
            Response("url", 200, {}, b"<html>Server error</html>")
        )
        with pytest.raises(ValueError):
            await scraper._fetch_page()

    @pytest.mark.asyncio
    async def test_invalid_response_structure_raises(self):
        scraper = self._make_scraper(
            self._mock_response(200, {"results": [{"result": {"data": None}}]})
        )
        with pytest.raises(TypeError):
            await scraper._fetch_page()

    @pytest.mark.asyncio
    async def test_missing_ic_defaults_to_complete(self):
        body = {
            "results": [{"result": {"data": {"dsr": {"DS": [{"PH": [{"DM0": []}]}]}}}}]
        }
        scraper = self._make_scraper(self._mock_response(200, body))
        ds, tokens, complete = await scraper._fetch_page()
        assert complete is True


def _ref(url: str) -> dict:
    return {
        "year": 2018,
        "title": "Portaria 1039 de 29/11/2018",
        "type": "Portaria",
        "situation": "em vigência",
        "document_url": url,
    }


_ARTICLE = (
    '<html><body><div class="texto-dou"><p class="identifica">PORTARIA Nº 1.039</p>'
    '<p class="ementa">Dispõe sobre algo.</p><p>Art. 1º Fica criado o conselho '
    "consultivo da unidade de conservação federal.</p></div></body></html>"
)


def _pdf() -> bytes:
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    page.insert_textbox(
        fitz.Rect(72, 72, 520, 770),
        "Art. 1º Fica aprovado o plano de manejo da unidade de conservação. " * 20,
        fontsize=12,
    )
    content = doc.tobytes()
    doc.close()
    return content


class TestICMBioFetch:
    async def test_dou_article_over_http_stores_html(self):
        scraper = make_scraper(ICMBioScraper)
        url = "https://www.in.gov.br/web/dou/-/portaria-n-1-039-de-29-de-novembro-de-2018-1"
        scraper.http.request.side_effect = [make_mock_response(_ARTICLE)]
        doc = await scraper.fetch(_ref(url))
        assert doc.raw == _ARTICLE.encode()
        assert doc.raw_ext == ".html"
        assert "conselho consultivo" in doc.text_markdown
        assert "PORTARIA Nº 1.039" not in doc.text_markdown
        scraper.browser.snapshot.assert_not_called()

    async def test_legacy_asset_publisher_link_uses_article_slug(self):
        """Azion blocks /materia/-/asset_publisher/ with 403; the same slug works under /web/dou/-/."""
        scraper = make_scraper(ICMBioScraper)
        scraper.http.request.side_effect = [make_mock_response(_ARTICLE)]
        doc = await scraper.fetch(
            _ref(
                "https://www.in.gov.br/web/guest/materia/-/asset_publisher/Kujrw0TZC2Mb/content/id/47099695/do1-2018-10-25-portaria-conjunta-n-407-de-19-de-outubro-de-2018-47099425"
            )
        )
        expected = "https://www.in.gov.br/web/dou/-/portaria-conjunta-n-407-de-19-de-outubro-de-2018-47099425"
        assert scraper.http.request.call_args.args[0] == expected
        assert doc.document_url == expected

    async def test_old_page_viewer_follows_pdf_frame(self):
        scraper = make_scraper(ICMBioScraper)
        frameset = (
            '<html><frameset><frame name="controlador" src="../visualiza/nav.jsp"/>'
            '<frame name="visualizador" src="../../servlet/INPDFViewer?jornal=515&amp;'
            'pagina=228&amp;data=30/11/2018&amp;captchafield=firstAccess"/></frameset></html>'
        )
        pdf = _pdf()
        scraper.http.request.side_effect = [
            make_mock_response(frameset),
            make_mock_response(pdf, content_type="application/pdf"),
        ]
        doc = await scraper.fetch(
            _ref(
                "https://pesquisa.in.gov.br/imprensa/jsp/visualiza/index.jsp?data=30/11/2018&jornal=515&pagina=228"
            )
        )
        pdf_url = "https://pesquisa.in.gov.br/imprensa/servlet/INPDFViewer?jornal=515&pagina=228&data=30/11/2018&captchafield=firstAccess"
        assert scraper.http.request.call_args.args[0] == pdf_url
        assert doc.document_url == pdf_url
        assert (doc.raw, doc.raw_ext) == (pdf, ".pdf")
        assert "plano de manejo" in doc.text_markdown

    async def test_pdf_viewer_returning_html_raises(self):
        scraper = make_scraper(ICMBioScraper)
        scraper.http.request.side_effect = [
            make_mock_response(
                "<html><body>Digite o captcha para continuar o acesso ao documento solicitado.</body></html>"
            )
        ]
        with pytest.raises(ValueError, match="PDF"):
            await scraper.fetch(
                _ref(
                    "https://pesquisa.in.gov.br/imprensa/servlet/INPDFViewer?jornal=515&pagina=202&data=20/12/2018&captchafield=firstAccess"
                )
            )

    async def test_page_without_dou_text_raises(self):
        scraper = make_scraper(ICMBioScraper)
        scraper.http.request.side_effect = [
            make_mock_response("<html><body>Página</body></html>")
        ]
        with pytest.raises(ValueError, match="texto-dou"):
            await scraper.fetch(_ref("https://www.in.gov.br/web/dou/-/x-1"))


INTEGRATION_YEAR = 2026
EXPECTED_MIN_DOCS_2026 = 30
