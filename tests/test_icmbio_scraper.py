"""Tests for ICMBioScraper.

Covers:
- _build_query_payload — first page and pagination
- _parse_dsr_rows — repeat bitmask, ValueDict resolution, epoch-ms dates
- _classify_type — all type categories + tightened IN heuristic
- _row_to_doc — valid and invalid rows, edge cases
- _before_scrape — bucketing, skipped rows
- _get_doc_data — already scraped, HTTP errors, missing div, empty markdown, happy path
- _fetch_page — non-200 response, IC flag, RestartTokens
- _scrape_year — no docs, has docs
- Integration — live PowerBI API + DOU HTML pages (2026)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.scraper.icmbio.scrape import (
    COLUMNS,
    DATASET_ID,
    MODEL_ID,
    PAGE_SIZE,
    REPORT_ID,
    VISUAL_ID,
    ICMBioScraper,
)


# =========================================================================
# _build_query_payload
# =========================================================================


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


# =========================================================================
# _parse_dsr_rows
# =========================================================================


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
                },
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
        # 1609459200000 ms = 2021-01-01 00:00:00 UTC
        ds = self._make_ds([{"C": ["em vigência", 1609459200000, "", "", "", "", ""]}])
        rows = ICMBioScraper._parse_dsr_rows(ds)
        assert rows[0]["publicacao"] == "01/01/2021"

    def test_repeat_bitmask_carries_previous_value(self):
        # Row 0: all values set. Row 1: col 0 (condicao) repeats via R=1.
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
        assert rows[1]["condicao"] == "em vigência"  # repeated
        assert rows[1]["ato"] == "IN 2"

    def test_value_dict_resolution(self):
        # The schema row itself is processed as a data row (with no C → all empty).
        # The actual data row is at index 1.
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
        # rows[0] is the schema row (all empty), rows[1] is the data row
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
            ],
        )
        rows = ICMBioScraper._parse_dsr_rows(ds, accumulated_dicts={"D0": ["revogado"]})
        # rows[0] is the schema row (all empty), rows[1] is the data row
        assert rows[1]["condicao"] == ""

    def test_none_value_becomes_empty_string(self):
        # Provide a row with fewer C values so some columns are None
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
        # accumulated_dicts is empty → falls back to ds["ValueDicts"]
        # rows[0] is the schema row (all empty), rows[1] is the data row
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
        # accumulated contains more entries built across pages
        # rows[0] is the schema row (all empty), rows[1] is the data row
        accumulated = {"D0": ["revogado", "em vigência com alteração"]}
        rows = ICMBioScraper._parse_dsr_rows(ds, accumulated_dicts=accumulated)
        assert rows[1]["condicao"] == "em vigência com alteração"


# =========================================================================
# _classify_type
# =========================================================================


class TestICMBioClassifyType:
    def test_instrucao_normativa_full_name(self):
        assert (
            ICMBioScraper._classify_type("Instrução Normativa Nº 1")
            == "Instrução Normativa"
        )

    def test_instrucao_normativa_in_numeric_prefix(self):
        assert ICMBioScraper._classify_type("IN 42, de 2024") == "Instrução Normativa"

    def test_in_without_digit_not_matched(self):
        # "in texto" starts with "in " but has no digit — tightened heuristic
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
        # "in 0" — numeric after "in " — should match
        assert ICMBioScraper._classify_type("in 0, de 2020") == "Instrução Normativa"


# =========================================================================
# _row_to_doc
# =========================================================================


class TestICMBioRowToDoc:
    def _make_scraper(self):
        scraper = object.__new__(ICMBioScraper)
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


# =========================================================================
# _before_scrape
# =========================================================================


class TestICMBioBeforeScrape:
    def _make_scraper(self, rows):
        scraper = object.__new__(ICMBioScraper)
        scraper.verbose = False
        scraper._fetch_all_rows = AsyncMock(return_value=rows)
        return scraper

    def _valid_row(self, year_str="2022", link="https://www.in.gov.br/doc"):
        return {
            "condicao": "em vigência",
            "publicacao": f"01/01/{year_str}",
            "instrumento": "DOU 1",
            "ementa": "Ementa",
            "link_dou": link,
            "ato": "Instrução Normativa Nº 1",
            "assunto": "Fauna",
        }

    @pytest.mark.asyncio
    async def test_all_valid_rows_bucketed(self):
        rows = [
            self._valid_row("2021"),
            self._valid_row("2022"),
            self._valid_row("2021"),
        ]
        scraper = self._make_scraper(rows)
        await scraper._before_scrape()
        assert 2021 in scraper._docs_by_year
        assert 2022 in scraper._docs_by_year
        assert len(scraper._docs_by_year[2021]) == 2
        assert len(scraper._docs_by_year[2022]) == 1

    @pytest.mark.asyncio
    async def test_invalid_rows_skipped(self):
        rows = [
            self._valid_row("2022"),
            self._valid_row("2022", link=""),  # no link → skipped
            self._valid_row(
                "2022", link="https://example.com/x"
            ),  # wrong domain → skipped
        ]
        scraper = self._make_scraper(rows)
        await scraper._before_scrape()
        assert len(scraper._docs_by_year.get(2022, [])) == 1

    @pytest.mark.asyncio
    async def test_empty_rows_gives_empty_buckets(self):
        scraper = self._make_scraper([])
        await scraper._before_scrape()
        assert scraper._docs_by_year == {}

    @pytest.mark.asyncio
    async def test_mixed_years_correct_bucketing(self):
        rows = [
            self._valid_row("2019"),
            self._valid_row("2020"),
            self._valid_row("2020"),
            self._valid_row("2023"),
        ]
        scraper = self._make_scraper(rows)
        await scraper._before_scrape()
        assert len(scraper._docs_by_year[2019]) == 1
        assert len(scraper._docs_by_year[2020]) == 2
        assert len(scraper._docs_by_year[2023]) == 1


# =========================================================================
# _fetch_page
# =========================================================================


class TestICMBioFetchPage:
    def _make_scraper(self, response):
        scraper = object.__new__(ICMBioScraper)
        scraper.request_service = MagicMock()
        scraper.request_service.make_request = AsyncMock(return_value=response)
        return scraper

    def _mock_response(self, status=200, body=None):
        resp = MagicMock()
        resp.status = status
        resp.__bool__ = lambda self: True

        import json as _json

        async def _text():
            return _json.dumps(body or {})

        resp.text = _text
        return resp

    @pytest.mark.asyncio
    async def test_non_200_returns_none_and_is_complete(self):
        failed = MagicMock()
        failed.__bool__ = lambda self: False
        failed.status = 503
        scraper = self._make_scraper(failed)
        ds, tokens, complete = await scraper._fetch_page()
        assert ds is None
        assert tokens is None
        assert complete is True

    @pytest.mark.asyncio
    async def test_ic_true_means_complete(self):
        body = {
            "results": [
                {
                    "result": {
                        "data": {
                            "dsr": {
                                "DS": [
                                    {
                                        "IC": True,
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
    async def test_missing_ic_defaults_to_complete(self):
        body = {
            "results": [
                {
                    "result": {
                        "data": {
                            "dsr": {
                                "DS": [
                                    {
                                        "PH": [{"DM0": []}],
                                        # no "IC" key
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
        assert complete is True


# =========================================================================
# _get_doc_data
# =========================================================================


class TestICMBioGetDocData:
    def _make_scraper(self):
        scraper = object.__new__(ICMBioScraper)
        scraper.request_service = MagicMock()
        scraper._scraped_keys = set()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_result = AsyncMock(return_value=None)
        scraper._save_doc_error = AsyncMock(return_value=None)
        scraper._html_to_markdown = AsyncMock(
            return_value="# Converted markdown content"
        )
        scraper._clean_norm_soup = MagicMock(side_effect=lambda soup, **kwargs: soup)
        scraper._wrap_html = MagicMock(
            side_effect=lambda html: f"<html><body>{html}</body></html>"
        )
        return scraper

    def _valid_doc_info(self):
        return {
            "year": 2022,
            "title": "IN Nº 1",
            "summary": "Ementa",
            "type": "Instrução Normativa",
            "document_url": "https://www.in.gov.br/web/dou/-/in-1",
            "situation": "em vigência",
            "subject": "Fauna",
            "publication_info": "DOU 1",
        }

    def _make_response(self, status=200, html=""):
        resp = MagicMock()
        resp.status = status
        resp.__bool__ = lambda self: True

        async def _text():
            return html

        resp.text = _text
        return resp

    @pytest.mark.asyncio
    async def test_already_scraped_returns_none(self):
        scraper = self._make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=True)
        result = await scraper._get_doc_data(self._valid_doc_info())
        assert result is None
        scraper.request_service.make_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_document_url_saves_error_and_returns_none(self):
        scraper = self._make_scraper()
        doc = self._valid_doc_info()
        doc["document_url"] = ""
        result = await scraper._get_doc_data(doc)
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_http_error_saves_error_and_returns_none(self):
        scraper = self._make_scraper()
        failed = MagicMock()
        failed.__bool__ = lambda self: False
        failed.status = 503
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        result = await scraper._get_doc_data(self._valid_doc_info())
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_div_texto_dou_not_found_saves_error(self):
        scraper = self._make_scraper()
        scraper.request_service.make_request = AsyncMock(
            return_value=self._make_response(
                200, "<html><body><p>No div here</p></body></html>"
            )
        )
        result = await scraper._get_doc_data(self._valid_doc_info())
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_empty_markdown_saves_error(self):
        scraper = self._make_scraper()
        scraper.request_service.make_request = AsyncMock(
            return_value=self._make_response(
                200,
                '<html><body><div class="texto-dou"><p>content</p></div></body></html>',
            )
        )
        scraper._html_to_markdown = AsyncMock(return_value="   ")
        result = await scraper._get_doc_data(self._valid_doc_info())
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_not_found_message_in_markdown_saves_error(self):
        scraper = self._make_scraper()
        scraper.request_service.make_request = AsyncMock(
            return_value=self._make_response(
                200,
                '<html><body><div class="texto-dou"><p>content</p></div></body></html>',
            )
        )
        scraper._html_to_markdown = AsyncMock(
            return_value="The requested URL was not found on this server."
        )
        result = await scraper._get_doc_data(self._valid_doc_info())
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_happy_path_saves_and_returns_result(self):
        scraper = self._make_scraper()
        scraper.request_service.make_request = AsyncMock(
            return_value=self._make_response(
                200,
                '<html><body><div class="texto-dou"><p>Art. 1º Esta instrução normativa...</p></div></body></html>',
            )
        )
        saved_result = {}

        async def capture_save(result):
            saved_result.update(result)

        scraper._save_doc_result = capture_save

        result = await scraper._get_doc_data(self._valid_doc_info())
        assert result is not None
        assert "text_markdown" in result
        assert result["text_markdown"] == "# Converted markdown content"
        assert result["year"] == 2022
        assert result["title"] == "IN Nº 1"
        assert "_raw_content" in result
        assert result["_content_extension"] == ".html"

    @pytest.mark.asyncio
    async def test_happy_path_clean_norm_soup_called(self):
        scraper = self._make_scraper()
        scraper.request_service.make_request = AsyncMock(
            return_value=self._make_response(
                200,
                '<html><body><div class="texto-dou"><p>Content here.</p></div></body></html>',
            )
        )
        await scraper._get_doc_data(self._valid_doc_info())
        scraper._clean_norm_soup.assert_called_once()

    @pytest.mark.asyncio
    async def test_identifica_and_ementa_stripped_before_conversion(self):
        """<p class="identifica"> and <p class="ementa"> must be removed before
        markdown conversion so the title and summary are not duplicated in text_markdown."""
        from bs4 import BeautifulSoup as BS

        scraper = self._make_scraper()

        html_sent_to_markdown = []

        async def capture_html(html_content):
            html_sent_to_markdown.append(html_content)
            return "# Body content only"

        scraper._html_to_markdown = capture_html
        scraper.request_service.make_request = AsyncMock(
            return_value=self._make_response(
                200,
                '<html><body><div class="texto-dou">'
                '<p class="identifica">INSTRUÇÃO NORMATIVA ICMBio Nº 14</p>'
                '<p class="ementa">Altera a Instrução Normativa nº 16...</p>'
                '<p class="dou-paragraph">Art. 1º Esta instrução normativa...</p>'
                "</div></body></html>",
            )
        )

        result = await scraper._get_doc_data(self._valid_doc_info())
        assert result is not None
        assert len(html_sent_to_markdown) == 1
        converted_html = html_sent_to_markdown[0]
        soup = BS(converted_html, "html.parser")
        # identifica and ementa must be absent
        assert soup.find("p", class_="identifica") is None
        assert soup.find("p", class_="ementa") is None
        # body paragraph must be present
        assert soup.find("p", class_="dou-paragraph") is not None


# =========================================================================
# _scrape_year
# =========================================================================


class TestICMBioScrapeYear:
    def _make_scraper(self, docs_by_year=None):
        scraper = object.__new__(ICMBioScraper)
        scraper._docs_by_year = docs_by_year or {}
        scraper.verbose = False
        scraper._get_doc_data = AsyncMock(return_value=None)
        scraper._gather_results = AsyncMock(return_value=[])
        return scraper

    @pytest.mark.asyncio
    async def test_no_docs_returns_empty_list(self):
        scraper = self._make_scraper({})
        result = await scraper._scrape_year(2022)
        assert result == []
        scraper._gather_results.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_docs_present_calls_gather_results(self):
        doc = {
            "year": 2022,
            "title": "IN 1",
            "document_url": "https://www.in.gov.br/a",
            "situation": "em vigência",
            "type": "Instrução Normativa",
        }
        scraper = self._make_scraper({2022: [doc]})
        fake_result = {**doc, "text_markdown": "# content"}
        scraper._gather_results = AsyncMock(return_value=[fake_result])

        result = await scraper._scrape_year(2022)

        scraper._gather_results.assert_awaited_once()
        call_kwargs = scraper._gather_results.call_args
        # Verify min_length=0 was passed
        assert call_kwargs.kwargs.get("min_length") == 0 or (
            len(call_kwargs.args) >= 4 and call_kwargs.args[3] == 0
        )
        assert len(result) == 1
        assert result[0]["text_markdown"] == "# content"

    @pytest.mark.asyncio
    async def test_none_results_filtered_out(self):
        docs = [
            {
                "year": 2022,
                "title": "IN 1",
                "document_url": "https://www.in.gov.br/a",
                "situation": "em vigência",
                "type": "Instrução Normativa",
            },
            {
                "year": 2022,
                "title": "IN 2",
                "document_url": "https://www.in.gov.br/b",
                "situation": "em vigência",
                "type": "Instrução Normativa",
            },
        ]
        scraper = self._make_scraper({2022: docs})
        # gather_results returns one valid and one None
        scraper._gather_results = AsyncMock(
            return_value=[
                {**docs[0], "text_markdown": "content"},
                None,
            ]
        )
        result = await scraper._scrape_year(2022)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_year_not_in_docs_by_year_returns_empty(self):
        scraper = self._make_scraper(
            {
                2021: [
                    {
                        "year": 2021,
                        "title": "X",
                        "document_url": "https://in.gov.br/x",
                        "situation": "em vigência",
                        "type": "Portaria",
                    }
                ]
            }
        )
        result = await scraper._scrape_year(2099)
        assert result == []


# =========================================================================
# Integration — live PowerBI API + DOU HTML pages
# =========================================================================

# Mirrors the pattern used in test_camara_dep_scraper.py:
#   - Inline tempfile.TemporaryDirectory() — no shared fixtures
#   - Hits live endpoints; mark with @pytest.mark.integration to allow
#     easy selective skipping: uv run pytest -m "not integration"

INTEGRATION_YEAR = 2026
# Known lower-bound for 2026 docs (confirmed via live API run on 2026-03-06).
# Using a conservative floor so the test is not brittle against new docs.
EXPECTED_MIN_DOCS_2026 = 30


@pytest.mark.integration
class TestICMBioIntegration:
    """Live integration tests — hit the real PowerBI API and DOU pages.

    These tests require a live network connection and may take several minutes
    because the PowerBI API returns ~1 000 rows across 3 paginated requests.

    Run with:
        uv run pytest tests/test_icmbio_scraper.py::TestICMBioIntegration -v -s
    Skip with:
        uv run pytest -m "not integration"
    """

    @pytest.mark.asyncio
    async def test_fetch_all_rows_returns_minimum_count(
        self, integration_scraper_factory
    ):
        """_fetch_all_rows must return at least EXPECTED_MIN_DOCS_2026 rows total
        (across all years) from the live PowerBI API."""
        async with integration_scraper_factory(
            ICMBioScraper,
            year_start=INTEGRATION_YEAR,
            year_end=INTEGRATION_YEAR,
            verbose=True,
            rps=5,
        ) as scraper:
            all_rows = await scraper._fetch_all_rows()
            print(f"\nTotal rows from PowerBI API: {len(all_rows)}")
            assert len(all_rows) > 0, "PowerBI API returned no rows"
            # The full dataset always has hundreds of records
            assert len(all_rows) >= 100, (
                f"Expected ≥100 rows from API, got {len(all_rows)}"
            )

    @pytest.mark.asyncio
    async def test_before_scrape_buckets_2026(self, integration_scraper_factory):
        """_before_scrape must populate _docs_by_year[2026] with at least
        EXPECTED_MIN_DOCS_2026 valid documents."""
        async with integration_scraper_factory(
            ICMBioScraper,
            year_start=INTEGRATION_YEAR,
            year_end=INTEGRATION_YEAR,
            verbose=True,
            rps=5,
        ) as scraper:
            await scraper._before_scrape()
            docs_2026 = scraper._docs_by_year.get(INTEGRATION_YEAR, [])
            print(f"\nDocs bucketed for {INTEGRATION_YEAR}: {len(docs_2026)}")
            assert len(docs_2026) >= EXPECTED_MIN_DOCS_2026, (
                f"Expected ≥{EXPECTED_MIN_DOCS_2026} docs for {INTEGRATION_YEAR}, "
                f"got {len(docs_2026)}"
            )

    @pytest.mark.asyncio
    async def test_before_scrape_all_docs_have_in_gov_br_url(
        self, integration_scraper_factory
    ):
        """Every document produced by _before_scrape must have an in.gov.br URL."""
        async with integration_scraper_factory(
            ICMBioScraper,
            year_start=INTEGRATION_YEAR,
            year_end=INTEGRATION_YEAR,
            verbose=True,
            rps=5,
        ) as scraper:
            await scraper._before_scrape()
            all_docs = [doc for docs in scraper._docs_by_year.values() for doc in docs]
            bad = [d for d in all_docs if "in.gov.br" not in d.get("document_url", "")]
            assert bad == [], (
                f"{len(bad)} docs have non-in.gov.br URLs: "
                f"{[d['document_url'] for d in bad[:3]]}"
            )

    @pytest.mark.asyncio
    async def test_scrape_year_2026_returns_results_with_markdown(
        self, integration_scraper_factory
    ):
        """Scraping year 2026 end-to-end must yield documents with non-empty
        text_markdown that does not contain the raw identifica/ementa classes."""
        import json

        async with integration_scraper_factory(
            ICMBioScraper,
            year_start=INTEGRATION_YEAR,
            year_end=INTEGRATION_YEAR,
            verbose=True,
            rps=5,
        ) as scraper:
            # Pre-fetch API data (normally called by the run() orchestrator)
            await scraper._before_scrape()
            results = await scraper._scrape_year(INTEGRATION_YEAR)

            print(f"\n_scrape_year({INTEGRATION_YEAR}) returned {len(results)} results")
            assert len(results) >= 1, (
                f"Expected at least 1 result for {INTEGRATION_YEAR}"
            )

            # Verify text_markdown is populated and non-trivial
            for doc in results:
                assert doc.get("text_markdown"), (
                    f"Empty text_markdown for {doc.get('title', '?')}"
                )

            # Spot-check: identifica/ementa classes must NOT appear in
            # any text_markdown (they would indicate the duplication bug)
            duplicated = [
                doc
                for doc in results
                if 'class="identifica"' in doc.get("text_markdown", "")
                or 'class="ementa"' in doc.get("text_markdown", "")
            ]
            assert duplicated == [], (
                f"{len(duplicated)} docs still contain identifica/ementa markup "
                f"in text_markdown: {[d.get('title') for d in duplicated[:3]]}"
            )

            # Verify the saved shard files exist and contain the documents
            save_dir = scraper.docs_save_dir
            shard_files = list(save_dir.rglob("chunk_*.json"))
            print(f"Shard files written: {len(shard_files)}")
            assert shard_files, "No chunk_*.json shard files written to save dir"

            saved_docs = []
            for sf in shard_files:
                content = json.loads(sf.read_text(encoding="utf-8"))
                docs = (
                    content
                    if isinstance(content, list)
                    else content.get("documents", [])
                )
                saved_docs.extend(docs)

            print(f"Total saved docs on disk: {len(saved_docs)}")
            assert len(saved_docs) >= 1, "No documents persisted to disk"

            # Every saved doc must have year == INTEGRATION_YEAR
            wrong_year = [d for d in saved_docs if d.get("year") != INTEGRATION_YEAR]
            assert wrong_year == [], (
                f"{len(wrong_year)} saved docs have wrong year: "
                f"{[d.get('year') for d in wrong_year[:3]]}"
            )
