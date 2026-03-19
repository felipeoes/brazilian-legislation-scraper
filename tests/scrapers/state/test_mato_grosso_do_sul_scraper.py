"""Tests for MSAlemsScraper (Mato Grosso do Sul).

Covers:
- TYPES constant: 12 types present, string IDs
- SITUATIONS module-level dict preserved for downstream consumers
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set on class
- _view_entries_url: correct URL construction with and without Expand
- _doc_url: correct UNID-based document URL
- _entry_text: static helper extracts text/number from XML entries and field fallbacks
- _get_type_year_docs: failed request, no year found, cache usage, summary/year fallbacks
- _get_doc_data: resume skip, failed fetch, missing body, invalid markdown, valid doc,
  revogada detection from notes, charset-aware decoding, html_link removed from result
- _scrape_year: no docs for year returns [], correct grouping by type,
  _is_already_scraped filter applied

Run with:
    .venv/bin/pytest tests/test_mato_grosso_do_sul_scraper.py -v
"""

from __future__ import annotations

import asyncio
import xml.etree.ElementTree as ET
from unittest.mock import AsyncMock, MagicMock

import pytest
from base_tests import ScraperClassTests, SituationsConstantTests, TypesConstantTests
from conftest import make_base_scraper, make_failed_request

from src.scraper.base.scraper import DEFAULT_INVALID_SITUATION
from src.scraper.state_legislation.mato_grosso_do_sul import (
    SITUATIONS,
    TYPES,
    MSAlemsScraper,
)

# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------

_NSF_BASE = "/appls/legislacao/secoge/govato.nsf"


def _make_scraper(**kwargs) -> MSAlemsScraper:
    """Instantiate MSAlemsScraper bypassing __init__ (no network, no I/O)."""
    defaults = dict(
        _nsf=f"https://aacpdappls.net.ms.gov.br{_NSF_BASE}",
        _type_year_index={},
    )
    defaults.update(kwargs)
    return make_base_scraper(
        MSAlemsScraper,
        "https://aacpdappls.net.ms.gov.br",
        "MATO_GROSSO_DO_SUL",
        TYPES,
        situations={
            "Não consta": "Não consta",
            DEFAULT_INVALID_SITUATION: DEFAULT_INVALID_SITUATION,
        },
        **defaults,
    )


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------


def _make_category_xml(
    categories: list[tuple[int, int, int]],
    *,
    year_field_name: str = "wano",
) -> str:
    """Build ReadViewEntries XML with year categories.

    *categories* is a list of (year, position, descendants).
    """
    entries = ""
    for year, pos, desc in categories:
        entries += f"""<viewentry position="{pos}" noteid="ABCD{pos:04X}" children="{desc}" descendants="{desc}" siblings="{len(categories)}">
  <entrydata columnnumber="0" name="{year_field_name}" category="true"><number>{year}</number></entrydata>
  <entrydata columnnumber="4" name="wtotal"><number>{desc}</number></entrydata>
</viewentry>"""
    return f'<?xml version="1.0" encoding="UTF-8"?>\n<viewentries toplevelentries="{len(categories)}">\n{entries}\n</viewentries>'


def _make_docs_xml(
    expand_pos: int,
    docs: list[dict],
    *,
    summary_field_name: str = "wementa",
) -> str:
    """Build ReadViewEntries XML with document entries inside a category.

    *docs* is a list of {"unid": ..., "title": ..., "summary": ...}.
    """
    cat_entry = f'<viewentry position="{expand_pos}" noteid="ABCD{expand_pos:04X}" children="{len(docs)}" descendants="{len(docs)}">\n  <entrydata columnnumber="0" name="wano" category="true"><number>2025</number></entrydata>\n</viewentry>\n'
    doc_entries = ""
    for i, doc in enumerate(docs, 1):
        doc_entries += f"""<viewentry position="{expand_pos}.{i}" unid="{doc["unid"]}" noteid="50{i:04X}" siblings="{len(docs)}">
  <entrydata columnnumber="2" name="wnumero"><text>{doc["title"]}</text></entrydata>
  <entrydata columnnumber="3" name="{summary_field_name}"><text>{doc.get("summary", "")}</text></entrydata>
</viewentry>
"""
    return f'<?xml version="1.0" encoding="UTF-8"?>\n<viewentries toplevelentries="{len(docs)}">\n{cat_entry}{doc_entries}</viewentries>'


def _make_fetch_bytes_result(
    html: str,
    *,
    charset: str = "utf-8",
) -> tuple[bytes, MagicMock]:
    body = html.encode(charset)
    response = MagicMock()
    response.charset = charset
    return body, response


def _make_xml_response(xml_content: str) -> MagicMock:
    response = MagicMock()
    response.__bool__ = lambda s: True
    response.text = AsyncMock(return_value=xml_content)
    return response


def _mock_fetch_bytes(
    scraper: MSAlemsScraper,
    html: str,
    *,
    charset: str = "utf-8",
) -> bytes:
    body, response = _make_fetch_bytes_result(html, charset=charset)
    scraper.request_service.fetch_bytes = AsyncMock(return_value=(body, response))
    return body


def _make_doc_html(
    *,
    title: str = "LEI Nº 1/2025",
    summary: str = "Resumo da norma.",
    notes_html: str = "<b><font face='Tahoma'>Publicada no Diário Oficial.</font></b><br>",
    body_html: str = "<font face='Tahoma'>Art. 1.</font><br><font face='Tahoma'>Art. 2.</font>",
) -> str:
    return f"""
<html><body>
<form action=''>
<table border="1"><tr><td>nav actions</td></tr></table>
<hr>
<ul><ul>
<table border="0" cellspacing="0" cellpadding="0">
<tr valign="top"><td width="559"><b><font face="Tahoma">{title}</font></b>
<table border="0" cellspacing="0" cellpadding="0">
<tr valign="top"><td width="276"><img src="/icons/ecblank.gif" border="0" height="1" width="1" alt=""></td><td width="282"><i><font face="Tahoma">{summary}</font></i></td></tr>
</table>
<font face="Tahoma">Notas:</font><br>
{notes_html}
</td></tr></table>
</ul></ul>
<table border="0" cellspacing="0" cellpadding="0">
<tr valign="top"><td width="562">{body_html}</td></tr>
</table>
</form>
</body></html>
""".strip()


# ---------------------------------------------------------------------------
# TYPES and SITUATIONS constants
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 12
    REQUIRED_KEYS = {"Emenda Constitucional", "Lei Estadual", "Decreto E Conjunto"}
    REQUIRE_INT_VALUES = False


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_nao_consta_key_present(self):
        assert "Não consta" in SITUATIONS

    def test_revogada_key_present(self):
        assert DEFAULT_INVALID_SITUATION in SITUATIONS


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = MSAlemsScraper
    STATE_NAME = "Mato Grosso do Sul"


# ---------------------------------------------------------------------------
# _view_entries_url
# ---------------------------------------------------------------------------


class TestViewEntriesUrl:
    def test_basic_url_structure(self):
        scraper = _make_scraper()
        url = scraper._view_entries_url("/Decreto")
        assert url.startswith("https://aacpdappls.net.ms.gov.br")
        assert "/Decreto?ReadViewEntries" in url

    def test_default_start_and_count(self):
        scraper = _make_scraper()
        url = scraper._view_entries_url("/Lei%20Estadual")
        assert "Start=1" in url
        assert "Count=200" in url

    def test_custom_start_and_count(self):
        scraper = _make_scraper()
        url = scraper._view_entries_url("/Decreto", start=5, count=100)
        assert "Start=5" in url
        assert "Count=100" in url

    def test_expand_included_when_provided(self):
        scraper = _make_scraper()
        url = scraper._view_entries_url("/Decreto", expand=3)
        assert "Expand=3" in url

    def test_expand_absent_when_not_provided(self):
        scraper = _make_scraper()
        url = scraper._view_entries_url("/Decreto")
        assert "Expand" not in url


# ---------------------------------------------------------------------------
# _doc_url
# ---------------------------------------------------------------------------


class TestDocUrl:
    def test_uses_zero_prefix(self):
        scraper = _make_scraper()
        url = scraper._doc_url("ABCDEF1234567890ABCDEF1234567890")
        assert "/0/ABCDEF1234567890ABCDEF1234567890?OpenDocument" in url

    def test_includes_base_url(self):
        scraper = _make_scraper()
        url = scraper._doc_url("ABCD")
        assert url.startswith("https://aacpdappls.net.ms.gov.br")


# ---------------------------------------------------------------------------
# _entry_text static helper
# ---------------------------------------------------------------------------


class TestEntryText:
    def _make_entry(self, col_name: str, value: str, tag: str = "text") -> ET.Element:
        entry = ET.Element("viewentry")
        data = ET.SubElement(entry, "entrydata", name=col_name)
        el = ET.SubElement(data, tag)
        el.text = value
        return entry

    def test_returns_text_value(self):
        entry = self._make_entry("wnumero", "LEI Nº 1/2025")
        assert MSAlemsScraper._entry_text(entry, "wnumero") == "LEI Nº 1/2025"

    def test_returns_number_value(self):
        entry = self._make_entry("wico1", "42", tag="number")
        assert MSAlemsScraper._entry_text(entry, "wico1") == "42"

    def test_missing_col_returns_empty(self):
        entry = self._make_entry("wnumero", "some value")
        assert MSAlemsScraper._entry_text(entry, "nonexistent") == ""

    def test_strips_whitespace(self):
        entry = self._make_entry("wnumero", "  Lei 1  ")
        assert MSAlemsScraper._entry_text(entry, "wnumero") == "Lei 1"

    def test_tuple_of_field_names_uses_first_nonempty_match(self):
        entry = ET.Element("viewentry")
        ET.SubElement(
            ET.SubElement(entry, "entrydata", name="wementa"), "text"
        ).text = ""
        ET.SubElement(
            ET.SubElement(entry, "entrydata", name="$246"), "text"
        ).text = "Resumo alternativo"
        assert MSAlemsScraper._entry_text(entry, ("wementa", "$246")) == (
            "Resumo alternativo"
        )


# ---------------------------------------------------------------------------
# _get_type_year_docs
# ---------------------------------------------------------------------------


class TestGetTypeYearDocs:
    @pytest.mark.asyncio
    async def test_failed_categories_request_returns_empty(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.make_request = AsyncMock(return_value=failed)
        result = await scraper._get_type_year_docs(
            "Lei Estadual", "/Lei%20Estadual", 2025
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_year_not_in_categories_returns_empty(self):
        scraper = _make_scraper()
        cat_xml = _make_category_xml([(2024, 1, 50), (2023, 2, 30)])
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_xml_response(cat_xml)
        )
        result = await scraper._get_type_year_docs(
            "Lei Estadual", "/Lei%20Estadual", 2025
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_year_with_zero_descendants_returns_empty(self):
        scraper = _make_scraper()
        cat_xml = _make_category_xml([(2025, 1, 0)])
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_xml_response(cat_xml)
        )
        result = await scraper._get_type_year_docs(
            "Lei Estadual", "/Lei%20Estadual", 2025
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_correct_doc_list(self):
        scraper = _make_scraper()
        cat_xml = _make_category_xml([(2026, 1, 5), (2025, 2, 2)])
        docs_xml = _make_docs_xml(
            expand_pos=2,
            docs=[
                {
                    "unid": "AABBCCDDEEFF00112233445566778899",
                    "title": "LEI Nº 1/2025",
                    "summary": "Ementa 1",
                },
                {
                    "unid": "BBCCDDEE11223344AABB556677889900",
                    "title": "LEI Nº 2/2025",
                    "summary": "Ementa 2",
                },
            ],
        )

        scraper.request_service.make_request = AsyncMock(
            side_effect=[_make_xml_response(cat_xml), _make_xml_response(docs_xml)]
        )

        result = await scraper._get_type_year_docs(
            "Lei Estadual", "/Lei%20Estadual", 2025
        )
        assert len(result) == 2
        assert result[0]["title"] == "LEI Nº 1/2025"
        assert result[1]["title"] == "LEI Nº 2/2025"
        assert "OpenDocument" in result[0]["html_link"]
        assert "AABBCCDDEEFF00112233445566778899" in result[0]["html_link"]

    @pytest.mark.asyncio
    async def test_uses_expand_in_docs_request(self):
        scraper = _make_scraper()
        cat_xml = _make_category_xml([(2025, 3, 10)])  # position=3 for 2025
        docs_xml = _make_docs_xml(
            expand_pos=3,
            docs=[{"unid": "AABB" * 8, "title": "LEI Nº 1/2025"}],
        )

        requests_made: list[str] = []

        async def mock_make_request(
            url: str,
            method: str = "GET",
            json: dict | None = None,
            payload: list | dict | None = None,
            timeout: int = 120,
            **kwargs,
        ):
            requests_made.append(url)
            xml_content = cat_xml if len(requests_made) == 1 else docs_xml
            return _make_xml_response(xml_content)

        scraper.request_service.make_request = mock_make_request

        await scraper._get_type_year_docs("Lei Estadual", "/Lei%20Estadual", 2025)
        assert len(requests_made) == 2
        assert "Expand=3" in requests_made[1]
        assert "Start=3" in requests_made[1]

    @pytest.mark.asyncio
    async def test_uses_cached_year_index_for_docs_request(self):
        scraper = _make_scraper(
            _type_year_index={"Lei Estadual": {2025: ("3", 1)}},
        )
        docs_xml = _make_docs_xml(
            expand_pos=3,
            docs=[{"unid": "AABB" * 8, "title": "LEI Nº 1/2025"}],
        )
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_xml_response(docs_xml)
        )

        await scraper._get_type_year_docs("Lei Estadual", "/Lei%20Estadual", 2025)

        scraper.request_service.make_request.assert_called_once()
        requested_url = scraper.request_service.make_request.call_args.args[0]
        assert "Expand=3" in requested_url
        assert "Start=3" in requested_url

    @pytest.mark.asyncio
    async def test_before_scrape_populates_year_index_cache(self):
        scraper = _make_scraper()
        cat_xml = _make_category_xml([(2025, 1, 2)])
        scraper.request_service.make_request = AsyncMock(
            return_value=_make_xml_response(cat_xml)
        )

        async def fake_gather(tasks, context=None, desc=""):
            return await asyncio.gather(*tasks)

        scraper._gather_results = fake_gather

        await scraper._before_scrape()

        assert scraper._type_year_index["Lei Estadual"][2025] == ("1", 2)

    @pytest.mark.asyncio
    async def test_year_field_fallback_supports_decreto_lei(self):
        scraper = _make_scraper()
        cat_xml = _make_category_xml([(1979, 1, 1)], year_field_name="$246")
        docs_xml = _make_docs_xml(
            expand_pos=1,
            docs=[{"unid": "AABB" * 8, "title": "DECRETO-LEI Nº 3"}],
        )
        scraper.request_service.make_request = AsyncMock(
            side_effect=[_make_xml_response(cat_xml), _make_xml_response(docs_xml)]
        )

        result = await scraper._get_type_year_docs("Decreto-Lei", "/Decreto-Lei", 1979)

        assert len(result) == 1
        assert result[0]["title"] == "DECRETO-LEI Nº 3"

    @pytest.mark.asyncio
    async def test_summary_field_fallback_supports_alternative_columns(self):
        scraper = _make_scraper()
        cat_xml = _make_category_xml([(2025, 1, 1)])
        docs_xml = _make_docs_xml(
            expand_pos=1,
            docs=[
                {
                    "unid": "AABB" * 8,
                    "title": "MENSAGEM 1",
                    "summary": "Veto Total",
                }
            ],
            summary_field_name="Ato_Ementa",
        )
        scraper.request_service.make_request = AsyncMock(
            side_effect=[_make_xml_response(cat_xml), _make_xml_response(docs_xml)]
        )

        result = await scraper._get_type_year_docs(
            "Mensagem Vetada", "/Mensagem%20Veto", 2025
        )

        assert result[0]["summary"] == "Veto Total"


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_resume_skip_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=True)
        scraper.request_service.fetch_bytes = AsyncMock()

        result = await scraper._get_doc_data(
            {"title": "Lei 1", "html_link": "/lei1.nsf/view"}
        )

        assert result is None
        scraper.request_service.fetch_bytes.assert_not_called()

    @pytest.mark.asyncio
    async def test_failed_fetch_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper.request_service.fetch_bytes = AsyncMock(
            return_value=make_failed_request()
        )
        scraper._save_doc_error = AsyncMock()
        doc = {"title": "Lei 1", "html_link": "/lei1.nsf/view"}
        result = await scraper._get_doc_data(doc)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_failed_soup_error_includes_norm_type(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper.request_service.fetch_bytes = AsyncMock(
            return_value=make_failed_request()
        )
        scraper._save_doc_error = AsyncMock()
        doc = {"title": "Lei 1", "html_link": "/lei1.nsf/view", "type": "Lei Estadual"}
        await scraper._get_doc_data(doc)
        call_kwargs = scraper._save_doc_error.call_args.kwargs
        assert call_kwargs.get("norm_type") == "Lei Estadual"

    @pytest.mark.asyncio
    async def test_missing_body_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        _mock_fetch_bytes(scraper, "<html></html>")
        scraper._save_doc_error = AsyncMock()
        doc = {"title": "Lei 1", "html_link": "/lei1.nsf/view"}
        result = await scraper._get_doc_data(doc)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_missing_body_error_includes_norm_type(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        _mock_fetch_bytes(scraper, "<html></html>")
        scraper._save_doc_error = AsyncMock()
        doc = {
            "title": "Lei 1",
            "html_link": "/lei1.nsf/view",
            "type": "Decreto",
        }
        await scraper._get_doc_data(doc)
        call_kwargs = scraper._save_doc_error.call_args.kwargs
        assert call_kwargs.get("norm_type") == "Decreto"

    @pytest.mark.asyncio
    async def test_invalid_markdown_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        _mock_fetch_bytes(
            scraper, _make_doc_html(body_html="<font face='Tahoma'>Short</font>")
        )
        scraper._get_markdown = AsyncMock(return_value="short")
        scraper._save_doc_error = AsyncMock()
        doc = {"title": "Lei 1", "html_link": "/lei1.nsf/view"}
        result = await scraper._get_doc_data(doc)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_valid_doc_returns_correct_shape(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        raw_body = _mock_fetch_bytes(
            scraper,
            _make_doc_html(
                body_html=(
                    "<font face='Tahoma'>Artigo 1.</font><br>"
                    "<font face='Tahoma'>Artigo 2.</font>"
                )
            ),
        )
        valid_md = "# Lei Estadual\n\n" + "Texto legislativo. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = {
            "title": "Lei 1",
            "html_link": "/lei1.nsf/view",
            "year": 2025,
            "type": "Lei Estadual",
            "situation": "Não consta",
        }
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert "# Lei Estadual" in result["text_markdown"]
        assert "document_url" in result
        assert result["_raw_content"] == raw_body
        assert result["_content_extension"] == ".html"

    @pytest.mark.asyncio
    async def test_body_content_used_for_markdown(self):
        """_get_doc_data uses body content and strips the border=1 action bar."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        _mock_fetch_bytes(
            scraper,
            _make_doc_html(
                body_html=(
                    "<font face='Tahoma'>Art. 1.</font><br>"
                    "<font face='Tahoma'>Art. 2.</font><br>"
                    "<font face='Tahoma'>Art. 3.</font>"
                )
            ),
        )

        captured_html: list[str] = []

        async def fake_get_markdown(
            url: str | None = None,
            response=None,
            stream=None,
            html_content: str | None = None,
            filename: str | None = None,
        ) -> str:
            captured_html.append(html_content or "")
            return "# Lei\n\n" + "Conteúdo. " * 30

        scraper._get_markdown = fake_get_markdown
        doc = {
            "title": "Lei 1",
            "summary": "Resumo da norma.",
            "html_link": "/lei1.nsf/view",
            "year": 2025,
            "type": "Lei Estadual",
            "situation": "Não consta",
        }
        await scraper._get_doc_data(doc)

        assert len(captured_html) == 1
        # Navigation action bar must be removed
        assert "nav actions" not in captured_html[0]
        # Summary is stored separately and must be stripped from markdown input
        assert "Resumo da norma." not in captured_html[0]
        # Notes must remain available for context and revogation detection
        assert "Notas:" in captured_html[0]
        assert "Publicada no Diário Oficial." in captured_html[0]
        # Document content must be present
        assert "Art. 1." in captured_html[0]
        assert "Art. 3." in captured_html[0]

    @pytest.mark.asyncio
    async def test_revogada_note_sets_situation(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        _mock_fetch_bytes(
            scraper,
            _make_doc_html(
                notes_html=(
                    "<a href='/norma'><b><font face='Tahoma'>"
                    "Revogado pela Lei nº 6.542, de 23 de dezembro de 2025"
                    "</font></b></a><b><font face='Tahoma'>.</font></b>"
                )
            ),
        )

        captured_html: list[str] = []

        async def fake_get_markdown(
            url: str | None = None,
            response=None,
            stream=None,
            html_content: str | None = None,
            filename: str | None = None,
        ) -> str:
            captured_html.append(html_content or "")
            return "# Lei\n\n" + "Texto. " * 30

        scraper._get_markdown = fake_get_markdown

        result = await scraper._get_doc_data(
            {
                "title": "Decreto-Lei 3",
                "summary": "Resumo da norma.",
                "html_link": "/lei1.nsf/view",
                "year": 2025,
                "type": "Decreto-Lei",
                "situation": "Não consta",
            }
        )

        assert result is not None
        assert result["situation"] == DEFAULT_INVALID_SITUATION
        assert captured_html
        assert "Resumo da norma." not in captured_html[0]
        assert (
            "Revogado pela Lei nº 6.542, de 23 de dezembro de 2025" in captured_html[0]
        )

    @pytest.mark.asyncio
    async def test_publication_note_does_not_set_revogada(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        _mock_fetch_bytes(scraper, _make_doc_html())
        scraper._get_markdown = AsyncMock(return_value="# Lei\n\n" + "Texto. " * 30)

        result = await scraper._get_doc_data(
            {
                "title": "Lei 1",
                "html_link": "/lei1.nsf/view",
                "year": 2025,
                "type": "Lei Estadual",
                "situation": "Não consta",
            }
        )

        assert result is not None
        assert result["situation"] == "Não consta"

    @pytest.mark.asyncio
    async def test_body_revogacao_text_does_not_set_revogada(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        _mock_fetch_bytes(
            scraper,
            _make_doc_html(
                body_html=(
                    "<font face='Tahoma'>Art. 1.</font><br>"
                    "<font face='Tahoma'>Revogado pela Lei X.</font>"
                )
            ),
        )
        scraper._get_markdown = AsyncMock(return_value="# Lei\n\n" + "Texto. " * 30)

        result = await scraper._get_doc_data(
            {
                "title": "Lei 1",
                "html_link": "/lei1.nsf/view",
                "year": 2025,
                "type": "Lei Estadual",
                "situation": "Não consta",
            }
        )

        assert result is not None
        assert result["situation"] == "Não consta"

    @pytest.mark.asyncio
    async def test_response_charset_used_for_decoding(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        _mock_fetch_bytes(
            scraper,
            _make_doc_html(
                summary="Ação pública.",
                body_html="<font face='Tahoma'>Órgão competente.</font>",
            ),
            charset="iso-8859-1",
        )

        captured_html: list[str] = []

        async def fake_get_markdown(
            url: str | None = None,
            response=None,
            stream=None,
            html_content: str | None = None,
            filename: str | None = None,
        ) -> str:
            captured_html.append(html_content or "")
            return "# Lei\n\n" + "Texto. " * 30

        scraper._get_markdown = fake_get_markdown

        await scraper._get_doc_data(
            {
                "title": "Lei 1",
                "summary": "Ação pública.",
                "html_link": "/lei1.nsf/view",
                "year": 2025,
                "type": "Lei Estadual",
                "situation": "Não consta",
            }
        )

        assert captured_html
        assert "Ação pública." not in captured_html[0]
        assert "Publicada no Diário Oficial." in captured_html[0]
        assert "Órgão competente." in captured_html[0]

    @pytest.mark.asyncio
    async def test_html_link_removed_from_result(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        _mock_fetch_bytes(
            scraper, _make_doc_html(body_html="<font face='Tahoma'>Texto.</font>")
        )
        valid_md = "# Lei\n\n" + "Conteúdo legislativo. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = {
            "title": "Lei 1",
            "html_link": "/lei1.nsf/view",
            "year": 2025,
            "type": "Lei Estadual",
            "situation": "Não consta",
        }
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert "html_link" not in result


# ---------------------------------------------------------------------------
# _scrape_year
# ---------------------------------------------------------------------------


class TestScrapeYear:
    @pytest.mark.asyncio
    async def test_no_docs_for_any_type_returns_empty(self):
        scraper = _make_scraper()
        scraper._get_type_year_docs = AsyncMock(return_value=[])

        async def fake_gather(tasks, context=None, desc=""):
            return await asyncio.gather(*tasks)

        scraper._gather_results = fake_gather
        scraper._is_already_scraped = MagicMock(return_value=False)
        result = await scraper._scrape_year(2025)
        assert result == []

    @pytest.mark.asyncio
    async def test_already_scraped_docs_filtered_out(self):
        scraper = _make_scraper()

        async def fake_get_docs(type_name, type_path, year):
            if type_name == "Lei Estadual":
                return [{"title": "LEI Nº 1", "html_link": "/l1"}]
            return []

        scraper._get_type_year_docs = fake_get_docs

        # Mark everything as already scraped
        scraper._is_already_scraped = MagicMock(return_value=True)

        process_calls: list = []

        async def fake_process(
            documents,
            *,
            year: int,
            norm_type: str,
            situation: str = "",
            desc: str = "",
            doc_data_fn=None,
            doc_data_kwargs: dict | None = None,
        ):
            process_calls.append(documents)
            return []

        async def fake_gather(tasks, context=None, desc=""):
            return await asyncio.gather(*tasks)

        scraper._process_documents = fake_process
        scraper._gather_results = fake_gather

        result = await scraper._scrape_year(2025)
        assert result == []
        assert process_calls == []  # nothing passed to process

    @pytest.mark.asyncio
    async def test_groups_by_type_and_calls_process_documents(self):
        scraper = _make_scraper()

        async def fake_get_docs(type_name, type_path, year):
            if type_name == "Lei Estadual":
                return [
                    {"title": "LEI Nº 1", "html_link": "/l1"},
                ]
            if type_name == "Decreto":
                return [
                    {"title": "DECRETO Nº 1", "html_link": "/d1"},
                    {"title": "DECRETO Nº 2", "html_link": "/d2"},
                ]
            return []

        scraper._get_type_year_docs = fake_get_docs
        scraper._is_already_scraped = MagicMock(return_value=False)

        calls: list[dict] = []

        async def fake_process(
            documents,
            *,
            year: int,
            norm_type: str,
            situation: str = "",
            desc: str = "",
            doc_data_fn=None,
            doc_data_kwargs: dict | None = None,
        ):
            calls.append({"norm_type": norm_type, "count": len(documents)})
            return [{"title": d["title"]} for d in documents]

        async def fake_gather(tasks, context=None, desc=""):
            return await asyncio.gather(*tasks)

        scraper._process_documents = fake_process
        scraper._gather_results = fake_gather

        await scraper._scrape_year(2025)

        types_called = {c["norm_type"] for c in calls}
        assert "Lei Estadual" in types_called
        assert "Decreto" in types_called
        decreto_call = next(c for c in calls if c["norm_type"] == "Decreto")
        assert decreto_call["count"] == 2

    @pytest.mark.asyncio
    async def test_listing_tuple_mapping_not_broken_by_failed_task(self):
        """Tuple-based listing preserves type→docs association even when some tasks fail.

        If a listing task raises an exception, _gather_results filters it out.
        The remaining (type_name, docs) tuples must still map correctly so that
        "Decreto" docs are not attributed to "Constituição Estadual" etc.
        """
        scraper = _make_scraper()

        async def fake_get_docs(type_name, type_path, year):
            if type_name == "Decreto":
                return [{"title": "DECRETO Nº 1", "html_link": "/d1"}]
            raise RuntimeError("simulated listing failure")

        scraper._get_type_year_docs = fake_get_docs
        scraper._is_already_scraped = MagicMock(return_value=False)

        process_calls: list[dict] = []

        async def fake_process(docs, **kwargs):
            process_calls.append({"norm_type": kwargs["norm_type"], "count": len(docs)})
            return [{"title": d["title"]} for d in docs]

        async def fake_gather(tasks, **kw):
            # Replicate _gather_results: collect results, filter exceptions/None
            raw = await asyncio.gather(*tasks, return_exceptions=True)
            return [r for r in raw if not isinstance(r, Exception) and r is not None]

        scraper._process_documents = fake_process
        scraper._gather_results = fake_gather

        await scraper._scrape_year(2025)

        # Only Decreto had docs and no exception
        assert len(process_calls) == 1
        assert process_calls[0]["norm_type"] == "Decreto"
        assert process_calls[0]["count"] == 1
