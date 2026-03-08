"""Tests for MSAlemsScraper (Mato Grosso do Sul).

Covers:
- TYPES constant: 11 types present, string IDs
- SITUATIONS module-level dict preserved for downstream consumers
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set on class
- _view_entries_url: correct URL construction with and without Expand
- _doc_url: correct UNID-based document URL
- _entry_text: static helper extracts text/number from XML entries
- _get_type_year_docs: failed request, no year found, returns docs with UNID URLs
- _get_doc_data: resume skip, failed soup, missing <p>, invalid markdown, valid doc
  (collects all <p> tags), html_link removed from result
- _scrape_year: no docs for year returns [], correct grouping by type,
  _is_already_scraped filter applied

Run with:
    .venv/bin/pytest tests/test_mato_grosso_do_sul_scraper.py -v
"""

import asyncio
import xml.etree.ElementTree as ET
from unittest.mock import AsyncMock, MagicMock

import pytest
from bs4 import BeautifulSoup

from src.scraper.state_legislation.mato_grosso_do_sul import (
    SITUATIONS,
    TYPES,
    MSAlemsScraper,
)
from base_tests import TypesConstantTests, SituationsConstantTests, ScraperClassTests
from conftest import make_base_scraper, make_failed_request, assert_resume_skips

# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------

_NSF_BASE = "/appls/legislacao/secoge/govato.nsf"


def _make_scraper(**kwargs) -> MSAlemsScraper:
    """Instantiate MSAlemsScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        MSAlemsScraper,
        "https://aacpdappls.net.ms.gov.br",
        "MATO_GROSSO_DO_SUL",
        TYPES,
        situations={"Não consta": "Não consta"},
        _nsf=f"https://aacpdappls.net.ms.gov.br{_NSF_BASE}",
        **kwargs,
    )


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------


def _make_category_xml(categories: list[tuple[int, int, int]]) -> str:
    """Build ReadViewEntries XML with year categories.

    *categories* is a list of (year, position, descendants).
    """
    entries = ""
    for year, pos, desc in categories:
        entries += f"""<viewentry position="{pos}" noteid="ABCD{pos:04X}" children="{desc}" descendants="{desc}" siblings="{len(categories)}">
  <entrydata columnnumber="0" name="wano" category="true"><number>{year}</number></entrydata>
  <entrydata columnnumber="4" name="wtotal"><number>{desc}</number></entrydata>
</viewentry>"""
    return f'<?xml version="1.0" encoding="UTF-8"?>\n<viewentries toplevelentries="{len(categories)}">\n{entries}\n</viewentries>'


def _make_docs_xml(expand_pos: int, docs: list[dict]) -> str:
    """Build ReadViewEntries XML with document entries inside a category.

    *docs* is a list of {"unid": ..., "title": ..., "summary": ...}.
    """
    cat_entry = f'<viewentry position="{expand_pos}" noteid="ABCD{expand_pos:04X}" children="{len(docs)}" descendants="{len(docs)}">\n  <entrydata columnnumber="0" name="wano" category="true"><number>2025</number></entrydata>\n</viewentry>\n'
    doc_entries = ""
    for i, doc in enumerate(docs, 1):
        doc_entries += f"""<viewentry position="{expand_pos}.{i}" unid="{doc["unid"]}" noteid="50{i:04X}" siblings="{len(docs)}">
  <entrydata columnnumber="2" name="wnumero"><text>{doc["title"]}</text></entrydata>
  <entrydata columnnumber="3" name="wementa"><text>{doc.get("summary", "")}</text></entrydata>
</viewentry>
"""
    return f'<?xml version="1.0" encoding="UTF-8"?>\n<viewentries toplevelentries="{len(docs)}">\n{cat_entry}{doc_entries}</viewentries>'


# ---------------------------------------------------------------------------
# TYPES and SITUATIONS constants
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 11
    REQUIRED_KEYS = {"Emenda Constitucional", "Lei Estadual"}
    REQUIRE_INT_VALUES = False


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_nao_consta_key_present(self):
        assert "Não consta" in SITUATIONS


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
        cat_resp = MagicMock()
        cat_resp.__bool__ = lambda s: True
        cat_resp.text = AsyncMock(return_value=cat_xml)
        scraper.request_service.make_request = AsyncMock(return_value=cat_resp)
        result = await scraper._get_type_year_docs(
            "Lei Estadual", "/Lei%20Estadual", 2025
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_year_with_zero_descendants_returns_empty(self):
        scraper = _make_scraper()
        cat_xml = _make_category_xml([(2025, 1, 0)])
        cat_resp = MagicMock()
        cat_resp.__bool__ = lambda s: True
        cat_resp.text = AsyncMock(return_value=cat_xml)
        scraper.request_service.make_request = AsyncMock(return_value=cat_resp)
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

        responses = []
        for xml_content in [cat_xml, docs_xml]:
            resp = MagicMock()
            resp.__bool__ = lambda s: True
            resp.text = AsyncMock(return_value=xml_content)
            responses.append(resp)

        scraper.request_service.make_request = AsyncMock(side_effect=responses)

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

        async def mock_make_request(url, **kwargs):
            requests_made.append(url)
            xml_content = cat_xml if len(requests_made) == 1 else docs_xml
            resp = MagicMock()
            resp.__bool__ = lambda s: True
            resp.text = AsyncMock(return_value=xml_content)
            return resp

        scraper.request_service.make_request = mock_make_request

        await scraper._get_type_year_docs("Lei Estadual", "/Lei%20Estadual", 2025)
        assert len(requests_made) == 2
        assert "Expand=3" in requests_made[1]
        assert "Start=3" in requests_made[1]


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_resume_skip_returns_none(self):
        await assert_resume_skips(
            _make_scraper(), {"title": "Lei 1", "html_link": "/lei1.nsf/view"}
        )

    @pytest.mark.asyncio
    async def test_failed_soup_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(return_value=failed)
        scraper._save_doc_error = AsyncMock()
        doc = {"title": "Lei 1", "html_link": "/lei1.nsf/view"}
        result = await scraper._get_doc_data(doc)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_missing_body_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        # Completely stripped HTML with no body tag
        soup = BeautifulSoup("<html></html>", "html.parser")
        # Remove the body that BeautifulSoup auto-inserts by replacing it
        for tag in soup.find_all("body"):
            tag.decompose()
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        scraper._save_doc_error = AsyncMock()
        doc = {"title": "Lei 1", "html_link": "/lei1.nsf/view"}
        result = await scraper._get_doc_data(doc)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_markdown_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        soup = BeautifulSoup("<html><body><p>Short</p></body></html>", "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
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
        soup = BeautifulSoup(
            "<html><body><font face='Tahoma'>Artigo 1.</font><br>"
            "<font face='Tahoma'>Artigo 2.</font></body></html>",
            "html.parser",
        )
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        valid_md = "# Lei Estadual\n\n" + "Texto legislativo. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = {"title": "Lei 1", "html_link": "/lei1.nsf/view"}
        result = await scraper._get_doc_data(doc)
        assert result is not None
        assert "# Lei Estadual" in result["text_markdown"]
        assert "document_url" in result
        assert result["_content_extension"] == ".html"
        assert isinstance(result["_raw_content"], bytes)

    @pytest.mark.asyncio
    async def test_body_content_used_for_markdown(self):
        """_get_doc_data uses body content; removes the border=1 action bar table."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        # Mimic real Domino doc: all content inside <form>, action bar table border="1"
        soup = BeautifulSoup(
            "<html><body>"
            "<form action=''>"
            '<table border="1"><tr><td>nav actions</td></tr></table>'
            "<hr>"
            "<ul><ul>"
            "<font face='Tahoma'>Art. 1.</font><br>"
            "<font face='Tahoma'>Art. 2.</font><br>"
            "<font face='Tahoma'>Art. 3.</font>"
            "</ul></ul>"
            "</form>"
            "</body></html>",
            "html.parser",
        )
        scraper.request_service.get_soup = AsyncMock(return_value=soup)

        captured_html: list[str] = []

        async def fake_get_markdown(html_content: str = "") -> str:
            captured_html.append(html_content)
            return "# Lei\n\n" + "Conteúdo. " * 30

        scraper._get_markdown = fake_get_markdown
        doc = {"title": "Lei 1", "html_link": "/lei1.nsf/view"}
        await scraper._get_doc_data(doc)

        assert len(captured_html) == 1
        # Navigation action bar must be removed
        assert "nav actions" not in captured_html[0]
        # Document content must be present
        assert "Art. 1." in captured_html[0]
        assert "Art. 3." in captured_html[0]

    @pytest.mark.asyncio
    async def test_html_link_removed_from_result(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        soup = BeautifulSoup(
            "<html><body><font face='Tahoma'>Texto.</font></body></html>",
            "html.parser",
        )
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        valid_md = "# Lei\n\n" + "Conteúdo legislativo. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc = {"title": "Lei 1", "html_link": "/lei1.nsf/view"}
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

        async def fake_gather(tasks, **kw):
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

        async def fake_process(docs, **kwargs):
            process_calls.append(docs)
            return []

        async def fake_gather(tasks, **kw):
            return await asyncio.gather(*tasks)

        scraper._process_documents = fake_process
        scraper._gather_results = fake_gather
        scraper._flatten_results = MSAlemsScraper._flatten_results

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

        async def fake_process(docs, **kwargs):
            calls.append({"norm_type": kwargs["norm_type"], "count": len(docs)})
            return [{"title": d["title"]} for d in docs]

        async def fake_gather(tasks, **kw):
            return await asyncio.gather(*tasks)

        scraper._process_documents = fake_process
        scraper._gather_results = fake_gather
        scraper._flatten_results = MSAlemsScraper._flatten_results

        await scraper._scrape_year(2025)

        types_called = {c["norm_type"] for c in calls}
        assert "Lei Estadual" in types_called
        assert "Decreto" in types_called
        decreto_call = next(c for c in calls if c["norm_type"] == "Decreto")
        assert decreto_call["count"] == 2
