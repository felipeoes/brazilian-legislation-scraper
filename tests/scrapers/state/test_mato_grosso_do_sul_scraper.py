"Parser and metadata regression tests for mato_grosso_do_sul_scraper."

import xml.etree.ElementTree as ET
from unittest.mock import AsyncMock, MagicMock

from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.core.scraper import DEFAULT_INVALID_SITUATION
from src.scrapers.states.ms import SITUATIONS, TYPES, MSAlemsScraper

_NSF_BASE = "/appls/legislacao/secoge/govato.nsf"


def _make_scraper(**kwargs) -> MSAlemsScraper:
    """Instantiate MSAlemsScraper with injected dependencies."""
    defaults = dict(
        _nsf=f"https://aacpdappls.net.ms.gov.br{_NSF_BASE}", _type_year_index={}
    )
    defaults.update(kwargs)
    return make_scraper(MSAlemsScraper, **defaults)


def _make_category_xml(
    categories: list[tuple[int, int, int]], *, year_field_name: str = "wano"
) -> str:
    """Build ReadViewEntries XML with year categories.

    *categories* is a list of (year, position, descendants).
    """
    entries = ""
    for year, pos, desc in categories:
        entries += f'<viewentry position="{pos}" noteid="ABCD{pos:04X}" children="{desc}" descendants="{desc}" siblings="{len(categories)}">\n  <entrydata columnnumber="0" name="{year_field_name}" category="true"><number>{year}</number></entrydata>\n  <entrydata columnnumber="4" name="wtotal"><number>{desc}</number></entrydata>\n</viewentry>'
    return f'<?xml version="1.0" encoding="UTF-8"?>\n<viewentries toplevelentries="{len(categories)}">\n{entries}\n</viewentries>'


def _make_docs_xml(
    expand_pos: int, docs: list[dict], *, summary_field_name: str = "wementa"
) -> str:
    """Build ReadViewEntries XML with document entries inside a category.

    *docs* is a list of {"unid": ..., "title": ..., "summary": ...}.
    """
    cat_entry = f'<viewentry position="{expand_pos}" noteid="ABCD{expand_pos:04X}" children="{len(docs)}" descendants="{len(docs)}">\n  <entrydata columnnumber="0" name="wano" category="true"><number>2025</number></entrydata>\n</viewentry>\n'
    doc_entries = ""
    for i, doc in enumerate(docs, 1):
        doc_entries += f'''<viewentry position="{expand_pos}.{i}" unid="{doc["unid"]}" noteid="50{i:04X}" siblings="{len(docs)}">\n  <entrydata columnnumber="2" name="wnumero"><text>{doc["title"]}</text></entrydata>\n  <entrydata columnnumber="3" name="{summary_field_name}"><text>{doc.get("summary", "")}</text></entrydata>\n</viewentry>\n'''
    return f'<?xml version="1.0" encoding="UTF-8"?>\n<viewentries toplevelentries="{len(docs)}">\n{cat_entry}{doc_entries}</viewentries>'


def _make_fetch_bytes_result(
    html: str, *, charset: str = "utf-8"
) -> tuple[bytes, MagicMock]:
    body = html.encode(charset)
    response = MagicMock()
    response.charset = charset
    return (body, response)


def _make_xml_response(xml_content: str) -> MagicMock:
    response = MagicMock()
    response.__bool__ = lambda s: True
    response.text = AsyncMock(return_value=xml_content)
    return response


def _make_doc_html(
    *,
    title: str = "LEI Nº 1/2025",
    summary: str = "Resumo da norma.",
    notes_html: str = "<b><font face='Tahoma'>Publicada no Diário Oficial.</font></b><br>",
    body_html: str = "<font face='Tahoma'>Art. 1.</font><br><font face='Tahoma'>Art. 2.</font>",
) -> str:
    return f"""\n<html><body>\n<form action=''>\n<table border="1"><tr><td>nav actions</td></tr></table>\n<hr>\n<ul><ul>\n<table border="0" cellspacing="0" cellpadding="0">\n<tr valign="top"><td width="559"><b><font face="Tahoma">{title}</font></b>\n<table border="0" cellspacing="0" cellpadding="0">\n<tr valign="top"><td width="276"><img src="/icons/ecblank.gif" border="0" height="1" width="1" alt=""></td><td width="282"><i><font face="Tahoma">{summary}</font></i></td></tr>\n</table>\n<font face="Tahoma">Notas:</font><br>\n{notes_html}\n</td></tr></table>\n</ul></ul>\n<table border="0" cellspacing="0" cellpadding="0">\n<tr valign="top"><td width="562">{body_html}</td></tr>\n</table>\n</form>\n</body></html>\n""".strip()


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


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = MSAlemsScraper
    STATE_NAME = "Mato Grosso do Sul"


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


class TestDocUrl:
    def test_uses_zero_prefix(self):
        scraper = _make_scraper()
        url = scraper._doc_url("ABCDEF1234567890ABCDEF1234567890")
        assert "/0/ABCDEF1234567890ABCDEF1234567890?OpenDocument" in url

    def test_includes_base_url(self):
        scraper = _make_scraper()
        url = scraper._doc_url("ABCD")
        assert url.startswith("https://aacpdappls.net.ms.gov.br")


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
        assert (
            MSAlemsScraper._entry_text(entry, ("wementa", "$246"))
            == "Resumo alternativo"
        )
