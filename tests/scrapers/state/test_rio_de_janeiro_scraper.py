"""Tests for RJAlerjScraper (Rio de Janeiro).

Covers:
- TYPES constant: 6 types present, correct structure
- SITUATIONS constant: exists at module level (empty dict)
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set
- _format_search_url: produces base64-encoded URL with correct page_id
- _extract_doc_number_and_year: parses number/year from title
- _normalize_rj_text: lowercases, strips diacritics, strips non-alphanumeric
- _clean_extracted_markdown: strips leading label lines, preserves content
- _get_docs_html_links: parses <tr> rows, ignores non-matching years
- _get_doc_data:
  - failed soup → None
  - no content_root → None
  - invalid markdown → None (with _valid_markdown check)
  - valid content → correct dict shape
- scrape_constitution:
  - already scraped → sets _scraped_constitution, returns early
  - failed soup → returns early
  - invalid markdown → sets _scraped_constitution, returns early
  - valid content → saves document and sets _scraped_constitution

Run with:
    .venv/bin/pytest tests/test_rio_de_janeiro_scraper.py -v
"""

from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from bs4 import BeautifulSoup

from src.scraper.state_legislation.rio_de_janeiro import (
    SITUATIONS,
    TYPES,
    RJAlerjScraper,
)
from base_tests import TypesConstantTests, SituationsConstantTests, ScraperClassTests
from conftest import make_base_scraper, make_failed_request


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> RJAlerjScraper:
    """Instantiate RJAlerjScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        RJAlerjScraper,
        "http://alerjln1.alerj.rj.gov.br/contlei.nsf",
        "RIO_DE_JANEIRO",
        TYPES,
        situations=SITUATIONS,
        lotus_base_url="https://www3.alerj.rj.gov.br/lotus_notes/default.asp",
        _scraped_constitution=False,
        params={"OpenForm": "", "Start": 1, "Count": 500},
        **kwargs,
    )


# ---------------------------------------------------------------------------
# TYPES constant
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 6
    REQUIRED_KEYS = {"LeiOrd", "Decreto", "Constituição Estadual"}
    REQUIRE_INT_VALUES = False

    def test_dict_types_have_view_name(self):
        for key, val in TYPES.items():
            if key != "Constituição Estadual":
                assert "view_name" in val
                assert "page_id" in val

    def test_page_ids_are_integers(self):
        for key, val in TYPES.items():
            if key != "Constituição Estadual":
                assert isinstance(val["page_id"], int)


# ---------------------------------------------------------------------------
# SITUATIONS constant
# ---------------------------------------------------------------------------


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = True


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = RJAlerjScraper
    STATE_NAME = "Rio de Janeiro"


# ---------------------------------------------------------------------------
# _extract_doc_number_and_year (static)
# ---------------------------------------------------------------------------


class TestExtractDocNumberAndYear:
    def test_standard_title(self):
        number, year = RJAlerjScraper._extract_doc_number_and_year("Decreto 123/2020")
        assert number == "123"
        assert year == "2020"

    def test_title_with_dotted_number(self):
        number, year = RJAlerjScraper._extract_doc_number_and_year("Lei 1.234/2019")
        assert number == "1.234"
        assert year == "2019"

    def test_fallback_to_date(self):
        number, year = RJAlerjScraper._extract_doc_number_and_year(
            "Decreto sem número", "2021"
        )
        assert number == ""
        assert year == "2021"

    def test_no_year_no_date(self):
        number, year = RJAlerjScraper._extract_doc_number_and_year("Sem info")
        assert number == ""
        assert year == ""


# ---------------------------------------------------------------------------
# _normalize_rj_text (static)
# ---------------------------------------------------------------------------


class TestNormalizeRjText:
    def test_removes_accents(self):
        result = RJAlerjScraper._normalize_rj_text("Constituição")
        assert result == "constituicao"

    def test_lowercase(self):
        result = RJAlerjScraper._normalize_rj_text("DECRETO")
        assert result == "decreto"

    def test_removes_punctuation(self):
        result = RJAlerjScraper._normalize_rj_text("Art. 1º —")
        assert "." not in result
        assert "º" not in result

    def test_empty_string(self):
        assert RJAlerjScraper._normalize_rj_text("") == ""


# ---------------------------------------------------------------------------
# _clean_extracted_markdown (static)
# ---------------------------------------------------------------------------


class TestCleanExtractedMarkdown:
    def test_strips_leading_label(self):
        md = "Lei Ordinária\n\nArt. 1º texto aqui"
        result = RJAlerjScraper._clean_extracted_markdown(md)
        assert not result.startswith("Lei Ordinária")
        assert "Art. 1º texto aqui" in result

    def test_strips_leading_whitespace_lines(self):
        md = "\n\n\nConteúdo da norma"
        result = RJAlerjScraper._clean_extracted_markdown(md)
        assert result == "Conteúdo da norma"

    def test_strips_texto_da_prefix(self):
        md = "Texto da Lei\n\nArt. 1º conteúdo"
        result = RJAlerjScraper._clean_extracted_markdown(md)
        assert not result.startswith("Texto da")

    def test_preserves_content_unchanged(self):
        md = "Art. 1º Esta lei dispõe sobre algo."
        result = RJAlerjScraper._clean_extracted_markdown(md)
        assert result == md


# ---------------------------------------------------------------------------
# _get_docs_html_links
# ---------------------------------------------------------------------------


class TestGetDocsHtmlLinks:
    def _make_soup(self, rows_html: str) -> BeautifulSoup:
        return BeautifulSoup(f"<table>{rows_html}</table>", "html.parser")

    def _make_row(
        self, number: str, year: int, author: str = "Gov", summary: str = "Sumário"
    ) -> str:
        return (
            f"<tr>"
            f'<td><a data-role="/contlei.nsf/doc?Open" href="#">{number}</a></td>'
            f"<td>{year}</td>"
            f"<td>{author}</td>"
            f"<td>{summary}</td>"
            f"</tr>"
        )

    def test_matching_year_returns_doc(self):
        scraper = _make_scraper()
        row = self._make_row("42", 2023)
        soup = self._make_soup(row)
        docs, years = scraper._get_docs_html_links("Decreto", 50, soup, 2023)
        assert len(docs) == 1
        assert docs[0]["title"] == "Decreto 42/2023"
        assert 2023 in years

    def test_different_year_excluded_from_docs_but_in_years(self):
        scraper = _make_scraper()
        row = self._make_row("10", 2021)
        soup = self._make_soup(row)
        docs, years = scraper._get_docs_html_links("Decreto", 50, soup, 2023)
        assert len(docs) == 0
        assert 2021 in years

    def test_row_without_link_skipped(self):
        scraper = _make_scraper()
        soup = self._make_soup(
            "<tr><td>No link</td><td>2023</td><td>A</td><td>B</td></tr>"
        )
        docs, years = scraper._get_docs_html_links("Decreto", 50, soup, 2023)
        assert len(docs) == 0

    def test_row_with_invalid_year_skipped(self):
        scraper = _make_scraper()
        soup = self._make_soup(
            '<tr><td><a href="#">42</a></td><td>not-a-year</td><td>A</td><td>B</td></tr>'
        )
        docs, years = scraper._get_docs_html_links("Decreto", 50, soup, 2023)
        assert len(docs) == 0
        assert years == []


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    def _make_doc_info(self, **kwargs):
        base = {
            "_html_link": "http://example.com/doc",
            "document_url": "http://example.com/doc",
            "title": "Decreto 1/2023",
            "summary": "Trata de algo",
            "date": "2023",
        }
        base.update(kwargs)
        return base

    @pytest.mark.asyncio
    async def test_already_scraped_returns_none(self):
        scraper = _make_scraper()
        scraper._scraped_keys = {("http://example.com/doc", "Decreto 1/2023")}
        result = await scraper._get_doc_data(self._make_doc_info())
        assert result is None

    @pytest.mark.asyncio
    async def test_failed_soup_returns_none(self):
        scraper = _make_scraper()
        scraper._fetch_soup_and_mhtml = AsyncMock(side_effect=Exception("fetch failed"))
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc_info())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_content_root_returns_none(self):
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        mhtml = b"MHTML content"
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, mhtml))
        scraper._extract_norm_content_root = MagicMock(return_value=None)
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc_info())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_markdown_returns_none(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(
            "<html><body><div>content</div></body></html>", "html.parser"
        )
        content_root = BeautifulSoup("<div>content</div>", "html.parser")
        mhtml = b"MHTML content"
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, mhtml))
        scraper._extract_norm_content_root = MagicMock(return_value=content_root)
        scraper._clean_norm_content_root = MagicMock()
        scraper._trim_to_norm_start = MagicMock()
        scraper._remove_header_metadata_element = MagicMock()
        scraper._remove_summary_element = MagicMock()
        scraper._html_to_markdown = AsyncMock(return_value="short")
        scraper._clean_extracted_markdown = MagicMock(return_value="short")
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc_info())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_valid_content_returns_correct_shape(self):
        scraper = _make_scraper()
        html = "<html><body><div>content</div></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        mhtml = b"MHTML content"
        content_root = BeautifulSoup("<div>Conteúdo da lei</div>", "html.parser")
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, mhtml))
        scraper._extract_norm_content_root = MagicMock(return_value=content_root)
        scraper._clean_norm_content_root = MagicMock()
        scraper._trim_to_norm_start = MagicMock()
        scraper._remove_header_metadata_element = MagicMock()
        scraper._remove_summary_element = MagicMock()
        valid_md = "# Decreto 1/2023\n\n" + "Texto do decreto. " * 30
        scraper._html_to_markdown = AsyncMock(return_value=valid_md)
        scraper._clean_extracted_markdown = MagicMock(return_value=valid_md)
        scraper._wrap_html = MagicMock(return_value="<html></html>")

        result = await scraper._get_doc_data(self._make_doc_info(), year=2023)
        assert result is not None
        assert result["text_markdown"] == valid_md
        assert result["year"] == 2023
        assert result["_content_extension"] == ".mhtml"
        assert result["_raw_content"] == mhtml
        assert "_html_link" not in result

    @pytest.mark.asyncio
    async def test_revogada_situation_detected(self):
        scraper = _make_scraper()
        html = "<html><body><div>[ Revogada ]</div></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        mhtml = b"MHTML content"
        content_root = BeautifulSoup("<div>[ Revogada ] conteúdo</div>", "html.parser")
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, mhtml))
        scraper._extract_norm_content_root = MagicMock(return_value=content_root)
        scraper._clean_norm_content_root = MagicMock()
        scraper._trim_to_norm_start = MagicMock()
        scraper._remove_header_metadata_element = MagicMock()
        scraper._remove_summary_element = MagicMock()
        valid_md = "# Decreto revogado\n\n" + "Texto do decreto. " * 30
        scraper._html_to_markdown = AsyncMock(return_value=valid_md)
        scraper._clean_extracted_markdown = MagicMock(return_value=valid_md)
        scraper._wrap_html = MagicMock(return_value="<html></html>")

        result = await scraper._get_doc_data(self._make_doc_info(), year=2023)
        assert result is not None
        assert result["situation"] == "Revogada"

    @pytest.mark.asyncio
    async def test_sem_revogacao_situation_default(self):
        scraper = _make_scraper()
        html = "<html><body><div>content sem revogacao</div></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        mhtml = b"MHTML content"
        content_root = BeautifulSoup("<div>Conteúdo normal</div>", "html.parser")
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, mhtml))
        scraper._extract_norm_content_root = MagicMock(return_value=content_root)
        scraper._clean_norm_content_root = MagicMock()
        scraper._trim_to_norm_start = MagicMock()
        scraper._remove_header_metadata_element = MagicMock()
        scraper._remove_summary_element = MagicMock()
        valid_md = "# Decreto\n\n" + "Texto do decreto. " * 30
        scraper._html_to_markdown = AsyncMock(return_value=valid_md)
        scraper._clean_extracted_markdown = MagicMock(return_value=valid_md)
        scraper._wrap_html = MagicMock(return_value="<html></html>")

        result = await scraper._get_doc_data(self._make_doc_info(), year=2023)
        assert result is not None
        assert result["situation"] == "Sem revogação expressa"


# ---------------------------------------------------------------------------
# scrape_constitution
# ---------------------------------------------------------------------------


class TestScrapeConstitution:
    @pytest.mark.asyncio
    async def test_already_scraped_sets_flag_and_returns(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=True)
        await scraper.scrape_constitution()
        assert scraper._scraped_constitution is True

    @pytest.mark.asyncio
    async def test_failed_soup_returns_early(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(return_value=failed)
        await scraper.scrape_constitution()
        # _scraped_constitution stays False since we returned early
        assert scraper._scraped_constitution is False

    @pytest.mark.asyncio
    async def test_invalid_markdown_sets_flag(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        html = '<html><body><a data-role="/doc1">Título I</a></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        scraper._fetch_constitution_section = AsyncMock(return_value="<div>ok</div>")
        scraper._get_markdown = AsyncMock(return_value="short")
        await scraper.scrape_constitution()
        assert scraper._scraped_constitution is True

    @pytest.mark.asyncio
    async def test_valid_content_saves_doc_and_sets_flag(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        html = '<html><body><a data-role="/doc1">Título I</a></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        scraper._fetch_constitution_section = AsyncMock(
            return_value="<div>Constituição</div>"
        )
        valid_md = "# Constituição Estadual\n\n" + "Art. 1º conteúdo. " * 50
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        scraper._wrap_html = MagicMock(return_value="<html></html>")
        scraper._capture_mhtml = AsyncMock(return_value=b"MHTML content")
        scraper._save_doc_result = AsyncMock(return_value={"saved": True})
        scraper._track_results = MagicMock()

        await scraper.scrape_constitution()
        assert scraper._scraped_constitution is True
        scraper._save_doc_result.assert_called_once()
        scraper._track_results.assert_called_once()

    @pytest.mark.asyncio
    async def test_emendas_link_filtered_out(self):
        """Links with EMENDAS CONSTITUCIONAIS in text are excluded from sections."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        html = (
            "<html><body>"
            '<a data-role="/doc1">Título I</a>'
            '<a data-role="/emendas">EMENDAS CONSTITUCIONAIS</a>'
            "</body></html>"
        )
        soup = BeautifulSoup(html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        call_args = []

        async def fake_fetch(a_link):
            call_args.append(a_link["data-role"])
            return "<div>ok</div>"

        scraper._fetch_constitution_section = fake_fetch
        valid_md = "# Constituição\n\n" + "Texto. " * 50
        scraper._html_to_markdown = AsyncMock(return_value=valid_md)
        scraper._wrap_html = MagicMock(return_value="<html></html>")
        scraper._save_doc_result = AsyncMock(return_value=None)
        scraper._track_results = MagicMock()

        await scraper.scrape_constitution()
        # Only /doc1 should be fetched, not /emendas
        assert "/emendas" not in call_args
        assert "/doc1" in call_args


# ---------------------------------------------------------------------------
# _clean_constitution_section_soup — string= not text=
# ---------------------------------------------------------------------------


class TestCleanConstitutionSectionSoup:
    def test_removes_text_titulo_string_kwarg(self):
        """Ensure deprecated text= kwarg was replaced by string= for BeautifulSoup.find_all."""
        scraper = _make_scraper()
        html = (
            "<html><body><p>Texto do Título</p><p>Conteúdo importante</p></body></html>"
        )
        soup = BeautifulSoup(html, "html.parser")
        # Should not raise MarkupResemblesLocatorWarning or DeprecationWarning
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            scraper._clean_constitution_section_soup(soup)
        # The "Texto do Título" paragraph should be decomposed
        texts = [tag.get_text() for tag in soup.find_all("p")]
        assert "Texto do Título" not in texts

    def test_removes_alert_warning_divs(self):
        scraper = _make_scraper()
        html = (
            "<html><body>"
            '<div class="alert alert-warning">Warning!</div>'
            "<p>Conteúdo</p>"
            "</body></html>"
        )
        soup = BeautifulSoup(html, "html.parser")
        scraper._clean_constitution_section_soup(soup)
        assert soup.find("div", class_="alert alert-warning") is None

    def test_removes_barra_botoes_div(self):
        scraper = _make_scraper()
        html = (
            '<html><body><div id="barraBotoes">Botões</div><p>Texto</p></body></html>'
        )
        soup = BeautifulSoup(html, "html.parser")
        scraper._clean_constitution_section_soup(soup)
        assert soup.find("div", id="barraBotoes") is None


# ---------------------------------------------------------------------------
# RJ scraper helper methods (moved from test_core.py)
# ---------------------------------------------------------------------------


class TestRJAlerjHelpers:
    def test_build_wrapped_data_role_url(self):
        scraper = object.__new__(RJAlerjScraper)
        scraper.lotus_base_url = "https://www3.alerj.rj.gov.br/lotus_notes/default.asp"

        wrapped_url = scraper._build_wrapped_data_role_url(
            53,
            "/contlei.nsf/LeiOrdInt?OpenForm&Count=500&Start=1",
        )

        assert wrapped_url.startswith(
            "https://www3.alerj.rj.gov.br/lotus_notes/default.asp?id=53&url="
        )

    def test_get_docs_html_links_filters_current_year_and_supports_live_table_shapes(
        self,
    ):
        scraper = object.__new__(RJAlerjScraper)
        scraper.base_url = "http://alerjln1.alerj.rj.gov.br/contlei.nsf"
        scraper.lotus_base_url = "https://www3.alerj.rj.gov.br/lotus_notes/default.asp"
        soup = BeautifulSoup(
            """
            <html><body><table>
                <tr valign="top">
                    <td><a data-role="/contlei.nsf/abc/def?OpenDocument">228</a></td>
                    <td>2025</td>
                    <td></td>
                    <td>Em Vigor</td>
                    <td>Lei complementar de teste</td>
                    <td>Autor 1</td>
                </tr>
                <tr valign="top">
                    <td><a data-role="/contlei.nsf/ghi/jkl?OpenDocument">03</a></td>
                    <td>2025</td>
                    <td>Autor 2</td>
                    <td>Decreto de teste</td>
                </tr>
                <tr valign="top">
                    <td><a data-role="/contlei.nsf/mno/pqr?OpenDocument">999</a></td>
                    <td>2024</td>
                    <td></td>
                    <td>Em Vigor</td>
                    <td>Outra lei</td>
                    <td>Autor 3</td>
                </tr>
            </table></body></html>
            """,
            "html.parser",
        )

        docs, years = scraper._get_docs_html_links(
            "Lei",
            53,
            soup,
            2025,
        )

        assert years == [2025, 2025, 2024]
        assert len(docs) == 2
        assert docs[0]["title"] == "Lei 228/2025"
        assert docs[0]["author"] == "Autor 1"
        assert docs[0]["_html_link"].startswith(
            "https://www3.alerj.rj.gov.br/lotus_notes/default.asp?id=53&url="
        )
        assert docs[1]["title"] == "Lei 03/2025"
        assert docs[1]["author"] == "Autor 2"
        assert (
            docs[1]["document_url"]
            == "http://alerjln1.alerj.rj.gov.br/contlei.nsf/ghi/jkl"
        )

    def test_extract_norm_content_root_uses_heading_and_sibling_body_content(self):
        scraper = object.__new__(RJAlerjScraper)
        soup = BeautifulSoup(
            """
            <html><body>
                <div class="pagina_central">
                    <div id="divConteudo">
                        <div class="alert alert-warning">metadata only</div>
                        <b>LEI COMPLEMENTAR Nº 226, DE 04 DE DEZEMBRO DE 2025.</b>
                    </div>
                    <b>LEI COMPLEMENTAR Nº 226, DE 04 DE DEZEMBRO DE 2025.</b>
                    <br/>
                    <b>O GOVERNADOR DO ESTADO DO RIO DE JANEIRO</b>
                    <b>Art. 1º</b><font>Conteúdo do artigo.</font>
                </div>
            </body></html>
            """,
            "html.parser",
        )

        content_root = scraper._extract_norm_content_root(soup)
        assert content_root is not None
        scraper._clean_norm_content_root(content_root)
        scraper._trim_to_norm_start(content_root)
        scraper._remove_summary_element(content_root, "")

        text = content_root.get_text(" ", strip=True)
        assert "metadata only" not in text
        assert "LEI COMPLEMENTAR Nº 226" in text
        assert "O GOVERNADOR DO ESTADO DO RIO DE JANEIRO" in text
        assert "Art. 1º" in text

    def test_remove_summary_element_removes_only_matching_summary_block(self):
        scraper = object.__new__(RJAlerjScraper)
        summary = "ALTERA A LEI COMPLEMENTAR Nº 15, DE 25 DE NOVEMBRO DE 1980."
        soup = BeautifulSoup(
            f"""
            <div>
                <table id="summary"><tr><td><b>{summary}</b></td></tr></table>
                <b>LEI COMPLEMENTAR Nº 226, DE 04 DE DEZEMBRO DE 2025.</b>
                <b>Art. 1º</b><table id="norm-table"><tr><td>Quadro do artigo</td></tr></table>
            </div>
            """,
            "html.parser",
        )

        assert soup.div is not None
        content_root = cast(BeautifulSoup, soup.div)
        scraper._remove_summary_element(content_root, summary)

        assert content_root.find("table", id="summary") is None
        assert content_root.find("table", id="norm-table") is not None
        text = content_root.get_text(" ", strip=True)
        assert summary not in text
        assert "Art. 1º" in text

    def test_remove_summary_element_keeps_non_matching_norm_table(self):
        scraper = object.__new__(RJAlerjScraper)
        summary = "ALTERA DISPOSITIVOS DA LEI X."
        soup = BeautifulSoup(
            """
            <div>
                <b>LEI Nº 1, DE 2025.</b>
                <table id="norm-table">
                    <tr><td>Art. 1º</td><td>Tabela normativa relevante</td></tr>
                </table>
            </div>
            """,
            "html.parser",
        )

        assert soup.div is not None
        content_root = cast(BeautifulSoup, soup.div)
        scraper._remove_summary_element(content_root, summary)

        assert content_root.find("table", id="norm-table") is not None
        assert "Tabela normativa relevante" in content_root.get_text(" ", strip=True)

    def test_remove_header_metadata_element_removes_only_matching_header_block(self):
        scraper = object.__new__(RJAlerjScraper)
        doc_info = {"title": "LeiComp 226/2025", "date": "2025"}
        soup = BeautifulSoup(
            """
            <div>
                <table id="header-meta">
                    <tr>
                        <td><b>Lei Complementar nº</b></td>
                        <td>226 / 2025</td>
                        <td><b>Data da promulgação</b></td>
                        <td>04/12/2025</td>
                    </tr>
                </table>
                <b>LEI COMPLEMENTAR Nº 226, DE 04 DE DEZEMBRO DE 2025.</b>
                <table id="norm-table"><tr><td>Art. 1º</td><td>Quadro normativo</td></tr></table>
            </div>
            """,
            "html.parser",
        )

        assert soup.div is not None
        content_root = cast(BeautifulSoup, soup.div)
        scraper._remove_header_metadata_element(content_root, doc_info)

        assert content_root.find("table", id="header-meta") is None
        assert content_root.find("table", id="norm-table") is not None
        text = content_root.get_text(" ", strip=True)
        assert "Data da promulgação" not in text
        assert "LEI COMPLEMENTAR Nº 226" in text
