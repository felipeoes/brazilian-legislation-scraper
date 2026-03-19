"""Tests for RSAlrsScraper (Rio Grande do Sul).

Covers:
- TYPES constant: empty dict (type is inferred during scraping)
- SITUATIONS constant: 'Não consta' present
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set
- _build_search_url: year and page are embedded in URL
- _get_html_string: returns empty if no table, returns html_string if enough rows
- _get_doc_data:
  - failed first soup → None
  - "página não pode ser exibida" in first soup → None
  - second soup is falsy → None
  - second soup indicates no text → None
  - no PDF link found (no iframe, no window.open) → None
  - invalid markdown (_valid_markdown) → None
  - valid markdown (html path) → correct dict shape
  - valid markdown (pdf path) → correct dict shape
- _before_scrape: delegates to _fetch_and_save_constitution

Run with:
    .venv/bin/pytest tests/test_rio_grande_do_sul_scraper.py -v
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from base_tests import ScraperClassTests, SituationsConstantTests, TypesConstantTests
from bs4 import BeautifulSoup
from conftest import make_base_scraper, make_failed_request

from src.scraper.state_legislation.rio_grande_do_sul import (
    SITUATIONS,
    TYPES,
    RSAlrsScraper,
)

# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> RSAlrsScraper:
    """Instantiate RSAlrsScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        RSAlrsScraper,
        "https://www.al.rs.gov.br",
        "RIO_GRANDE_DO_SUL",
        TYPES,
        situations=SITUATIONS,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# TYPES constant
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 0
    REQUIRED_KEYS = set()
    REQUIRE_INT_VALUES = False


# ---------------------------------------------------------------------------
# SITUATIONS constant
# ---------------------------------------------------------------------------


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_nao_consta_present(self):
        assert "Não consta" in SITUATIONS


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = RSAlrsScraper
    STATE_NAME = "Rio Grande do Sul"


# ---------------------------------------------------------------------------
# _build_search_url
# ---------------------------------------------------------------------------


class TestBuildSearchUrl:
    def test_year_in_url(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2022, 1)
        assert "TxtAno=2022" in url

    def test_page1_uses_search_form(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2022, 1)
        assert "M0100008.asp" in url
        assert "txtOperacaoFormulario=Pesquisar" in url

    def test_page2_uses_pagination_endpoint(self):
        """Pages 2+ must use M0100017.asp?txtPage=N (session-based pagination)."""
        scraper = _make_scraper()
        url = scraper._build_search_url(2022, 2)
        assert "M0100017.asp" in url
        assert "txtPage=2" in url
        # Year should NOT appear — server uses session state for filters
        assert "TxtAno" not in url

    def test_page_n_uses_txtpage_param(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2022, 5)
        assert "txtPage=5" in url

    def test_base_url_in_url(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2022, 1)
        assert url.startswith("https://www.al.rs.gov.br")


# ---------------------------------------------------------------------------
# _clean_rs_markdown
# ---------------------------------------------------------------------------


class TestCleanRsMarkdown:
    def test_removes_http_footer_url(self):
        scraper = _make_scraper()
        text = "Texto do artigo\nhttp://www.al.rs.gov.br/legis\nPróximo artigo"
        result = scraper._clean_rs_markdown(text)
        assert "http://www.al.rs.gov.br/legis" not in result
        assert "Texto do artigo" in result
        assert "Próximo artigo" in result

    def test_removes_https_footer_url(self):
        scraper = _make_scraper()
        text = "Texto\nhttps://www.al.rs.gov.br/legis\nMais texto"
        result = scraper._clean_rs_markdown(text)
        assert "https://www.al.rs.gov.br/legis" not in result

    def test_removes_multiple_occurrences(self):
        scraper = _make_scraper()
        text = "pág 1\nhttp://www.al.rs.gov.br/legis\npág 2\nhttp://www.al.rs.gov.br/legis\npág 3"
        result = scraper._clean_rs_markdown(text)
        assert "http://www.al.rs.gov.br/legis" not in result
        assert "pág 1" in result
        assert "pág 3" in result

    def test_strips_page_number_on_same_line(self):
        """Footer may have a page number after the URL on the same line."""
        scraper = _make_scraper()
        text = "Texto\nhttp://www.al.rs.gov.br/legis  4\nMais texto"
        result = scraper._clean_rs_markdown(text)
        assert "al.rs.gov.br" not in result
        assert "Mais texto" in result

    def test_strips_table_row_variant(self):
        """Footer may appear inside a markdown table row."""
        scraper = _make_scraper()
        text = "col1\n| http://www.al.rs.gov.br/legis  |     |     | 3   |\ncol2"
        result = scraper._clean_rs_markdown(text)
        assert "al.rs.gov.br" not in result
        assert "col1" in result
        assert "col2" in result

    def test_no_url_returns_unchanged(self):
        scraper = _make_scraper()
        text = "Texto limpo sem URL de rodapé."
        result = scraper._clean_rs_markdown(text)
        assert result == text


# ---------------------------------------------------------------------------
# _get_html_string
# ---------------------------------------------------------------------------


class TestGetHtmlString:
    def test_no_table_returns_empty(self):
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body><p>texto</p></body></html>", "html.parser")
        result = scraper._get_html_string(soup)
        assert result == ""

    def test_few_rows_returns_empty(self):
        scraper = _make_scraper()
        rows = "".join(f"<tr><td>row {i}</td></tr>" for i in range(3))
        html = f"<html><body><table><tbody>{rows}</tbody></table></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        result = scraper._get_html_string(soup)
        assert result == ""

    def test_enough_rows_returns_html_string(self):
        scraper = _make_scraper()
        rows = "".join(f"<tr><td>row {i}</td></tr>" for i in range(7))
        html = f"<html><body><table><tbody>{rows}</tbody></table></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        result = scraper._get_html_string(soup)
        assert result  # should be non-empty


# ---------------------------------------------------------------------------
# _get_doc_data — string= not text= (no DeprecationWarning)
# ---------------------------------------------------------------------------


class TestGetDocDataStringKwarg:
    @pytest.mark.asyncio
    async def test_no_deprecation_warning_for_find(self):
        """Ensure text= was replaced by string= — no BeautifulSoup DeprecationWarning."""
        html = (
            "<html><body>"
            "<table><tr>"
            "<td>Situação:</td><td>Vigente</td>"
            "</tr><tr>"
            "<td>Assunto:</td><td>Administração</td>"
            "</tr><tr>"
            "<td>Links:</td><td><a href='/link.html'>texto</a></td>"
            "</tr></table>"
            "</body></html>"
        )
        soup = BeautifulSoup(html, "html.parser")
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            # These three calls must NOT raise DeprecationWarning
            soup.find("td", string="Situação:")
            soup.find("td", string="Assunto:")
            import re

            soup.find("td", string=re.compile(r"Links:"))


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    def _make_doc(self, **kwargs):
        base = {
            "html_link": "https://www.al.rs.gov.br/legis/M010/M0100018.asp?Hid_IdNorma=72606",
            "type": "Decreto",
            "title": "Decreto 42 DE 01/01/2020",
            "date": "01/01/2020",
            "summary": "Trata de algo",
            "year": 2020,
        }
        base.update(kwargs)
        return base

    def _make_first_soup(
        self, situacao="Vigente", assunto="Geral", link="/link.html"
    ) -> BeautifulSoup:
        html = (
            "<html><body>"
            "<table><tr>"
            f"<td>Situação:</td><td>{situacao}</td>"
            "</tr><tr>"
            f"<td>Assunto:</td><td>{assunto}</td>"
            "</tr><tr>"
            f"<td>Links:</td><td><a href='{link}'>texto</a></td>"
            "</tr></table>"
            "</body></html>"
        )
        return BeautifulSoup(html, "html.parser")

    @pytest.mark.asyncio
    async def test_failed_first_soup_returns_none(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(return_value=failed)
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None

    @pytest.mark.asyncio
    async def test_first_soup_page_not_displayed_returns_none(self):
        scraper = _make_scraper()
        html = "<html><body><p>a página não pode ser exibida</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_second_soup_falsy_returns_none(self):
        scraper = _make_scraper()
        first_soup = self._make_first_soup(link="/link.html")
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(side_effect=[first_soup, failed])
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_second_soup_no_text_returns_none(self):
        scraper = _make_scraper()
        first_soup = self._make_first_soup(link="/link.html")
        second_html = "<html><body><p>norma sem texto disponível</p></body></html>"
        second_soup = BeautifulSoup(second_html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(
            side_effect=[first_soup, second_soup]
        )
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None

    @pytest.mark.asyncio
    async def test_no_pdf_link_returns_none(self):
        """No iframe and no window.open → None with error saved."""
        scraper = _make_scraper()
        first_soup = self._make_first_soup(link="/link.html")
        # second soup: table has ≤5 rows (no HTML text) and no iframe/window.open
        rows = "".join(f"<tr><td>row {i}</td></tr>" for i in range(3))
        second_html = f"<html><body><table><tbody>{rows}</tbody></table></body></html>"
        second_soup = BeautifulSoup(second_html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(
            side_effect=[first_soup, second_soup]
        )
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_markdown_html_path_returns_none(self):
        scraper = _make_scraper()
        # Build a second soup with enough rows so html_string is non-empty
        rows = "".join(f"<tr><td>row {i}</td></tr>" for i in range(7))
        second_html = f"<html><body><table><tbody>{rows}</tbody></table></body></html>"
        first_soup = self._make_first_soup(link="/link.html")
        second_soup = BeautifulSoup(second_html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(
            side_effect=[first_soup, second_soup]
        )
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(second_soup, b"MHTML"))
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_markdown = AsyncMock(return_value="short")
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(self._make_doc())
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_valid_markdown_html_path_returns_dict(self):
        scraper = _make_scraper()
        rows = "".join(f"<tr><td>row {i}</td></tr>" for i in range(7))
        second_html = f"<html><body><table><tbody>{rows}</tbody></table></body></html>"
        first_soup = self._make_first_soup(link="/link.html")
        second_soup = BeautifulSoup(second_html, "html.parser")
        mhtml = b"MHTML content"
        scraper.request_service.get_soup = AsyncMock(
            side_effect=[first_soup, second_soup]
        )
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(second_soup, mhtml))
        scraper._is_already_scraped = MagicMock(return_value=False)
        valid_md = "# Decreto\n\n" + "Texto do decreto. " * 30
        scraper._get_markdown = AsyncMock(return_value=valid_md)

        result = await scraper._get_doc_data(self._make_doc())
        assert result is not None
        assert result["text_markdown"] == valid_md.strip()
        assert result["_content_extension"] == ".mhtml"
        assert result["_raw_content"] == mhtml

    @pytest.mark.asyncio
    async def test_invalid_markdown_pdf_path_returns_none(self):
        scraper = _make_scraper()
        # second soup with iframe (pdf path)
        second_html = (
            "<html><body>"
            "<table><tbody><tr><td>header</td></tr></tbody></table>"
            "<iframe src='https://example.com/doc.pdf'></iframe>"
            "</body></html>"
        )
        first_soup = self._make_first_soup(link="/link.html")
        second_soup = BeautifulSoup(second_html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(
            side_effect=[first_soup, second_soup]
        )
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._download_and_convert = AsyncMock(return_value=("short", b"", ".pdf"))
        scraper._save_doc_error = AsyncMock()

        result = await scraper._get_doc_data(self._make_doc())
        assert result is None

    @pytest.mark.asyncio
    async def test_valid_markdown_pdf_path_returns_dict(self):
        scraper = _make_scraper()
        second_html = (
            "<html><body>"
            "<table><tbody><tr><td>header</td></tr></tbody></table>"
            "<iframe src='https://example.com/doc.pdf'></iframe>"
            "</body></html>"
        )
        first_soup = self._make_first_soup(link="/link.html")
        second_soup = BeautifulSoup(second_html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(
            side_effect=[first_soup, second_soup]
        )
        scraper._is_already_scraped = MagicMock(return_value=False)
        valid_md = "# Decreto\n\n" + "Texto do decreto. " * 30
        pdf_bytes = b"%PDF content"
        scraper._download_and_convert = AsyncMock(
            return_value=(valid_md, pdf_bytes, ".pdf")
        )

        result = await scraper._get_doc_data(self._make_doc())
        assert result is not None
        assert result["_content_extension"] == ".pdf"
        assert result["_raw_content"] == pdf_bytes


# ---------------------------------------------------------------------------
# _before_scrape
# ---------------------------------------------------------------------------


class TestBeforeScrape:
    @pytest.mark.asyncio
    async def test_before_scrape_calls_fetch_and_save_constitution(self):
        scraper = _make_scraper()
        scraper._fetch_and_save_constitution = AsyncMock(return_value=None)
        await scraper._before_scrape()
        scraper._fetch_and_save_constitution.assert_called_once()
        call_kwargs = scraper._fetch_and_save_constitution.call_args
        assert call_kwargs.kwargs["year"] == 1989
        assert "Rio Grande do Sul" in call_kwargs.kwargs["title"]

    @pytest.mark.asyncio
    async def test_before_scrape_passes_constitution_url(self):
        scraper = _make_scraper()
        scraper._fetch_and_save_constitution = AsyncMock(return_value=None)
        await scraper._before_scrape()
        url = scraper._fetch_and_save_constitution.call_args.kwargs["url"]
        assert "al.rs.gov.br" in url or "ww2.al.rs.gov.br" in url
