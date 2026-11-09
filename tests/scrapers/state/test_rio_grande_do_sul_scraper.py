"Parser and metadata regression tests for rio_grande_do_sul_scraper."

import subprocess
import sys

import pytest
from bs4 import BeautifulSoup
from conftest import make_mock_response, make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.rs import SITUATIONS, TYPES, RSAlrsScraper


def _make_scraper(**kwargs) -> RSAlrsScraper:
    """Instantiate RSAlrsScraper with injected dependencies."""
    return make_scraper(RSAlrsScraper, **kwargs)


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 0
    REQUIRED_KEYS = set()
    REQUIRE_INT_VALUES = False


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_nao_consta_present(self):
        assert "Não consta" in SITUATIONS


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = RSAlrsScraper
    STATE_NAME = "Rio Grande do Sul"


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
        assert "TxtAno" not in url

    def test_page_n_uses_txtpage_param(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2022, 5)
        assert "txtPage=5" in url

    def test_base_url_in_url(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2022, 1)
        assert url.startswith("https://www.al.rs.gov.br")


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

    def test_keeps_inline_image_lines(self):
        scraper = _make_scraper()
        image = "![](data:image/png;base64,AAAA)"
        text = f"Art. 1º\n{image}\nhttp://www.al.rs.gov.br/legis  2\nArt. 2º"
        assert scraper._clean_rs_markdown(text) == f"Art. 1º\n{image}\nArt. 2º"

    def test_multi_megabyte_image_line_is_linear(self):
        """Embedded base64 images are one huge line; a backtracking regex froze the event loop.

        A subprocess is the only reliable timeout: the regex holds the GIL.
        """
        code = (
            "from src.scrapers.states.rs import RSAlrsScraper as S; "
            "S._clean_rs_markdown('![](data:,' + 'A' * 1_000_000 + ')')"
        )
        subprocess.run([sys.executable, "-c", code], timeout=10, check=True)


class TestGetHtmlString:
    def test_no_table_returns_empty(self):
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body><p>texto</p></body></html>", "lxml")
        result = scraper._get_html_string(soup)
        assert result == ""

    def test_few_rows_returns_empty(self):
        scraper = _make_scraper()
        rows = "".join((f"<tr><td>row {i}</td></tr>" for i in range(3)))
        html = f"<html><body><table><tbody>{rows}</tbody></table></body></html>"
        soup = BeautifulSoup(html, "lxml")
        result = scraper._get_html_string(soup)
        assert result == ""

    def test_enough_rows_returns_html_string(self):
        scraper = _make_scraper()
        rows = "".join((f"<tr><td>row {i}</td></tr>" for i in range(7)))
        html = f"<html><body><table><tbody>{rows}</tbody></table></body></html>"
        soup = BeautifulSoup(html, "lxml")
        result = scraper._get_html_string(soup)
        assert result


class TestGetDocDataStringKwarg:
    @pytest.mark.asyncio
    async def test_no_deprecation_warning_for_find(self):
        """Ensure text= was replaced by string= — no BeautifulSoup DeprecationWarning."""
        html = "<html><body><table><tr><td>Situação:</td><td>Vigente</td></tr><tr><td>Assunto:</td><td>Administração</td></tr><tr><td>Links:</td><td><a href='/link.html'>texto</a></td></tr></table></body></html>"
        soup = BeautifulSoup(html, "lxml")
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            soup.find("td", string="Situação:")
            soup.find("td", string="Assunto:")
            import re

            soup.find("td", string=re.compile("Links:"))


class TestFetchHtmlNorm:
    async def test_stores_http_body_instead_of_browser_mhtml(self):
        scraper = _make_scraper()
        meta = (
            "<html><body><table>"
            "<tr><td>Situação:</td><td>Vigente</td></tr>"
            "<tr><td>Assunto:</td><td>Administração</td></tr>"
            "<tr><td>Links:</td><td><a href='M0100099.asp?Hid_IdNorma=1'>Texto</a></td></tr>"
            "</table></body></html>"
        )
        rows = "".join(f"<tr><td>cabeçalho {i}</td></tr>" for i in range(6))
        text = (
            f"<html><body><table><tbody>{rows}<tr><td>Art. 1º Fica instituído o "
            "programa estadual de teste. Art. 2º Esta lei entra em vigor."
            "</td></tr></tbody></table></body></html>"
        )
        scraper.http.request.side_effect = [
            make_mock_response(meta),
            make_mock_response(text),
        ]
        doc = await scraper.fetch(
            {
                "year": 2012,
                "type": "Lei ordinária",
                "title": "Lei ordinária 1 DE 01/01/2012",
                "situation": "Não consta revogação expressa",
                "_html_link": "https://www.al.rs.gov.br/legis/M010/M0100018.asp?Hid_IdNorma=1",
                "document_url": "https://www.al.rs.gov.br/legis/M010/M0100018.asp?Hid_IdNorma=1",
            }
        )
        assert doc.raw == text.encode()
        assert doc.raw_ext == ".html"
        assert (
            doc.document_url
            == "https://www.al.rs.gov.br/legis/M010/M0100099.asp?Hid_IdNorma=1"
        )
        assert "programa estadual de teste" in doc.text_markdown
        assert doc.situation == "Vigente"
        scraper.browser.snapshot.assert_not_called()
