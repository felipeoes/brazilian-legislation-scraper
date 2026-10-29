"Parser and metadata regression tests for rio_de_janeiro_scraper."

from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.rj import SITUATIONS, TYPES, RJAlerjScraper


def _make_scraper(**kwargs) -> RJAlerjScraper:
    """Instantiate RJAlerjScraper with injected dependencies."""
    return make_scraper(
        RJAlerjScraper,
        lotus_base_url="https://www3.alerj.rj.gov.br/lotus_notes/default.asp",
        params={"OpenForm": "", "Start": 1, "Count": 500},
        **kwargs,
    )


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


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = True


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = RJAlerjScraper
    STATE_NAME = "Rio de Janeiro"


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


class TestGetDocsHtmlLinks:
    def _make_soup(self, rows_html: str) -> BeautifulSoup:
        return BeautifulSoup(f"<table>{rows_html}</table>", "lxml")

    def _make_row(
        self, number: str, year: int, author: str = "Gov", summary: str = "Sumário"
    ) -> str:
        return f'<tr><td><a data-role="/contlei.nsf/doc?Open" href="#">{number}</a></td><td>{year}</td><td>{author}</td><td>{summary}</td></tr>'

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


class TestCleanConstitutionSectionSoup:
    def test_removes_text_titulo_string_kwarg(self):
        """Ensure deprecated text= kwarg was replaced by string= for BeautifulSoup.find_all."""
        scraper = _make_scraper()
        html = (
            "<html><body><p>Texto do Título</p><p>Conteúdo importante</p></body></html>"
        )
        soup = BeautifulSoup(html, "lxml")
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            scraper._clean_constitution_section_soup(soup)
        texts = [tag.get_text() for tag in soup.find_all("p")]
        assert "Texto do Título" not in texts

    def test_removes_alert_warning_divs(self):
        scraper = _make_scraper()
        html = '<html><body><div class="alert alert-warning">Warning!</div><p>Conteúdo</p></body></html>'
        soup = BeautifulSoup(html, "lxml")
        scraper._clean_constitution_section_soup(soup)
        assert soup.find("div", class_="alert alert-warning") is None

    def test_removes_barra_botoes_div(self):
        scraper = _make_scraper()
        html = (
            '<html><body><div id="barraBotoes">Botões</div><p>Texto</p></body></html>'
        )
        soup = BeautifulSoup(html, "lxml")
        scraper._clean_constitution_section_soup(soup)
        assert soup.find("div", id="barraBotoes") is None
