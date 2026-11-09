"Parser and metadata regression tests for camara_dep_scraper."

from bs4 import BeautifulSoup
from conftest import make_scraper

from src.scrapers.federal.camara import TYPES, CamaraDepScraper

TEST_YEAR = 2020
TEST_TYPE = "Decreto"
TEST_TYPE_ID = TYPES[TEST_TYPE]
PER_PAGE = 20


def build_unit_scraper():
    return make_scraper(CamaraDepScraper)


def test_types_dict_completeness():
    """TYPES must contain 'Decreto' and other key legislative types."""
    required = [
        "Decreto",
        "Lei Ordinária",
        "Lei Complementar",
        "Decreto-Lei",
        "Emenda Constitucional",
        "Medida Provisória",
        "Decreto Legislativo",
        "Ato da Mesa",
        "Ato do Presidente Sem Número",
        "Ato da Presidência Sem Número",
        "Decisão da Mesa Sem Número",
        "Resolução",
        "Ato",
        "Ato Sem Número",
    ]
    for t in required:
        assert t in TYPES, f"TYPES missing required key: {t!r}"


class TestTypeResolution:
    def test_resolve_norm_type_prefers_metadata_slug(self):
        scraper = build_unit_scraper()
        assert (
            scraper._resolve_norm_type(
                "Lei nº 15.323, de 6 de Janeiro de 2026",
                metadata_url="https://www2.camara.leg.br/legin/fed/lei/2026/lei-15323-6-janeiro-2026-798626-norma-pl.html",
            )
            == "Lei Ordinária"
        )

    def test_resolve_norm_type_maps_slug_only_titles(self):
        scraper = build_unit_scraper()
        assert (
            scraper._resolve_norm_type(
                "Carta de Lei  de 29 de Novembro de 1808",
                metadata_url="https://www2.camara.leg.br/legin/fed/carlei_sn/anterioresa1824/cartadelei-40273-29-novembro-1808-572462-norma-pe.html",
            )
            == "Carta de Lei"
        )


class TestListingParsing:
    def test_parse_export_documents_extracts_metadata(self):
        scraper = build_unit_scraper()
        soup = BeautifulSoup(
            '\n            <html><body>\n              <div id="impressaoPDF">\n                <ul class="busca-resultados">\n                  <li>\n                    <a href="https://www2.camara.leg.br/legin/fed/decret/example-norma.html">\n                      Decreto nº 1\n                    </a>\n                    <p><span class="bold color-gray">Ementa:</span> Dispõe sobre teste.</p>\n                    <p><span class="bold color-gray">Situação: </span>Não consta revogação expressa</p>\n                  </li>\n                </ul>\n              </div>\n            </body></html>\n            ',
            "lxml",
        )
        docs = scraper._parse_export_documents(soup)
        assert docs == [
            {
                "title": "Decreto nº 1",
                "summary": "Dispõe sobre teste.",
                "metadata_url": "https://www2.camara.leg.br/legin/fed/decret/example-norma.html",
                "situation": "Não consta revogação expressa",
            }
        ]


class TestExtractTotalResults:
    """Unit tests for CamaraDepScraper._extract_total_results."""

    @staticmethod
    def _wrap(inner_html: str) -> BeautifulSoup:
        return BeautifulSoup(f"<html><body>{inner_html}</body></html>", "lxml")

    def test_extracts_count_from_well_formed_div(self):
        soup = self._wrap(
            '<div class="busca-info__resultado busca-info__resultado--informado">  Foram encontrados 42</div>'
        )
        assert CamaraDepScraper._extract_total_results(soup) == 42

    def test_returns_none_when_div_missing(self):
        soup = self._wrap("<p>nothing here</p>")
        assert CamaraDepScraper._extract_total_results(soup) is None

    def test_returns_none_when_text_has_no_number(self):
        soup = self._wrap(
            '<div class="busca-info__resultado busca-info__resultado--informado">  Nenhum documento encontrado</div>'
        )
        assert CamaraDepScraper._extract_total_results(soup) is None

    def test_extracts_large_count(self):
        soup = self._wrap(
            '<div class="busca-info__resultado busca-info__resultado--informado">  Encontrados 12345</div>'
        )
        assert CamaraDepScraper._extract_total_results(soup) == 12345


class TestParseListingPageDocuments:
    """Unit tests for CamaraDepScraper._parse_listing_page_documents."""

    def test_parses_single_result_item(self):
        scraper = build_unit_scraper()
        soup = BeautifulSoup(
            '\n            <ul>\n              <li class="busca-resultados__item">\n                <h3 class="busca-resultados__cabecalho">\n                  <a href="https://www2.camara.leg.br/legin/fed/decret/2020/norma.html">\n                    Decreto nº 10.000\n                  </a>\n                </h3>\n                <p class="busca-resultados__descricao js-fade-read-more">\n                  Ementa: Dispõe sobre coisas.\n                </p>\n                <p class="busca-resultados__situacao">\n                  Situação: Não consta revogação expressa\n                </p>\n              </li>\n            </ul>\n            ',
            "lxml",
        )
        docs = scraper._parse_listing_page_documents(soup)
        assert len(docs) == 1
        assert docs[0]["title"] == "Decreto nº 10.000"
        assert docs[0]["summary"] == "Dispõe sobre coisas."
        assert docs[0]["situation"] == "Não consta revogação expressa"
        assert docs[0]["metadata_url"].endswith("norma.html")

    def test_skips_items_without_heading(self):
        scraper = build_unit_scraper()
        soup = BeautifulSoup(
            '\n            <ul>\n              <li class="busca-resultados__item">\n                <p>No heading here</p>\n              </li>\n            </ul>\n            ',
            "lxml",
        )
        assert scraper._parse_listing_page_documents(soup) == []

    def test_skips_items_without_anchor(self):
        scraper = build_unit_scraper()
        soup = BeautifulSoup(
            '\n            <ul>\n              <li class="busca-resultados__item">\n                <h3 class="busca-resultados__cabecalho">\n                  Plain text, no link\n                </h3>\n              </li>\n            </ul>\n            ',
            "lxml",
        )
        assert scraper._parse_listing_page_documents(soup) == []

    def test_handles_missing_summary_and_situation(self):
        scraper = build_unit_scraper()
        soup = BeautifulSoup(
            '\n            <ul>\n              <li class="busca-resultados__item">\n                <h3 class="busca-resultados__cabecalho">\n                  <a href="https://example.com/doc">Decreto nº 5</a>\n                </h3>\n              </li>\n            </ul>\n            ',
            "lxml",
        )
        docs = scraper._parse_listing_page_documents(soup)
        assert len(docs) == 1
        assert docs[0]["summary"] == ""
        assert docs[0]["situation"] == ""

    def test_parses_multiple_items(self):
        scraper = build_unit_scraper()
        soup = BeautifulSoup(
            '\n            <ul>\n              <li class="busca-resultados__item">\n                <h3 class="busca-resultados__cabecalho">\n                  <a href="https://example.com/d1">Lei nº 1</a>\n                </h3>\n              </li>\n              <li class="busca-resultados__item">\n                <h3 class="busca-resultados__cabecalho">\n                  <a href="https://example.com/d2">Lei nº 2</a>\n                </h3>\n              </li>\n            </ul>\n            ',
            "lxml",
        )
        docs = scraper._parse_listing_page_documents(soup)
        assert len(docs) == 2
        assert docs[0]["title"] == "Lei nº 1"
        assert docs[1]["title"] == "Lei nº 2"
