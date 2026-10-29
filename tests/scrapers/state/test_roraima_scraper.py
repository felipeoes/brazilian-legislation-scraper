"""Tests for RoraimaAlerScraper.

Roraima uses SAPLBaseScraper with no overrides — all logic is inherited.
Covers:
- TYPES constant: 10 types with integer IDs
- Class docstring accessible (__doc__ is not None)
- _iterate_situations NOT set (SAPLBaseScraper passes situations={})
- situations is empty dict
- _format_search_url (inherited): correct SAPL API URL construction
- _get_docs_links (inherited):
  - failed request → []
  - no results key → []
  - items without texto_integral → skipped
  - data_vigencia present → situation = DEFAULT_INVALID_SITUATION
  - data_vigencia absent → situation = DEFAULT_VALID_SITUATION
  - valid item → correct doc dict shape
- _process_pdf (inherited base class):
  - _download_and_convert returns falsy markdown → None
  - _download_and_convert returns valid markdown → correct dict shape

Run with:
    .venv/bin/pytest tests/test_roraima_scraper.py -v
"""

from conftest import make_scraper
from parser_assertions import ScraperClassTests, TypesConstantTests

from src.scrapers.states.rr import TYPES, RoraimaAlerScraper


def _make_scraper(**kwargs) -> RoraimaAlerScraper:
    """Instantiate RoraimaAlerScraper with injected dependencies."""
    kwargs.setdefault("subjects", {})
    kwargs.setdefault("_id_to_type", {v: k for k, v in TYPES.items()})
    kwargs.setdefault("_page_size", 100)
    return make_scraper(RoraimaAlerScraper, **kwargs)


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 10
    REQUIRED_KEYS = {
        "Lei Ordinária",
        "Lei Complementar",
        "Constituição Estadual",
        "Emenda à Constituição",
        "Decreto Legislativo",
    }


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = RoraimaAlerScraper
    STATE_NAME = "Roraima"

    def test_situations_empty(self):
        scraper = _make_scraper()
        assert scraper.situations == {}

    def test_subjects_initialized_empty(self):
        scraper = _make_scraper()
        assert scraper.subjects == {}

    def test_id_to_type_built_correctly(self):
        scraper = _make_scraper()
        assert scraper._id_to_type[2] == "Lei Ordinária"
        assert scraper._id_to_type[3] == "Lei Complementar"
        assert len(scraper._id_to_type) == len(TYPES)


class TestFormatSearchUrl:
    def test_includes_tipo_param(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2, 2023)
        assert "tipo=2" in url

    def test_includes_year(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2, 2023)
        assert "ano=2023" in url

    def test_default_page_is_1(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2, 2023)
        assert "page=1" in url

    def test_includes_page_size(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2, 2023)
        assert "page_size=100" in url

    def test_custom_page(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(3, 2020, page=5)
        assert "page=5" in url

    def test_base_url_present(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(1, 2022)
        assert url.startswith("https://sapl.al.rr.leg.br")

    def test_api_path_present(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(1, 2022)
        assert "/api/norma/normajuridica/" in url


class TestFormatYearUrl:
    def test_no_tipo_param(self):
        scraper = _make_scraper()
        url = scraper._format_year_url(2023)
        assert "tipo=" not in url

    def test_includes_year(self):
        scraper = _make_scraper()
        url = scraper._format_year_url(2023)
        assert "ano=2023" in url

    def test_default_page_is_1(self):
        scraper = _make_scraper()
        url = scraper._format_year_url(2023)
        assert "page=1" in url

    def test_includes_page_size(self):
        scraper = _make_scraper()
        url = scraper._format_year_url(2023)
        assert "page_size=100" in url

    def test_custom_page(self):
        scraper = _make_scraper()
        url = scraper._format_year_url(2023, page=5)
        assert "page=5" in url


def _make_api_item(
    doc_id: int = 1,
    texto_integral: str | None = "https://sapl.al.rr.leg.br/media/lei.pdf",
    data_vigencia: str | None = None,
    tipo: int = 2,
    **kwargs,
) -> dict:
    return {
        "id": doc_id,
        "numero": str(doc_id),
        "__str__": f"Lei Ordinária {doc_id}/2023",
        "link_detail_backend": f"/norma/{doc_id}",
        "ementa": "Ementa da lei.",
        "assuntos": [],
        "data": "2023-01-01",
        "ano": 2023,
        "esfera_federacao": None,
        "veiculo_publicacao": None,
        "texto_integral": texto_integral,
        "data_vigencia": data_vigencia,
        "tipo": tipo,
        **kwargs,
    }
