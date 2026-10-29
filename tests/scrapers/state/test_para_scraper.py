"Parser and metadata regression tests for para_scraper."

from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.pa import (
    _FEDERAL_TYPES,
    _TYPE_NORMALIZE,
    SITUATIONS,
    TYPES,
    ParaAlepaScraper,
)


def _make_scraper(**kwargs) -> ParaAlepaScraper:
    """Instantiate ParaAlepaScraper with injected dependencies."""
    import re

    return make_scraper(
        ParaAlepaScraper,
        regex_total_count=re.compile("Total de Registros:\\s+(\\d+)"),
        **kwargs,
    )


def _make_listing_html(count: int = 3, doc_type: str = "Decreto Estadual") -> bytes:
    """Build a minimal listing page with `count` document rows including type/number."""
    rows = ""
    for i in range(1, count + 1):
        rows += f'\n        <tr>\n          <td>\n            <strong>Tipo da Lei:</strong> {doc_type}\n            <strong>Nº da Lei:</strong> 00{i} / 2000\n          </td>\n          <td><a href="/pdf/doc{i}.pdf">Ementa {i}</a></td>\n        </tr>'
    return f"<html><body>\n        <p>Total de Registros: {count}</p>\n        <table><tr><td>header</td></tr>{rows}</table>\n    </body></html>".encode()


def _make_mixed_listing_html() -> bytes:
    """Build a listing page with multiple types including a federal type."""
    rows = '\n    <tr>\n      <td>\n        <strong>Tipo da Lei:</strong> Decreto Estadual\n        <strong>Nº da Lei:</strong> 001 / 2020\n      </td>\n      <td><a href="/pdf/dec1.pdf">Ementa decreto</a></td>\n    </tr>\n    <tr>\n      <td>\n        <strong>Tipo da Lei:</strong> Resoluções\n        <strong>Nº da Lei:</strong> 010 / 2020\n      </td>\n      <td><a href="/pdf/res1.pdf">Ementa resolução</a></td>\n    </tr>\n    <tr>\n      <td>\n        <strong>Tipo da Lei:</strong> Emendas Constitucionais Federais\n        <strong>Nº da Lei:</strong> 100 / 2020\n      </td>\n      <td><a href="/pdf/fed1.pdf">Ementa federal</a></td>\n    </tr>\n    '
    return f"<html><body>\n        <p>Total de Registros: 3</p>\n        <table><tr><td>header</td></tr>{rows}</table>\n    </body></html>".encode()


def _make_empty_listing_html() -> bytes:
    return (
        b"<html><body><p>Total de Registros:                      0</p></body></html>"
    )


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 6
    REQUIRED_KEYS = {"Decreto Estadual", "Lei Ordinária", "Lei Complementar"}
    REQUIRE_INT_VALUES = True

    def test_decreto_estadual_id(self):
        assert TYPES["Decreto Estadual"] == 2


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_nao_consta_key_present(self):
        assert "Não consta" in SITUATIONS


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = ParaAlepaScraper
    STATE_NAME = "Para"

    def test_situations_empty_in_instance(self):
        scraper = _make_scraper()
        assert scraper.situations == {}


class TestNormalizeType:
    def test_canonical_name_unchanged(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("Decreto Estadual") == "Decreto Estadual"

    def test_plural_resolucoes_normalized(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("Resoluções") == "Resolução"

    def test_emendas_estaduais_normalized(self):
        scraper = _make_scraper()
        assert (
            scraper._normalize_type("Emendas Constitucionais Estaduais")
            == "Emenda Constitucional"
        )

    def test_federal_type_returns_none(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("Emendas Constitucionais Federais") is None

    def test_unknown_type_passthrough(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("Tipo Desconhecido") == "Tipo Desconhecido"

    def test_empty_string_returns_empty(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("") == ""

    def test_none_returns_empty(self):
        scraper = _make_scraper()
        assert scraper._normalize_type(None) == ""

    def test_case_insensitive(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("decreto estadual") == "Decreto Estadual"

    def test_extra_whitespace_cleaned(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("  Decreto   Estadual  ") == "Decreto Estadual"

    def test_all_types_in_normalize_map(self):
        for name in TYPES:
            assert _TYPE_NORMALIZE.get(name.casefold()) == name

    def test_federal_types_set(self):
        assert "emendas constitucionais federais" in _FEDERAL_TYPES


class TestBuildParams:
    def test_returns_correct_keys(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert "anoLei" in params
        assert "tipo" in params
        assert "verifica" in params
        assert "button" in params

    def test_default_tipo_is_empty(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert params["tipo"] == ""

    def test_year_set(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert params["anoLei"] == 2000

    def test_explicit_tipo(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000, norm_type_id=2)
        assert params["tipo"] == 2

    def test_verifica_is_1(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert params["verifica"] == 1

    def test_no_shared_state_mutation(self):
        scraper = _make_scraper()
        p1 = scraper._build_params(2000, norm_type_id=2)
        p2 = scraper._build_params(2001, norm_type_id=3)
        assert p1["anoLei"] != p2["anoLei"]
        assert p1["tipo"] != p2["tipo"]
