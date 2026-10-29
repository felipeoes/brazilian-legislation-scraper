"""Shared test mixins and helpers for scraper test suites.

Usage example::

    from base_tests import TypesConstantTests, ScraperClassTests, SituationsConstantTests

    class TestTypesConstant(TypesConstantTests):
        TYPES = TYPES  # module-level TYPES dict
        EXPECTED_COUNT = 11
        REQUIRED_KEYS = {"Lei Ordinária", "Constituição Estadual"}
        REQUIRE_INT_VALUES = True  # set False when values are strings/dicts

    class TestSituationsConstant(SituationsConstantTests):
        SITUATIONS = SITUATIONS
        EXPECTED_TYPE = dict
        EXPECTED_EMPTY = True

    class TestClassAttributes(ScraperClassTests):
        SCRAPER_CLS = MyScraper
        STATE_NAME = "Meu Estado"
"""

from __future__ import annotations


class TypesConstantTests:
    """Mixin for testing a scraper's module-level TYPES constant.

    Subclasses must set:
        TYPES          – the TYPES dict (or list) to test
        EXPECTED_COUNT – expected number of entries
        REQUIRED_KEYS  – set of type-name strings that must be present
        REQUIRE_INT_VALUES – whether all values should be ``int`` (default True)
    """

    TYPES: dict | list | None = None
    EXPECTED_COUNT: int | None = None
    REQUIRED_KEYS: set[str] = set()
    REQUIRE_INT_VALUES: bool = True

    def test_expected_count(self):
        if self.EXPECTED_COUNT is None:
            return
        assert len(self.TYPES) == self.EXPECTED_COUNT, (
            f"Expected {self.EXPECTED_COUNT} types, got {len(self.TYPES)}"
        )

    def test_required_keys_present(self):
        keys = (
            set(self.TYPES.keys()) if isinstance(self.TYPES, dict) else set(self.TYPES)
        )
        missing = self.REQUIRED_KEYS - keys
        assert not missing, f"Missing required type keys: {missing}"

    def test_values_are_distinct(self):
        if not isinstance(self.TYPES, dict):
            return
        vals = list(self.TYPES.values())
        # Handle unhashable values (e.g. dicts) by comparing string representations
        str_vals = [str(v) for v in vals]
        assert len(set(str_vals)) == len(str_vals), "TYPES values are not unique"

    def test_values_are_integers(self):
        if not self.REQUIRE_INT_VALUES or not isinstance(self.TYPES, dict):
            return
        non_int = {k: v for k, v in self.TYPES.items() if not isinstance(v, int)}
        assert not non_int, f"Non-integer TYPES values: {non_int}"


class SituationsConstantTests:
    """Mixin for testing a scraper's module-level SITUATIONS constant.

    Subclasses must set:
        SITUATIONS     – the SITUATIONS dict or list to test
        EXPECTED_TYPE  – expected type (``dict`` or ``list``), default ``dict``
        EXPECTED_EMPTY – whether SITUATIONS should be empty (default False)
    """

    SITUATIONS: dict | list | None = None
    EXPECTED_TYPE: type = dict
    EXPECTED_EMPTY: bool = False

    def test_situations_exists(self):
        assert self.SITUATIONS is not None

    def test_situations_has_expected_type(self):
        assert isinstance(self.SITUATIONS, self.EXPECTED_TYPE), (
            f"Expected {self.EXPECTED_TYPE.__name__}, got {type(self.SITUATIONS).__name__}"
        )

    def test_situations_emptiness(self):
        if self.EXPECTED_EMPTY:
            assert len(self.SITUATIONS) == 0, "Expected SITUATIONS to be empty"
        else:
            assert len(self.SITUATIONS) > 0, "Expected SITUATIONS to be non-empty"


class ScraperClassTests:
    """Mixin for basic class-level attribute tests shared across scrapers.

    Subclasses must set:
        SCRAPER_CLS              – the scraper class under test
        STATE_NAME               – substring expected in ``__doc__`` (case-insensitive)
        EXPECT_ITERATE_SITUATIONS – expected value of ``_iterate_situations`` (default False)
    """

    SCRAPER_CLS = None
    STATE_NAME: str = ""
    EXPECT_ITERATE_SITUATIONS: bool = False

    def test_docstring_is_accessible(self):
        assert self.SCRAPER_CLS.__doc__ is not None, "Missing class docstring"
        if self.STATE_NAME:
            assert self.STATE_NAME.lower() in self.SCRAPER_CLS.__doc__.lower(), (
                f"Expected '{self.STATE_NAME}' in docstring"
            )

    def test_iterate_situations(self):
        actual = getattr(self.SCRAPER_CLS, "_iterate_situations", False)
        assert actual == self.EXPECT_ITERATE_SITUATIONS, (
            f"Expected _iterate_situations={self.EXPECT_ITERATE_SITUATIONS}, got {actual}"
        )


class FormatSearchUrlTests:
    """Mixin for ``_format_search_url`` smoke tests.

    Subclasses must set:
        MAKE_SCRAPER  – callable returning a scraper instance
        SAMPLE_ARGS   – tuple of (norm_type_id, year) for the URL builder
        BASE_URL      – expected URL prefix
    """

    MAKE_SCRAPER = None
    SAMPLE_ARGS: tuple = ("1", 2023)
    BASE_URL: str = ""

    def _url(self):
        scraper = self.MAKE_SCRAPER()
        return scraper._format_search_url(*self.SAMPLE_ARGS)

    def test_starts_with_base_url(self):
        assert self._url().startswith(self.BASE_URL)

    def test_includes_year(self):
        assert str(self.SAMPLE_ARGS[1]) in self._url()

    def test_includes_type_id(self):
        assert str(self.SAMPLE_ARGS[0]) in self._url()
