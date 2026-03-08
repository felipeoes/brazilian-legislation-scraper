"""Tests for AcreLegisScraper.

Covers:
- TYPES constant completeness (including Emendas Constitucionais)
- _get_docs_links HTML parsing (title extraction, year regex, summary, link)
- _get_docs_links year regex fallback for malformed titles
- _clean_acre_html content extraction (body-law, exportacao fallback)
- _clean_acre_html layout row and doe-span removal
- _clean_acre_html link unwrapping via base helper
- _scrape_type constitution-once-only guard
- _scrape_type returns empty for missing year/type combo
- _prefetch_all_links buckets docs by year and skips Constituição Estadual
- scrape() year filtering from prefetched keys
"""

import re
from collections import defaultdict
from unittest.mock import AsyncMock, patch

import pytest
from bs4 import BeautifulSoup

from src.scraper.state_legislation.acre import TYPES, AcreLegisScraper

from base_tests import TypesConstantTests
from conftest import make_base_scraper


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> AcreLegisScraper:
    """Instantiate AcreLegisScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        AcreLegisScraper,
        "https://legis.ac.gov.br/principal",
        "ACRE",
        TYPES,
        situations={"Não consta revogação expressa": "Não consta revogação expressa"},
        year_regex=re.compile(r"\d{4}"),
        _prefetched_docs=defaultdict(lambda: defaultdict(list)),
        years=list(range(2000, 2026)),
        year_start=2000,
        year_end=2025,
        **kwargs,
    )


def _listing_html(norm_type_id: str, rows: list[tuple[str, str, str]]) -> str:
    """Build a minimal listing page HTML with the given rows.

    Each row is a tuple of (title, href, summary).
    """
    row_html = ""
    for title, href, summary in rows:
        row_html += f"""
        <tr class="visaoQuadrosTr">
            <td><a href="{href}">{title}</a></td>
            <td>{summary}</td>
        </tr>"""
    return f"""
    <html><body>
        <div id="{norm_type_id}">
            <table>{row_html}</table>
        </div>
    </body></html>"""


# ---------------------------------------------------------------------------
# TYPES completeness
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 5
    REQUIRED_KEYS = {
        "Lei Ordinária",
        "Lei Complementar",
        "Constituição Estadual",
        "Decreto",
        "Emenda Constitucional",
    }
    REQUIRE_INT_VALUES = False

    def test_emenda_constitucional_maps_to_emendas(self):
        assert TYPES["Emenda Constitucional"] == "emendas"

    def test_constituicao_maps_to_detalhar_constituicao(self):
        assert TYPES["Constituição Estadual"] == "detalhar_constituicao"


# ---------------------------------------------------------------------------
# _get_docs_links
# ---------------------------------------------------------------------------


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_parses_title_link_summary_year(self):
        scraper = _make_scraper()
        rows = [
            (
                "Lei Ordinária nº 1.234, de 15 de março de 2022",
                "/detalhar/999",
                "Dispõe sobre X.",
            )
        ]
        soup = BeautifulSoup(_listing_html("lei_ordinarias", rows), "html.parser")

        docs = await scraper._get_docs_links(soup, "lei_ordinarias")

        assert len(docs) == 1
        doc = docs[0]
        assert doc["title"] == "Lei Ordinária nº 1.234, de 15 de março de 2022"
        assert doc["html_link"] == "/detalhar/999"
        assert doc["summary"] == "Dispõe sobre X."
        assert doc["year"] == "2022"

    @pytest.mark.asyncio
    async def test_parses_multiple_rows(self):
        scraper = _make_scraper()
        rows = [
            ("Lei nº 1, de 1963", "/detalhar/1", "Cria X."),
            ("Lei nº 2, de 1964", "/detalhar/2", "Cria Y."),
        ]
        soup = BeautifulSoup(_listing_html("lei_ordinarias", rows), "html.parser")
        docs = await scraper._get_docs_links(soup, "lei_ordinarias")
        assert len(docs) == 2
        assert docs[0]["year"] == "1963"
        assert docs[1]["year"] == "1964"

    @pytest.mark.asyncio
    async def test_year_fallback_on_malformed_title(self):
        """Should return '0000' instead of raising AttributeError."""
        scraper = _make_scraper()
        rows = [("Lei sem data", "/detalhar/42", "Ementa qualquer.")]
        soup = BeautifulSoup(_listing_html("lei_ordinarias", rows), "html.parser")

        docs = await scraper._get_docs_links(soup, "lei_ordinarias")

        assert docs[0]["year"] == "0000"

    @pytest.mark.asyncio
    async def test_emendas_section_parsed_correctly(self):
        scraper = _make_scraper()
        rows = [
            (
                "Emenda Constitucional nº 77, de 2023",
                "/detalhar_emendas/77",
                "Altera a Constituição.",
            )
        ]
        soup = BeautifulSoup(_listing_html("emendas", rows), "html.parser")

        docs = await scraper._get_docs_links(soup, "emendas")

        assert len(docs) == 1
        assert docs[0]["year"] == "2023"


# ---------------------------------------------------------------------------
# _clean_acre_html
# ---------------------------------------------------------------------------


class TestCleanAcreHtml:
    def _make_detail_page(
        self, container_id: str, body: str, *, extra: str = ""
    ) -> str:
        return f"""<html><body>
            <div id="{container_id}">
                <div class="row">LAYOUT JUNK</div>
                <span id="texto_publicado_doe">DOE DATE</span>
                {extra}
                {body}
            </div>
        </body></html>"""

    def test_extracts_body_law_content(self):
        scraper = _make_scraper()
        html = self._make_detail_page("body-law", "<p>Conteúdo da lei.</p>")
        soup = BeautifulSoup(html, "html.parser")
        result = scraper._clean_acre_html(soup)
        assert "Conteúdo da lei." in result

    def test_falls_back_to_exportacao_when_no_body_law(self):
        scraper = _make_scraper()
        html = self._make_detail_page("exportacao", "<p>Texto via exportacao.</p>")
        soup = BeautifulSoup(html, "html.parser")
        result = scraper._clean_acre_html(soup)
        assert "Texto via exportacao." in result

    def test_returns_empty_string_when_no_container(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(
            "<html><body><p>No relevant div.</p></body></html>", "html.parser"
        )
        result = scraper._clean_acre_html(soup)
        assert result == ""

    def test_removes_layout_rows(self):
        scraper = _make_scraper()
        html = self._make_detail_page("body-law", "<p>Lei text.</p>")
        soup = BeautifulSoup(html, "html.parser")
        result = scraper._clean_acre_html(soup)
        assert "LAYOUT JUNK" not in result

    def test_removes_doe_span(self):
        scraper = _make_scraper()
        html = self._make_detail_page("body-law", "<p>Lei text.</p>")
        soup = BeautifulSoup(html, "html.parser")
        result = scraper._clean_acre_html(soup)
        assert "DOE DATE" not in result

    def test_unwraps_anchor_tags(self):
        """Links should be unwrapped (text kept, <a> tag removed)."""
        scraper = _make_scraper()
        html = self._make_detail_page(
            "body-law", '<p>Veja <a href="/detalhar/1">art. 1º</a>.</p>'
        )
        soup = BeautifulSoup(html, "html.parser")
        result = scraper._clean_acre_html(soup)
        assert "art. 1º" in result
        assert "<a " not in result

    def test_does_not_include_body_law_id_twice(self):
        """body-law fallback logic: body-law preferred over exportacao."""
        scraper = _make_scraper()
        html = """<html><body>
            <div id="body-law"><p>Primary content.</p></div>
            <div id="exportacao"><p>Secondary content.</p></div>
        </body></html>"""
        soup = BeautifulSoup(html, "html.parser")
        result = scraper._clean_acre_html(soup)
        assert "Primary content." in result
        # exportacao content should not be present (body-law was found first)
        assert "Secondary content." not in result


# ---------------------------------------------------------------------------
# _scrape_type
# ---------------------------------------------------------------------------


class TestScrapeType:
    @pytest.mark.asyncio
    async def test_constitution_scraped_only_once(self):
        scraper = _make_scraper()
        scraper._scraped_constitution = True  # already scraped

        result = await scraper._scrape_type(
            "Constituição Estadual", "detalhar_constituicao", 2000
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_constitution_calls_fetch_and_save(self):
        scraper = _make_scraper()
        fake_doc = {"title": "Constituição Estadual", "year": 2025}

        with patch.object(
            scraper,
            "_fetch_and_save_constitution",
            new=AsyncMock(return_value=fake_doc),
        ) as mock_fetch:
            result = await scraper._scrape_type(
                "Constituição Estadual", "detalhar_constituicao", 2000
            )

        mock_fetch.assert_awaited_once()
        call_kwargs = mock_fetch.call_args.kwargs
        assert call_kwargs["title"] == "Constituição Estadual"
        assert "legis.ac.gov.br/detalhar_constituicao" in call_kwargs["url"]
        assert result == [fake_doc]

    @pytest.mark.asyncio
    async def test_constitution_returns_empty_when_fetch_fails(self):
        scraper = _make_scraper()
        with patch.object(
            scraper, "_fetch_and_save_constitution", new=AsyncMock(return_value=None)
        ):
            result = await scraper._scrape_type(
                "Constituição Estadual", "detalhar_constituicao", 2000
            )
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_for_year_with_no_docs(self):
        scraper = _make_scraper()
        # No docs registered for year 1990
        result = await scraper._scrape_type("Lei Ordinária", "lei_ordinarias", 1990)
        assert result == []

    @pytest.mark.asyncio
    async def test_delegates_to_process_documents_for_regular_type(self):
        scraper = _make_scraper()
        fake_docs = [
            {
                "title": "Lei nº 1, de 2020",
                "year": "2020",
                "summary": "X",
                "html_link": "/1",
            }
        ]
        scraper._prefetched_docs[2020]["Lei Ordinária"] = fake_docs

        with patch.object(
            scraper, "_process_documents", new=AsyncMock(return_value=fake_docs)
        ) as mock_proc:
            result = await scraper._scrape_type("Lei Ordinária", "lei_ordinarias", 2020)

        mock_proc.assert_awaited_once()
        assert result == fake_docs


# ---------------------------------------------------------------------------
# _prefetch_all_links
# ---------------------------------------------------------------------------


class TestPrefetchAllLinks:
    @pytest.mark.asyncio
    async def test_buckets_docs_by_year(self):
        scraper = _make_scraper()
        # Build a minimal soup with only the 'lei_ordinarias' section
        page_html = _listing_html(
            "lei_ordinarias",
            [
                ("Lei nº 1, de 2020", "/detalhar/1", "Ementa 1."),
                ("Lei nº 2, de 2021", "/detalhar/2", "Ementa 2."),
            ],
        )
        # Also add stubs for other non-constitution types so the scraper doesn't error
        for div_id in ["lei_complementares", "lei_decretos", "emendas"]:
            page_html = page_html.replace(
                "</body>", f'<div id="{div_id}"><table></table></div></body>'
            )

        soup = BeautifulSoup(page_html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)

        await scraper._prefetch_all_links()

        assert 2020 in scraper._prefetched_docs
        assert 2021 in scraper._prefetched_docs
        assert len(scraper._prefetched_docs[2020]["Lei Ordinária"]) == 1
        assert len(scraper._prefetched_docs[2021]["Lei Ordinária"]) == 1

    @pytest.mark.asyncio
    async def test_skips_constituicao_estadual(self):
        scraper = _make_scraper()
        # Soup with no sections at all (empty tables for each non-constitution type)
        page_html = "<html><body>"
        for div_id in [
            "lei_ordinarias",
            "lei_complementares",
            "lei_decretos",
            "emendas",
        ]:
            page_html += f'<div id="{div_id}"><table></table></div>'
        page_html += "</body></html>"

        soup = BeautifulSoup(page_html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)

        await scraper._prefetch_all_links()

        # Constituição Estadual should never appear in _prefetched_docs
        for year_data in scraper._prefetched_docs.values():
            assert "Constituição Estadual" not in year_data

    @pytest.mark.asyncio
    async def test_handles_failed_soup_gracefully(self):
        scraper = _make_scraper()
        scraper.request_service.get_soup = AsyncMock(return_value=None)

        # Should not raise; _prefetched_docs remains empty
        await scraper._prefetch_all_links()
        assert len(scraper._prefetched_docs) == 0


# ---------------------------------------------------------------------------
# scrape() year filtering
# ---------------------------------------------------------------------------


class TestScrapeYearFiltering:
    @pytest.mark.asyncio
    async def test_years_filtered_to_year_start_end(self):
        scraper = _make_scraper()
        scraper.year_start = 2020
        scraper.year_end = 2022

        # Inject pre-fetched docs spanning a wider range
        for yr in [2019, 2020, 2021, 2022, 2023]:
            scraper._prefetched_docs[yr]["Lei Ordinária"].append(
                {
                    "title": f"Lei {yr}",
                    "year": str(yr),
                    "summary": "",
                    "html_link": f"/{yr}",
                }
            )

        # Patch super().scrape() so we don't trigger I/O; just inspect self.years
        with patch(
            "src.scraper.base.scraper.BaseScraper.scrape",
            new=AsyncMock(return_value=0),
        ):
            with patch.object(scraper, "_prefetch_all_links", new=AsyncMock()):
                await scraper.scrape()

        assert scraper.years == [2020, 2021, 2022]
        assert 2019 not in scraper.years
        assert 2023 not in scraper.years
