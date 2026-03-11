"""Tests for BahiaLegislaScraper.

Covers:
- TYPES constant completeness (16 canonical types) and corrected IDs
- SITUATIONS module-level dict preserved for downstream consumers
- _build_search_url with and without categoria[]
- _normalize_type maps site labels from listing/document pages to canonical names
- _get_docs_links parses year-only listings and extracts type from Categoria column
- _get_doc_data resume skip, fetch failure, missing body, revogado detection,
  false-positive guard for annex notes, category normalization from document page,
  style/script stripping before markdown conversion, invalid markdown handling,
  correct result dict shape
- _scrape_year fetches one unfiltered listing per year, reuses page 0 soup, and
  passes document year/type through to _process_documents

Integration (live site):
- test_get_docs_links_year_only_returns_mixed_types
- test_get_docs_links_constituicao_1989_returns_results
- test_get_doc_data_year_only_returns_valid_markdown

Run with:
    .venv/bin/pytest tests/test_bahia_scraper.py -v
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from bs4 import BeautifulSoup

from src.scraper.state_legislation.bahia import SITUATIONS, TYPES, BahiaLegislaScraper

from base_tests import TypesConstantTests, ScraperClassTests, SituationsConstantTests
from conftest import (
    make_base_scraper,
    assert_resume_skips,
    assert_fetch_failure_saves_error,
)


def _make_scraper(**kwargs) -> BahiaLegislaScraper:
    """Instantiate BahiaLegislaScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        BahiaLegislaScraper,
        "https://www.legislabahia.ba.gov.br",
        "BAHIA",
        TYPES,
        situations={},
        **kwargs,
    )


def _make_listing_soup(
    rows: list[tuple[str, str, str]],
    *,
    last_page_index: int | None = None,
) -> BeautifulSoup:
    """Build a Bahia listing table from (href, title, category) rows."""
    trs = ""
    for href, title, category in rows:
        trs += (
            f'<tr><td><b>{title}</b><a href="{href}">{title}</a></td>'
            f"<td>{category}</td></tr>"
        )

    pagination = ""
    if last_page_index is not None:
        pagination = (
            '<ul class="pagination js-pager__items">'
            f'<li class="pager__item pager__item--last"><a href="?page={last_page_index}">'
            "Ultimo</a></li></ul>"
        )

    return BeautifulSoup(
        f"<html><body><table><tbody>{trs}</tbody></table>{pagination}</body></html>",
        "html.parser",
    )


def _make_doc_soup(
    *,
    category: str = "Leis Ordinárias",
    body_html: str = "<p>" + "Texto da lei. " * 30 + "</p>",
    revogado_span: bool = False,
    revogado_div_text: str = "",
) -> bytes:
    """Build a minimal document page for Bahia."""
    revogado_span_html = (
        '<span class="revogado">revogado</span>' if revogado_span else ""
    )
    revogado_div_html = (
        f'<div class="alteracao">{revogado_div_text}</div>' if revogado_div_text else ""
    )
    return f"""<html><body>
        <div class="field--name-field-categoria-doc"><div class="field--item">{category}</div></div>
        <div class="field--name-field-numero-doc"><div class="field--item">001</div></div>
        <div class="field--name-field-data-doc"><div class="field--item">2022-01-01</div></div>
        <div class="field--name-field-data-de-publicacao-no-doe"><div class="field--item">2022-01-05</div></div>
        <div class="field--name-field-ementa"><div class="field--item">Ementa da lei.</div></div>
        <div class="field--name-body">
            {revogado_span_html}
            {revogado_div_html}
            {body_html}
        </div>
    </body></html>""".encode()


def _make_response(body: bytes) -> MagicMock:
    resp = MagicMock()
    resp.__bool__ = lambda s: True
    resp.read = AsyncMock(return_value=body)
    return resp


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 16
    REQUIRED_KEYS = {"Lei Complementar", "Constituição Estadual Atual 1989"}
    REQUIRE_INT_VALUES = True


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = True


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = BahiaLegislaScraper
    STATE_NAME = "Bahia"

    def test_situations_is_empty_in_instance(self):
        scraper = _make_scraper()
        assert scraper.situations == {}

    def test_revogado_regex_set(self):
        assert hasattr(BahiaLegislaScraper, "_REVOGADO_RE")


class TestNormalizeType:
    def test_maps_plural_site_label(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("Leis Ordinárias") == "Lei Ordinária"

    def test_maps_decretos_numerados_to_decreto(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("Decretos Numerados") == "Decreto"

    def test_unknown_type_returns_cleaned_label(self):
        scraper = _make_scraper()
        assert scraper._normalize_type("  Tipo   Especial  ") == "Tipo Especial"


class TestBuildSearchUrl:
    def test_url_contains_categoria_when_provided(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(7, 2022, 0)
        assert "categoria" in url
        assert "7" in url

    def test_url_omits_categoria_for_year_only_query(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(None, 2022, 0)
        assert "categoria" not in url

    def test_url_contains_year_bounds_and_page(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(None, 2022, 3)
        assert "2022-01-01" in url
        assert "2022-12-31" in url
        assert "page=3" in url
        assert url.startswith(scraper.base_url)


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_happy_path_returns_docs_with_normalized_type(self):
        scraper = _make_scraper()
        soup = _make_listing_soup(
            [
                ("/doc/1", "Lei Ordinária 001/2022", "Leis Ordinárias"),
                ("/doc/2", "Decreto 002/2022", "Decretos Numerados"),
            ]
        )
        scraper._fetch_soup_with_retry = AsyncMock(return_value=soup)
        docs = await scraper._get_docs_links("http://example.com")
        assert len(docs) == 2
        assert docs[0]["title"] == "Lei Ordinária 001/2022"
        assert docs[0]["html_link"] == "/doc/1"
        assert docs[0]["type"] == "Lei Ordinária"
        assert docs[1]["type"] == "Decreto"

    @pytest.mark.asyncio
    async def test_empty_page_class_returns_empty(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(
            '<html><body><td class="views-empty">Nenhum resultado encontrado</td></body></html>',
            "html.parser",
        )
        scraper._fetch_soup_with_retry = AsyncMock(return_value=soup)
        docs = await scraper._get_docs_links("http://example.com")
        assert docs == []

    @pytest.mark.asyncio
    async def test_no_tbody_returns_empty(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(
            "<html><body><div>nothing</div></body></html>", "html.parser"
        )
        scraper._fetch_soup_with_retry = AsyncMock(return_value=soup)
        docs = await scraper._get_docs_links("http://example.com")
        assert docs == []

    @pytest.mark.asyncio
    async def test_row_with_wrong_column_count_is_skipped(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(
            "<html><body><table><tbody><tr><td>only one</td></tr></tbody></table></body></html>",
            "html.parser",
        )
        scraper._fetch_soup_with_retry = AsyncMock(return_value=soup)
        docs = await scraper._get_docs_links("http://example.com")
        assert docs == []

    @pytest.mark.asyncio
    async def test_passed_soup_skips_fetch(self):
        scraper = _make_scraper()
        scraper._fetch_soup_with_retry = AsyncMock()
        soup = _make_listing_soup([("/doc/1", "Lei 001", "Leis Ordinárias")])
        docs = await scraper._get_docs_links("http://example.com", soup=soup)
        assert len(docs) == 1
        scraper._fetch_soup_with_retry.assert_not_called()


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_resume_skip_returns_none(self):
        await assert_resume_skips(
            _make_scraper(), {"title": "Lei 001", "html_link": "/doc/1"}
        )

    @pytest.mark.asyncio
    async def test_fetch_failure_logs_error_and_returns_none(self):
        await assert_fetch_failure_saves_error(
            _make_scraper(), {"title": "Lei 001", "html_link": "/doc/1"}
        )

    @pytest.mark.asyncio
    async def test_missing_body_div_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(
                BeautifulSoup(
                    "<html><body><p>no body div here</p></body></html>", "html.parser"
                ),
                b"fake-mhtml",
            )
        )
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(
            {"title": "Lei 001", "html_link": "/doc/1"}
        )
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_revogado_span_sets_situation(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(
                BeautifulSoup(_make_doc_soup(revogado_span=True), "html.parser"),
                b"fake-mhtml",
            )
        )
        scraper._get_markdown = AsyncMock(
            return_value="# Lei\n\n" + "Texto revogado. " * 20
        )
        result = await scraper._get_doc_data(
            {"title": "Lei 001", "html_link": "/doc/1"}
        )
        assert result is not None
        assert result.get("situation") == "Revogado"

    @pytest.mark.asyncio
    async def test_revogado_div_alteracao_sets_situation(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(
                BeautifulSoup(
                    _make_doc_soup(revogado_div_text="Revogada pelo art. 13 da Lei X"),
                    "html.parser",
                ),
                b"fake-mhtml",
            )
        )
        scraper._get_markdown = AsyncMock(
            return_value="# Lei\n\n" + "Texto revogado. " * 20
        )
        result = await scraper._get_doc_data(
            {"title": "Lei 001", "html_link": "/doc/1"}
        )
        assert result is not None
        assert result.get("situation") == "Revogado"

    @pytest.mark.asyncio
    async def test_annex_note_does_not_set_revogado(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(
                BeautifulSoup(
                    _make_doc_soup(
                        revogado_div_text="NOTA: Anexos disponíveis no download."
                    ),
                    "html.parser",
                ),
                b"fake-mhtml",
            )
        )
        scraper._get_markdown = AsyncMock(
            return_value="# Lei\n\n" + "Texto regular. " * 20
        )
        result = await scraper._get_doc_data(
            {"title": "Lei 001", "html_link": "/doc/1"}
        )
        assert result is not None
        assert result.get("situation") != "Revogado"

    @pytest.mark.asyncio
    async def test_document_page_category_overrides_listing_type(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(
                BeautifulSoup(
                    _make_doc_soup(category="Leis Complementares"), "html.parser"
                ),
                b"fake-mhtml",
            )
        )
        scraper._get_markdown = AsyncMock(
            return_value="# Lei Complementar\n\n" + "Texto da lei. " * 20
        )
        result = await scraper._get_doc_data(
            {"title": "Lei 001", "type": "Decreto", "html_link": "/doc/1"}
        )
        assert result is not None
        assert result["type"] == "Lei Complementar"

    @pytest.mark.asyncio
    async def test_embedded_styles_and_scripts_are_removed_before_markdown(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        body_html = (
            "<style>.foo { color: red; }</style>"
            "<script>window.alert('x')</script>"
            '<p style="color:red">Texto da lei.</p>'
            '<div style="font-weight:bold">Outro trecho.</div>'
        )
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(
                BeautifulSoup(_make_doc_soup(body_html=body_html), "html.parser"),
                b"fake-mhtml",
            )
        )
        scraper._get_markdown = AsyncMock(
            return_value="# Lei Ordinária\n\n" + "Texto da lei. " * 20
        )
        result = await scraper._get_doc_data(
            {"title": "Lei 001", "html_link": "/doc/1"}
        )
        assert result is not None
        assert scraper._get_markdown.await_args is not None
        html_content = scraper._get_markdown.await_args.kwargs["html_content"]
        assert "<style" not in html_content
        assert "<script" not in html_content
        assert "style=" not in html_content

    @pytest.mark.asyncio
    async def test_invalid_markdown_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(BeautifulSoup(_make_doc_soup(), "html.parser"), b"fake-mhtml")
        )
        scraper._get_markdown = AsyncMock(return_value="short")
        scraper._save_doc_error = AsyncMock()
        result = await scraper._get_doc_data(
            {"title": "Lei 001", "html_link": "/doc/1"}
        )
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_valid_doc_returns_correct_shape(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(
                BeautifulSoup(_make_doc_soup(), "html.parser"),
                b"fake-mhtml-content",
            )
        )
        valid_md = "# Lei Ordinária\n\n" + "Texto da lei. " * 20
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        result = await scraper._get_doc_data(
            {
                "title": "Lei Ordinária 001/2022",
                "type": "Lei Ordinária",
                "html_link": "/doc/1",
            }
        )
        assert result is not None
        assert result["text_markdown"] == valid_md
        assert result["type"] == "Lei Ordinária"
        assert "norm_number" in result
        assert "date" in result
        assert "publication_date" in result
        assert "summary" in result
        assert "document_url" in result
        assert result["_raw_content"] == b"fake-mhtml-content"
        assert result["_content_extension"] == ".mhtml"


class TestScrapeYear:
    @pytest.mark.asyncio
    async def test_scrape_year_reuses_page_zero_and_processes_all_docs(self):
        scraper = _make_scraper()
        first_page_soup = _make_listing_soup(
            [("/doc/1", "Lei 001", "Leis Ordinárias")],
            last_page_index=1,
        )
        scraper._fetch_soup_with_retry = AsyncMock(return_value=first_page_soup)
        extra_docs = [
            {"title": "Decreto 002", "type": "Decreto", "html_link": "/doc/2"}
        ]
        calls: list[tuple[str, BeautifulSoup | None]] = []

        async def fake_get_docs_links(url: str, *, soup: BeautifulSoup | None = None):
            calls.append((url, soup))
            if soup is not None:
                return [
                    {"title": "Lei 001", "type": "Lei Ordinária", "html_link": "/doc/1"}
                ]
            return extra_docs

        scraper._get_docs_links = fake_get_docs_links

        async def fake_gather_results(tasks, **kwargs):
            return [await task for task in tasks]

        scraper._gather_results = AsyncMock(side_effect=fake_gather_results)
        scraper._process_documents = AsyncMock(return_value=[{"title": "ok"}])

        results = await scraper._scrape_year(2022)

        assert results == [{"title": "ok"}]
        assert calls[0][1] is first_page_soup
        assert scraper._gather_results.await_args is not None
        gather_tasks = scraper._gather_results.await_args.args[0]
        assert len(gather_tasks) == 1
        assert scraper._process_documents.await_args is not None
        process_docs = scraper._process_documents.await_args.args[0]
        assert len(process_docs) == 2
        assert all(doc["year"] == 2022 for doc in process_docs)
        assert {doc["type"] for doc in process_docs} == {"Lei Ordinária", "Decreto"}
        assert scraper._process_documents.await_args.kwargs["norm_type"] == "NA"
        assert scraper._process_documents.await_args.kwargs["situation"] == "Não consta"

    @pytest.mark.asyncio
    async def test_scrape_year_listing_failure_returns_empty(self):
        scraper = _make_scraper()
        scraper._fetch_soup_with_retry = AsyncMock(side_effect=RuntimeError("boom"))
        scraper._save_doc_error = AsyncMock()
        result = await scraper._scrape_year(2022)
        assert result == []
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_scrape_year_fetches_remaining_pages_concurrently(self):
        scraper = _make_scraper()
        first_page_soup = _make_listing_soup(
            [("/doc/1", "Lei 001", "Leis Ordinárias")],
            last_page_index=6,
        )
        scraper._fetch_soup_with_retry = AsyncMock(return_value=first_page_soup)

        async def fake_get_docs_links(url: str, *, soup: BeautifulSoup | None = None):
            if soup is not None:
                return [
                    {"title": "Lei 001", "type": "Lei Ordinária", "html_link": "/doc/1"}
                ]
            page = int(url.split("page=")[-1])
            return [
                {"title": f"Doc {page}", "type": "Decreto", "html_link": f"/doc/{page}"}
            ]

        scraper._get_docs_links = AsyncMock(side_effect=fake_get_docs_links)

        async def fake_gather_results(tasks, **kwargs):
            return [await task for task in tasks]

        scraper._gather_results = AsyncMock(side_effect=fake_gather_results)
        scraper._process_documents = AsyncMock(return_value=[{"title": "ok"}])

        results = await scraper._scrape_year(2022)

        assert results == [{"title": "ok"}]
        # _fetch_all_pages calls _gather_results once with all remaining pages (1-6)
        assert scraper._gather_results.await_count == 1
        all_page_tasks = scraper._gather_results.await_args_list[0].args[0]
        assert len(all_page_tasks) == 6
        assert scraper._process_documents.await_args is not None
        assert len(scraper._process_documents.await_args.args[0]) == 7


async def test_get_docs_links_year_only_returns_mixed_types(
    integration_scraper_factory,
):
    """Year-only Bahia query should return docs with type sourced from Categoria."""
    async with integration_scraper_factory(BahiaLegislaScraper) as scraper:
        url = scraper._build_search_url(None, 2025, 0)
        docs = await scraper._get_docs_links(url)
        assert isinstance(docs, list)
        assert len(docs) > 0
        assert all("type" in doc and doc["type"] for doc in docs)
        assert len({doc["type"] for doc in docs}) >= 2


async def test_get_docs_links_constituicao_1989_returns_results(
    integration_scraper_factory,
):
    """The current Bahia constitution should be reachable via corrected category 11."""
    async with integration_scraper_factory(BahiaLegislaScraper) as scraper:
        url = scraper._build_search_url(
            TYPES["Constituição Estadual Atual 1989"], 1989, 0
        )
        docs = await scraper._get_docs_links(url)
        assert len(docs) > 0
        assert docs[0]["type"] == "Constituição Estadual Atual 1989"


async def test_get_doc_data_year_only_returns_valid_markdown(
    integration_scraper_factory,
):
    """Fetching the first document from a year-only query should yield markdown."""
    async with integration_scraper_factory(BahiaLegislaScraper) as scraper:
        url = scraper._build_search_url(None, 2025, 0)
        docs = await scraper._get_docs_links(url)
        assert len(docs) > 0

        result = await scraper._get_doc_data(docs[0])
        if result is not None:
            assert result["text_markdown"] is not None
            assert len(result["text_markdown"]) > 50
