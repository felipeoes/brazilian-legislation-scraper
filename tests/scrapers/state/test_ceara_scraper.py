"""Tests for CearaAleceScraper.

Covers:
- TYPES constant: 7 types present (3 PAGINATED + 3 STATIC_INDEX + Lei Ordinária)
- SITUATIONS module-level dict preserved for downstream consumers
- Class docstring accessible (__doc__ is not None)
- PAGINATED_TYPES and STATIC_INDEX_TYPES class-level sets
- _format_search_url: correct URL + query params
- _get_docs_links: happy-path returns docs with all keys; empty page returns [];
  FailedRequest returns []
- _get_doc_data: resume skip, FailedRequest → error + None, invalid markdown → error + None,
  valid → correct shape
- _get_laws_constitution_amendments_doc_data: resume skip, HTTP error → None,
  invalid-doc guard, invalid markdown → error + None, valid → correct shape
- _get_laws_constitution_amendments_docs_links: FailedRequest returns []
- _fetch_lei_ordinaria_years: FailedRequest returns []
- construct_url: https passthrough, file:// rewrite, type-specific base URL logic
- _scrape_type: PAGINATED branch delegates, Lei Ordinária skip when year absent,
  STATIC_INDEX_TYPES uses prefetched docs, unknown type returns []

Run with:
    .venv/bin/pytest tests/test_ceara_scraper.py -v
"""

from collections import defaultdict
from unittest.mock import AsyncMock, MagicMock

import pytest
from bs4 import BeautifulSoup

from src.scraper.state_legislation.ceara import (
    SITUATIONS,
    TYPES,
    CearaAleceScraper,
)

from base_tests import ScraperClassTests, SituationsConstantTests, TypesConstantTests
from conftest import make_base_scraper, make_failed_request, assert_resume_skips


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------


def _make_scraper(**kwargs) -> CearaAleceScraper:
    """Instantiate CearaAleceScraper bypassing __init__ (no network, no I/O)."""
    defaults = {
        "params": {"categoria": "", "page": 1},
        "_prefetched_docs": defaultdict(lambda: defaultdict(list)),
        "_lei_ordinaria_years": set(),
        "years": [],
        "year_start": 1968,
        "year_end": 2026,
    }
    defaults.update(kwargs)
    return make_base_scraper(
        CearaAleceScraper,
        "https://www.al.ce.gov.br/legislativo",
        "CEARA",
        dict(TYPES),
        situations=dict(SITUATIONS),
        **defaults,
    )


def _make_failed_request():
    """Create a falsy FailedRequest-like sentinel."""
    return make_failed_request()


def _make_valid_md() -> str:
    return "# Lei Estadual\n\nO governador do estado decreta a presente lei. " * 30


def _make_paginated_html(count: int = 3, norm_type: str = "Ato Normativo") -> str:
    """Build HTML for paginated scraper pages (6-td rows)."""
    rows = ""
    for i in range(1, count + 1):
        rows += f"""
        <tr>
            <td>{i:04d}/2020</td>
            <td>2020-01-{i:02d}</td>
            <td>Ementa do ato {i}</td>
            <td>Aprovado</td>
            <td>Publicado</td>
            <td><a href="https://www.al.ce.gov.br/doc/{i}">Ver</a></td>
        </tr>"""
    return f"<html><body><table><tr><th>Nr</th></tr>{rows}</table></body></html>"


def _make_static_index_html(
    count: int = 2, norm_type: str = "Emenda Constitucional"
) -> str:
    """Build HTML for static index pages (tds[0]=title with link, tds[1]=summary)."""
    rows = ""
    for i in range(1, count + 1):
        rows += f"""
        <tr>
            <td><a href="emenda{i}.htm">Emenda Constitucional Nº {i}, DE 01.06.20{i:02d}</a></td>
            <td>Ementa emenda {i}</td>
        </tr>"""
    return f"<html><body><table><tr><th>Título</th><th>Ementa</th></tr>{rows}</table></body></html>"


# ---------------------------------------------------------------------------
# TYPES constant
# ---------------------------------------------------------------------------


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 7
    REQUIRED_KEYS = {"Emenda Constitucional", "Lei Complementar", "Lei Ordinária"}
    REQUIRE_INT_VALUES = False


# ---------------------------------------------------------------------------
# SITUATIONS constant
# ---------------------------------------------------------------------------


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_nao_consta_key_present(self):
        assert "Não consta" in SITUATIONS


# ---------------------------------------------------------------------------
# Class-level attributes
# ---------------------------------------------------------------------------


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = CearaAleceScraper
    STATE_NAME = "Ceara"

    def test_paginated_types_class_set(self):
        assert CearaAleceScraper.PAGINATED_TYPES == {
            "Ato Deliberativo",
            "Ato Normativo",
            "Resolução",
        }

    def test_static_index_types_class_set(self):
        assert CearaAleceScraper.STATIC_INDEX_TYPES == {
            "Emenda Constitucional",
            "Lei Complementar",
            "Decreto Legislativo",
        }


# ---------------------------------------------------------------------------
# _format_search_url
# ---------------------------------------------------------------------------


class TestFormatSearchUrl:
    def test_contains_categoria_param(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("ato-normativo", 1)
        assert "categoria=ato-normativo" in url

    def test_contains_page_param(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("resolucao", 3)
        assert "page=3" in url

    def test_base_url_included(self):
        scraper = _make_scraper()
        url = scraper._format_search_url("ato-normativo", 1)
        assert "al.ce.gov.br" in url

    def test_no_shared_mutation(self):
        scraper = _make_scraper()
        url1 = scraper._format_search_url("ato-normativo", 1)
        url2 = scraper._format_search_url("resolucao", 2)
        assert "ato-normativo" in url1
        assert "resolucao" in url2
        assert "ato-normativo" not in url2


# ---------------------------------------------------------------------------
# construct_url
# ---------------------------------------------------------------------------


class TestConstructUrl:
    def test_https_passthrough(self):
        scraper = _make_scraper()
        url = scraper.construct_url(
            "Lei Ordinária", "https://example.com/lei.htm", 2020
        )
        assert url == "https://example.com/lei.htm"

    def test_http_normalized_to_https(self):
        scraper = _make_scraper()
        url = scraper.construct_url(
            "Lei Ordinária",
            "http://www2.al.ce.gov.br/legislativo/legislacao5/leis85/11120.htm",
            1985,
        )
        assert (
            url == "https://www2.al.ce.gov.br/legislativo/legislacao5/leis85/11120.htm"
        )

    def test_file_protocol_rewrite(self):
        scraper = _make_scraper()
        link = r"file:///\\10.85.100.8\10.85.100.8\legislativo\legislacao5\leis2014\15517.htm"
        url = scraper.construct_url("Emenda Constitucional", link, None)
        assert "al.ce.gov.br" in url
        assert "15517.htm" in url
        assert "file://" not in url

    def test_lei_complementar_base_url(self):
        scraper = _make_scraper()
        url = scraper.construct_url("Lei Complementar", "lc001.htm", None)
        assert "al.ce.gov.br" in url
        assert "ementario" in url

    def test_decreto_legislativo_base_url(self):
        scraper = _make_scraper()
        url = scraper.construct_url("Decreto Legislativo", "decleg001.htm", None)
        assert "decleg" in url

    def test_lei_ordinaria_injects_year_folder(self):
        scraper = _make_scraper()
        url = scraper.construct_url("Lei Ordinária", "lei001.htm", 2020)
        assert "leis2020" in url
        assert "lei001.htm" in url


# ---------------------------------------------------------------------------
# _get_docs_links
# ---------------------------------------------------------------------------


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_happy_path_returns_docs(self):
        scraper = _make_scraper()
        html = _make_paginated_html(3, "Ato Normativo")
        soup = BeautifulSoup(html, "html.parser")
        docs = await scraper._get_docs_links(
            "Ato Normativo", "https://example.com", soup=soup
        )
        assert len(docs) == 3
        for doc in docs:
            assert "title" in doc
            assert "year" in doc
            assert "document_url" in doc
            assert "summary" in doc

    @pytest.mark.asyncio
    async def test_empty_tag_returns_empty(self):
        scraper = _make_scraper()
        html = '<html><body><p class="mt-5">Nenhum dado localizado</p></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        docs = await scraper._get_docs_links(
            "Ato Normativo", "https://example.com", soup=soup
        )
        assert docs == []

    @pytest.mark.asyncio
    async def test_title_includes_norm_type(self):
        scraper = _make_scraper()
        html = _make_paginated_html(1, "Resolução")
        soup = BeautifulSoup(html, "html.parser")
        docs = await scraper._get_docs_links(
            "Resolução", "https://example.com", soup=soup
        )
        assert len(docs) == 1
        assert docs[0]["title"].startswith("Resolução")

    @pytest.mark.asyncio
    async def test_second_table_preferred(self):
        """When 2+ tables exist the second is used."""
        scraper = _make_scraper()
        inner = _make_paginated_html(2, "Ato Normativo")
        # Wrap with a dummy first table
        html = (
            f"<html><body><table><tr><td>dummy</td></tr></table>{inner}</body></html>"
        )
        soup = BeautifulSoup(html, "html.parser")
        docs = await scraper._get_docs_links(
            "Ato Normativo", "https://example.com", soup=soup
        )
        assert len(docs) == 2

    @pytest.mark.asyncio
    async def test_fetches_soup_when_not_provided(self):
        scraper = _make_scraper()
        html = _make_paginated_html(1, "Ato Normativo")
        soup = BeautifulSoup(html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        docs = await scraper._get_docs_links("Ato Normativo", "https://example.com")
        scraper.request_service.get_soup.assert_called_once()
        assert len(docs) == 1

    @pytest.mark.asyncio
    async def test_failed_request_returns_empty(self):
        """FailedRequest sentinel from get_soup returns []."""
        scraper = _make_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=_make_failed_request()
        )
        docs = await scraper._get_docs_links("Ato Normativo", "https://example.com")
        assert docs == []


# ---------------------------------------------------------------------------
# _get_doc_data
# ---------------------------------------------------------------------------


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_resume_skip_returns_none(self):
        await assert_resume_skips(
            _make_scraper(),
            {
                "title": "Ato 001/2020",
                "document_url": "https://www.al.ce.gov.br/doc/1",
                "year": "2020",
            },
        )

    @pytest.mark.asyncio
    async def test_failed_request_saves_error_and_returns_none(self):
        """FailedRequest from get_soup triggers _save_doc_error and returns None."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper.request_service.get_soup = AsyncMock(
            return_value=_make_failed_request()
        )
        scraper._save_doc_error = AsyncMock()
        doc_info = {
            "title": "Ato 001/2020",
            "document_url": "https://www.al.ce.gov.br/doc/1",
            "year": "2020",
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_markdown_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        html = "<html><body><div class='card-body'>ok</div></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        scraper._clean_norm_soup = MagicMock()
        scraper._get_markdown = AsyncMock(return_value="short")
        scraper._save_doc_error = AsyncMock()
        doc_info = {
            "title": "Ato 001/2020",
            "document_url": "https://www.al.ce.gov.br/doc/1",
            "year": "2020",
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_valid_doc_returns_correct_shape(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        html = "<html><body><div class='card-body'>content</div></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        mhtml = b"MHTML content"
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, mhtml))
        scraper._clean_norm_soup = MagicMock()
        valid_md = _make_valid_md()
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc_info = {
            "title": "Ato 001/2020",
            "document_url": "https://www.al.ce.gov.br/doc/1",
            "year": "2020",
        }
        result = await scraper._get_doc_data(doc_info)
        assert result is not None
        assert result["text_markdown"] == valid_md
        assert result["_content_extension"] == ".mhtml"
        assert result["_raw_content"] == mhtml

    @pytest.mark.asyncio
    async def test_visualizar_url_constructed(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        html = "<html><body><main>content</main></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        mhtml = b"MHTML content"
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, mhtml))
        scraper._clean_norm_soup = MagicMock()
        valid_md = _make_valid_md()
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc_info = {
            "title": "Ato 001/2020",
            "document_url": "https://www.al.ce.gov.br/doc/1/",
            "year": "2020",
        }
        await scraper._get_doc_data(doc_info)
        called_url = scraper._fetch_soup_and_mhtml.call_args[0][0]
        assert called_url.endswith("/visualizar")
        assert "//" not in called_url.replace("https://", "")


# ---------------------------------------------------------------------------
# _get_laws_constitution_amendments_doc_data
# ---------------------------------------------------------------------------


class TestGetLawsConstitutionAmendmentsDocData:
    @pytest.mark.asyncio
    async def test_resume_skip_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=True)
        doc_info = {"title": "Emenda 1", "html_link": "emenda1.htm", "year": 2020}
        result = await scraper._get_laws_constitution_amendments_doc_data(
            doc_info, "Emenda Constitucional", year=2020
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_http_error_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._fetch_soup_and_mhtml = AsyncMock(side_effect=Exception("fetch failed"))
        scraper._save_doc_error = AsyncMock()
        doc_info = {"title": "Emenda 1", "html_link": "emenda1.htm", "year": 2020}
        result = await scraper._get_laws_constitution_amendments_doc_data(
            doc_info, "Emenda Constitucional", year=2020
        )
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_doc_guard_saves_error(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        html = "<html><body>NÄO EXISTE LEI COM ESTE NÚMERO</body></html>"
        soup = BeautifulSoup(html, "html.parser")
        mhtml = b"MHTML content"
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, mhtml))
        scraper._save_doc_error = AsyncMock()
        doc_info = {"title": "Lei 9999", "html_link": "lei9999.htm", "year": 2020}
        result = await scraper._get_laws_constitution_amendments_doc_data(
            doc_info, "Lei Ordinária", year=2020
        )
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_markdown_saves_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        html = "<html><body><p>Content here</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        mhtml = b"MHTML content"
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, mhtml))
        scraper._get_markdown = AsyncMock(return_value="short")
        scraper._browser_pdf_to_markdown = AsyncMock(return_value="")
        scraper._save_doc_error = AsyncMock()
        doc_info = {"title": "Lei 001", "html_link": "lei001.htm", "year": 2020}
        result = await scraper._get_laws_constitution_amendments_doc_data(
            doc_info, "Lei Ordinária", year=2020
        )
        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_valid_doc_returns_correct_shape(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        html = "<html><body><p>Valid content</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        mhtml = b"MHTML content"
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, mhtml))
        valid_md = _make_valid_md()
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc_info = {
            "title": "Lei 001",
            "html_link": "lei001.htm",
            "year": 2020,
            "summary": "ementa",
        }
        result = await scraper._get_laws_constitution_amendments_doc_data(
            doc_info, "Lei Ordinária", year=2020
        )
        assert result is not None
        assert result["text_markdown"] == valid_md
        assert result["_content_extension"] == ".mhtml"
        assert result["_raw_content"] == mhtml
        assert "document_url" in result
        # html_link must be popped
        assert "html_link" not in result

    @pytest.mark.asyncio
    async def test_section_divs_merged_when_present(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        html = (
            "<html><body>"
            '<div class="Section1"><p>Header</p></div>'
            '<div class="Section2"><p>Body text</p></div>'
            "</body></html>"
        )
        soup = BeautifulSoup(html, "html.parser")
        mhtml = b"MHTML content"
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(soup, mhtml))
        valid_md = _make_valid_md()
        scraper._get_markdown = AsyncMock(return_value=valid_md)
        doc_info = {
            "title": "Lei 001",
            "html_link": "lei001.htm",
            "year": 2020,
            "summary": "",
        }
        await scraper._get_laws_constitution_amendments_doc_data(
            doc_info, "Lei Ordinária", year=2020
        )
        called_html = scraper._get_markdown.call_args[1]["html_content"]
        # Both sections' content should be merged into the container
        assert "Header" in called_html
        assert "Body text" in called_html
        # Section class names should not be in the merged container
        assert "Section1" not in called_html
        assert "Section2" not in called_html


# ---------------------------------------------------------------------------
# _get_laws_constitution_amendments_docs_links — FailedRequest guard
# ---------------------------------------------------------------------------


class TestGetLawsConstitutionAmendmentsDocsLinks:
    @pytest.mark.asyncio
    async def test_failed_request_returns_empty(self):
        scraper = _make_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=_make_failed_request()
        )
        result = await scraper._get_laws_constitution_amendments_docs_links(
            "https://example.com", "Emenda Constitucional"
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_happy_path_returns_docs(self):
        scraper = _make_scraper()
        html = _make_static_index_html(2)
        soup = BeautifulSoup(html, "html.parser")
        scraper.request_service.get_soup = AsyncMock(return_value=soup)
        result = await scraper._get_laws_constitution_amendments_docs_links(
            "https://example.com", "Emenda Constitucional"
        )
        assert len(result) == 2
        for doc in result:
            assert "title" in doc
            assert "year" in doc
            assert "html_link" in doc


# ---------------------------------------------------------------------------
# _fetch_lei_ordinaria_years — FailedRequest guard
# ---------------------------------------------------------------------------


class TestFetchLeiOrdinariaYears:
    @pytest.mark.asyncio
    async def test_failed_request_returns_empty(self):
        scraper = _make_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=_make_failed_request()
        )
        result = await scraper._fetch_lei_ordinaria_years()
        assert result == []


# ---------------------------------------------------------------------------
# _scrape_type
# ---------------------------------------------------------------------------


class TestScrapeType:
    @pytest.mark.asyncio
    async def test_paginated_type_delegates_to_paginated_method(self):
        scraper = _make_scraper()
        scraper._scrape_paginated_type = AsyncMock(return_value=[{"title": "doc"}])
        results = await scraper._scrape_type("Ato Normativo", "ato-normativo", 2020)
        scraper._scrape_paginated_type.assert_called_once_with("Ato Normativo", 2020)
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_lei_ordinaria_skips_when_year_not_in_set(self):
        scraper = _make_scraper()
        scraper._lei_ordinaria_years = {2019, 2021}
        results = await scraper._scrape_type("Lei Ordinária", "lei_ordinaria.htm", 2020)
        assert results == []

    @pytest.mark.asyncio
    async def test_lei_ordinaria_calls_scrape_when_year_present(self):
        scraper = _make_scraper()
        scraper._lei_ordinaria_years = {2020}
        scraper._scrape_laws_constitution_amendments = AsyncMock(
            return_value=[{"title": "Lei 001"}]
        )
        results = await scraper._scrape_type("Lei Ordinária", "lei_ordinaria.htm", 2020)
        scraper._scrape_laws_constitution_amendments.assert_called_once()
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_static_index_type_uses_prefetched_docs(self):
        """Static index types use prefetched docs per year."""
        scraper = _make_scraper()
        scraper.years = [2019, 2020]
        scraper._prefetched_docs[2020]["Emenda Constitucional"] = [
            {"title": "Emenda 1", "html_link": "e1.htm", "year": 2020},
        ]
        scraper._process_documents = AsyncMock(
            return_value=[{"title": "Emenda 1", "year": 2020}]
        )
        results = await scraper._scrape_type(
            "Emenda Constitucional", "legislacao5/const_e/ement.htm", 2020
        )
        scraper._process_documents.assert_called_once()
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_static_index_type_empty_when_no_prefetched(self):
        """Static index type returns [] when no prefetched docs for year."""
        scraper = _make_scraper()
        results = await scraper._scrape_type(
            "Emenda Constitucional", "legislacao5/const_e/ement.htm", 2020
        )
        assert results == []

    @pytest.mark.asyncio
    async def test_unknown_type_returns_empty(self):
        scraper = _make_scraper()
        results = await scraper._scrape_type("UnknownType", "unknown-id", 2020)
        assert results == []


# ---------------------------------------------------------------------------
# _bucket_prefetched
# ---------------------------------------------------------------------------


class TestBucketPrefetched:
    def test_filters_below_resume_from(self):
        scraper = _make_scraper()
        docs = [{"year": "2018"}, {"year": "2020"}]
        scraper._bucket_prefetched(docs, "Ato Normativo", resume_from=2019)
        assert 2018 not in scraper._prefetched_docs
        assert len(scraper._prefetched_docs[2020]["Ato Normativo"]) == 1

    def test_filters_above_year_end(self):
        scraper = _make_scraper(year_end=2020)
        docs = [{"year": "2020"}, {"year": "2021"}]
        scraper._bucket_prefetched(docs, "Ato Normativo", resume_from=2019)
        assert 2021 not in scraper._prefetched_docs
        assert len(scraper._prefetched_docs[2020]["Ato Normativo"]) == 1
