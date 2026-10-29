"Parser and metadata regression tests for ceara_scraper."

from collections import defaultdict

import pytest
from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.ce import SITUATIONS, TYPES, CearaAleceScraper


def _make_scraper(**kwargs) -> CearaAleceScraper:
    """Instantiate CearaAleceScraper with injected dependencies."""
    defaults = {
        "params": {"categoria": "", "page": 1},
        "_prefetched_docs": defaultdict(lambda: defaultdict(list)),
        "_lei_ordinaria_years": set(),
        "years": [],
        "year_start": 1968,
        "year_end": 2026,
    }
    defaults.update(kwargs)
    return make_scraper(CearaAleceScraper, **defaults)


def _make_valid_md() -> str:
    return "# Lei Estadual\n\nO governador do estado decreta a presente lei. " * 30


def _make_paginated_html(count: int = 3, norm_type: str = "Ato Normativo") -> str:
    """Build HTML for paginated scraper pages (6-td rows)."""
    rows = ""
    for i in range(1, count + 1):
        rows += f'\n        <tr>\n            <td>{i:04d}/2020</td>\n            <td>2020-01-{i:02d}</td>\n            <td>Ementa do ato {i}</td>\n            <td>Aprovado</td>\n            <td>Publicado</td>\n            <td><a href="https://www.al.ce.gov.br/doc/{i}">Ver</a></td>\n        </tr>'
    return f"<html><body><table><tr><th>Nr</th></tr>{rows}</table></body></html>"


def _make_static_index_html(
    count: int = 2, norm_type: str = "Emenda Constitucional"
) -> str:
    """Build HTML for static index pages (tds[0]=title with link, tds[1]=summary)."""
    rows = ""
    for i in range(1, count + 1):
        rows += f'\n        <tr>\n            <td><a href="emenda{i}.htm">Emenda Constitucional Nº {i}, DE 01.06.20{i:02d}</a></td>\n            <td>Ementa emenda {i}</td>\n        </tr>'
    return f"<html><body><table><tr><th>Título</th><th>Ementa</th></tr>{rows}</table></body></html>"


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 7
    REQUIRED_KEYS = {"Emenda Constitucional", "Lei Complementar", "Lei Ordinária"}
    REQUIRE_INT_VALUES = False


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_nao_consta_key_present(self):
        assert "Não consta" in SITUATIONS


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
        link = "file:///\\\\10.85.100.8\\10.85.100.8\\legislativo\\legislacao5\\leis2014\\15517.htm"
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


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_happy_path_returns_docs(self):
        scraper = _make_scraper()
        html = _make_paginated_html(3, "Ato Normativo")
        soup = BeautifulSoup(html, "lxml")
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
        soup = BeautifulSoup(html, "lxml")
        docs = await scraper._get_docs_links(
            "Ato Normativo", "https://example.com", soup=soup
        )
        assert docs == []

    @pytest.mark.asyncio
    async def test_title_includes_norm_type(self):
        scraper = _make_scraper()
        html = _make_paginated_html(1, "Resolução")
        soup = BeautifulSoup(html, "lxml")
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
        html = (
            f"<html><body><table><tr><td>dummy</td></tr></table>{inner}</body></html>"
        )
        soup = BeautifulSoup(html, "lxml")
        docs = await scraper._get_docs_links(
            "Ato Normativo", "https://example.com", soup=soup
        )
        assert len(docs) == 2


class TestGetDocsLinksMalformedRows:
    """Rows that don't match the expected 6-td format are silently skipped."""

    @pytest.mark.asyncio
    async def test_rows_with_wrong_td_count_are_skipped(self):
        """Only rows with exactly 6 <td> cells are parsed; others are ignored."""
        scraper = _make_scraper()
        html = '<html><body><table><tr><th>Nr</th></tr><tr><td>0001/2020</td><td>2020-01-01</td><td>Ementa</td><td>Ok</td><td>Ok</td><td><a href="https://example.com/1">Ver</a></td></tr><tr><td>Bad</td><td>only two</td></tr><tr><td>Single</td></tr><tr><td>0002/2020</td><td>2020-01-02</td><td>Ementa 2</td><td>Ok</td><td>Ok</td><td><a href="https://example.com/2">Ver</a></td></tr></table></body></html>'
        soup = BeautifulSoup(html, "lxml")
        docs = await scraper._get_docs_links(
            "Ato Normativo", "https://example.com", soup=soup
        )
        assert len(docs) == 2
        assert docs[0]["document_url"] == "https://example.com/1"
        assert docs[1]["document_url"] == "https://example.com/2"

    @pytest.mark.asyncio
    async def test_header_only_table_returns_empty(self):
        """A table containing only header rows (no data rows with 6 tds) returns []."""
        scraper = _make_scraper()
        html = "<html><body><table><tr><th>Nr</th><th>Data</th><th>Ementa</th><th>Situação</th><th>Pub</th><th>Link</th></tr></table></body></html>"
        soup = BeautifulSoup(html, "lxml")
        docs = await scraper._get_docs_links(
            "Ato Normativo", "https://example.com", soup=soup
        )
        assert docs == []
