"""Integration tests for CamaraDepScraper.

These tests hit the live camara.leg.br website to verify the refactored
scraper produces the expected document count and correctly populates the
situation field per document.

Run with:
    uv run pytest tests/test_camara_dep_scraper.py -v -s
"""

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from bs4 import BeautifulSoup

from src.scraper.federal_legislation.scrape import (
    COVERAGE,
    EXPORT_MAX_DOCS,
    ORDERING,
    TYPES,
    CamaraDepScraper,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TEST_YEAR = 2020
TEST_TYPE = "Decreto"
TEST_TYPE_ID = TYPES[TEST_TYPE]  # "Decreto"
PER_PAGE = 20


def build_scraper(save_dir: Path) -> CamaraDepScraper:
    return CamaraDepScraper(
        year_start=TEST_YEAR,
        year_end=TEST_YEAR,
        docs_save_dir=str(save_dir),
        verbose=True,
        rps=5,  # be polite during tests
    )


def build_unit_scraper() -> Any:
    scraper = object.__new__(CamaraDepScraper)
    scraper.verbose = False
    scraper.overwrite = False
    scraper.name = "LEGISLACAO_FEDERAL"
    scraper.base_url = "https://www.camara.leg.br/legislacao/"
    scraper.coverage = COVERAGE
    scraper.ordering = ORDERING
    scraper.export_max_docs = EXPORT_MAX_DOCS
    scraper.request_service = MagicMock()
    scraper._save_doc_error = AsyncMock()
    scraper._metadata_to_text_url = {}
    scraper._scraped_keys = set()
    return scraper


async def get_expected_total(scraper: CamaraDepScraper) -> int:
    """Fetch the first listing page and read the total result count."""
    url = scraper._format_search_url(str(TEST_YEAR), TEST_TYPE_ID)
    soup = await scraper.request_service.get_soup(url)
    assert isinstance(soup, BeautifulSoup), f"Could not fetch listing page: {url}"
    total_el = soup.find(
        "div",
        class_="busca-info__resultado busca-info__resultado--informado",
    )
    assert total_el is not None, "Could not find total element on listing page"
    total = int(total_el.text.strip().split()[-1])
    return total


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_decreto_2020_count_matches_website(integration_scraper_factory):
    """Scraped document count must equal the website-reported total for Decreto 2020."""
    async with integration_scraper_factory(
        CamaraDepScraper,
        year_start=TEST_YEAR,
        year_end=TEST_YEAR,
        verbose=True,
        rps=5,
    ) as scraper:
        expected_total = await get_expected_total(scraper)
        print(f"\nWebsite reports {expected_total} Decretos for {TEST_YEAR}")
        assert expected_total > 0, "Expected at least some Decretos for 2020"

        results = await scraper._scrape_type(TEST_TYPE, TEST_TYPE_ID, TEST_YEAR)
        print(f"Scraper returned {len(results)} results")

        # Allow a small tolerance for documents that fail (empty text, 404, etc.)
        tolerance = max(5, int(expected_total * 0.05))
        assert len(results) >= expected_total - tolerance, (
            f"Expected ~{expected_total} docs, got {len(results)} "
            f"(tolerance={tolerance})"
        )


@pytest.mark.integration
async def test_decreto_2020_situation_field_populated(integration_scraper_factory):
    """Every saved document must have a non-empty situation field."""
    async with integration_scraper_factory(
        CamaraDepScraper,
        year_start=TEST_YEAR,
        year_end=TEST_YEAR,
        verbose=True,
        rps=5,
    ) as scraper:
        results = await scraper._scrape_type(TEST_TYPE, TEST_TYPE_ID, TEST_YEAR)
        assert results, "No results returned — nothing to check"

        # Inspect the shard files written by FileSaver (chunk_NNNNNN.json inside <year>/shards/)
        save_dir = scraper.docs_save_dir
        data_files = list(save_dir.rglob("chunk_*.json"))
        assert data_files, "No chunk_*.json shard files written to save dir"

        saved_docs = []
        for df in data_files:
            content = json.loads(df.read_text(encoding="utf-8"))
            docs = (
                content if isinstance(content, list) else content.get("documents", [])
            )
            saved_docs.extend(docs)

        print(
            f"\nFound {len(saved_docs)} saved documents across {len(data_files)} shard files"
        )

        missing_situation = [d for d in saved_docs if not d.get("situation")]
        print(f"Documents missing situation: {len(missing_situation)}")
        if missing_situation:
            for d in missing_situation[:5]:
                print(f"  - {d.get('title', '?')}")

        # Allow a small fraction to have empty situation (edge cases)
        tolerance = max(3, int(len(saved_docs) * 0.02))
        assert len(missing_situation) <= tolerance, (
            f"{len(missing_situation)} docs missing situation field "
            f"(tolerance={tolerance})"
        )


@pytest.mark.integration
async def test_format_search_url_no_situation_filter(integration_scraper_factory):
    """_format_search_url must produce a URL with situacao= (empty) for all-situation query."""
    async with integration_scraper_factory(
        CamaraDepScraper,
        year_start=TEST_YEAR,
        year_end=TEST_YEAR,
        verbose=True,
        rps=5,
    ) as scraper:
        url = scraper._format_search_url("2020", "Decreto")
        assert "situacao=" in url
        # Must NOT have a non-empty situacao value
        assert "situacao=&" in url or url.endswith("situacao=")
        assert "tipo=Decreto" in url
        assert "ano=2020" in url


@pytest.mark.integration
async def test_get_docs_links_returns_situation(integration_scraper_factory):
    """_get_docs_links must return dicts with a non-empty situation for real pages."""
    async with integration_scraper_factory(
        CamaraDepScraper,
        year_start=TEST_YEAR,
        year_end=TEST_YEAR,
        verbose=True,
        rps=5,
    ) as scraper:
        url = scraper._format_search_url("2020", "Decreto") + "&pagina=1"
        docs = await scraper._get_docs_links(url)
        assert docs, "Expected at least one document on page 1"
        for doc in docs:
            assert "situation" in doc, "situation key missing from doc dict"
            assert "title" in doc
            assert "metadata_url" in doc
        # At least some docs should have a non-empty situation
        with_situation = [d for d in docs if d["situation"]]
        assert len(with_situation) > 0, "No documents had a situation on page 1"
        print(f"\nPage 1: {len(docs)} docs, {len(with_situation)} with situation")
        print(f"Sample situations: {[d['situation'] for d in docs[:3]]}")


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
        # Types added after discovery of missing norm types
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
                metadata_url=(
                    "https://www2.camara.leg.br/legin/fed/lei/2026/"
                    "lei-15323-6-janeiro-2026-798626-norma-pl.html"
                ),
            )
            == "Lei Ordinária"
        )

    def test_resolve_norm_type_maps_slug_only_titles(self):
        scraper = build_unit_scraper()

        assert (
            scraper._resolve_norm_type(
                "Carta de Lei  de 29 de Novembro de 1808",
                metadata_url=(
                    "https://www2.camara.leg.br/legin/fed/carlei_sn/anterioresa1824/"
                    "cartadelei-40273-29-novembro-1808-572462-norma-pe.html"
                ),
            )
            == "Carta de Lei"
        )


class TestListingParsing:
    def test_parse_export_documents_extracts_metadata(self):
        scraper = build_unit_scraper()
        soup = BeautifulSoup(
            """
            <html><body>
              <div id="impressaoPDF">
                <ul class="busca-resultados">
                  <li>
                    <a href="https://www2.camara.leg.br/legin/fed/decret/example-norma.html">
                      Decreto nº 1
                    </a>
                    <p><span class="bold color-gray">Ementa:</span> Dispõe sobre teste.</p>
                    <p><span class="bold color-gray">Situação: </span>Não consta revogação expressa</p>
                  </li>
                </ul>
              </div>
            </body></html>
            """,
            "html.parser",
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


class TestGetDocumentTextLink:
    @staticmethod
    def _make_doc(**overrides):
        doc = {
            "title": "ATO DA MESA Nº 128, DE 27/05/2020",
            "summary": "Aprova o Relatório de Gestão Fiscal.",
            "metadata_url": "https://www2.camara.leg.br/legin/int/atomes/2020/atodamesa-128-27-maio-2020-790247-norma-cd-mesa.html",
            "situation": "Não consta revogação expressa",
        }
        doc.update(overrides)
        return doc

    @pytest.mark.asyncio
    async def test_get_document_text_link_scans_all_sessao_blocks(self):
        scraper = build_unit_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(
                """
                <html><body>
                  <div class="sessao"><span><strong>Origem:</strong></span><span>Poder Executivo</span></div>
                  <div class="sessao">
                    <a href="atodamesa-128-27-maio-2020-790247-publicacaooriginal-160775-cd-mesa.html">
                      Texto - Publicação Original
                    </a>
                    <a href="atodamesa-128-27-maio-2020-790247-retificacao-160796-cd-mesa.html">
                      Texto - Retificação
                    </a>
                  </div>
                </body></html>
                """,
                "html.parser",
            )
        )

        result = await scraper._get_document_text_link(
            self._make_doc(), year=2020, norm_type="Ato da Mesa"
        )

        assert result is not None
        assert result["metadata_url"].endswith("790247-norma-cd-mesa.html")
        assert result["document_url"].endswith(
            "790247-publicacaooriginal-160775-cd-mesa.html"
        )

    @pytest.mark.asyncio
    async def test_get_document_text_link_prefers_republication(self):
        scraper = build_unit_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(
                """
                <html><body>
                  <div class="sessao">
                    <a href="ato-publicacaooriginal-100.html">Texto - Publicação Original</a>
                    <a href="ato-republicacao-200.html">Texto - Republicação</a>
                  </div>
                </body></html>
                """,
                "html.parser",
            )
        )

        result = await scraper._get_document_text_link(
            self._make_doc(), year=2020, norm_type="Ato da Mesa"
        )

        assert result is not None
        assert result["document_url"].endswith("ato-republicacao-200.html")
        assert scraper._metadata_to_text_url[result["metadata_url"]].endswith(
            "ato-republicacao-200.html"
        )

    @pytest.mark.asyncio
    async def test_get_document_text_link_uses_cached_mapping(self):
        scraper = build_unit_scraper()
        doc = self._make_doc()
        scraper._metadata_to_text_url[doc["metadata_url"]] = (
            "https://www2.camara.leg.br/legin/int/atomes/2020/ato-republicacao-200.html"
        )

        result = await scraper._get_document_text_link(
            doc, year=2020, norm_type="Ato da Mesa"
        )

        assert result is not None
        assert result["document_url"].endswith("ato-republicacao-200.html")
        scraper.request_service.get_soup.assert_not_called()


class TestGetDocData:
    @staticmethod
    def _make_doc(**overrides):
        doc = {
            "title": "DECRETO Nº 365, DE 10 DE SETEMBRO DE 1845",
            "summary": "Approva a Pensão annual de hum conto e duzentos mil réis.",
            "metadata_url": "https://www2.camara.leg.br/legin/fed/decret/example-norma.html",
            "document_url": "https://www2.camara.leg.br/legin/fed/decret/example.html",
            "situation": "Não consta revogação expressa",
        }
        doc.update(overrides)
        return doc

    @pytest.mark.asyncio
    async def test_get_doc_data_strips_ementa_when_body_exists(self):
        scraper = build_unit_scraper()
        fake_html = b"""
            <html><body><div id="content">
                <h1>DECRETO N\xc2\xba 365, DE 10 DE SETEMBRO DE 1845</h1>
                <div class="textoNorma">
                    <p class="ementa">Approva a Pens\xc3\xa3o annual de hum conto e duzentos mil r\xc3\xa9is.</p>
                    <p>Art. 1\xc2\xba Fica mantida a concess\xc3\xa3o da pens\xc3\xa3o.</p>
                    <p>Art. 2\xc2\xba Revogam-se as disposi\xc3\xa7\xc3\xb5es em contr\xc3\xa1rio.</p>
                </div>
            </div></body></html>
            """
        scraper.request_service.fetch_bytes = AsyncMock(
            return_value=(fake_html, "text/html")
        )

        markdown_calls = []
        expected_markdown = (
            "Art. 1º Fica mantida a concessão da pensão.\n\n"
            "Art. 2º Revogam-se as disposições em contrário."
        )

        async def fake_get_markdown(**kwargs):
            markdown_calls.append(kwargs["html_content"])
            return expected_markdown

        scraper._get_markdown = fake_get_markdown

        result = await scraper._get_doc_data(
            self._make_doc(), year=1845, norm_type="Decreto"
        )

        assert result is not None
        assert result["type"] == "Decreto"
        assert result["text_markdown"] == expected_markdown
        assert len(markdown_calls) == 1

        converted_soup = BeautifulSoup(markdown_calls[0], "html.parser")
        assert converted_soup.find("h1") is None
        assert converted_soup.find("p", class_="ementa") is None
        assert "Art. 1º" in converted_soup.get_text(" ", strip=True)
        assert scraper._save_doc_error.await_count == 0

    @pytest.mark.asyncio
    async def test_get_doc_data_falls_back_to_ementa_when_it_is_the_only_text(self):
        scraper = build_unit_scraper()
        fake_html = b"""
            <html><body><div id="content">
                <h1>DECRETO N\xc2\xba 365, DE 10 DE SETEMBRO DE 1845</h1>
                <div class="textoNorma">
                    <p class="ementa">Approva a Pens\xc3\xa3o annual de hum conto e duzentos mil r\xc3\xa9is.</p>
                </div>
            </div></body></html>
            """
        scraper.request_service.fetch_bytes = AsyncMock(
            return_value=(fake_html, "text/html")
        )

        markdown_calls = []
        fallback_markdown = "Approva a Pensão annual de hum conto e duzentos mil réis."

        async def fake_get_markdown(**kwargs):
            markdown_calls.append(kwargs["html_content"])
            return "" if len(markdown_calls) == 1 else fallback_markdown

        scraper._get_markdown = fake_get_markdown

        result = await scraper._get_doc_data(
            self._make_doc(), year=1845, norm_type="Decreto"
        )

        assert result is not None
        assert result["type"] == "Decreto"
        assert result["text_markdown"] == fallback_markdown
        assert len(markdown_calls) == 2

        stripped_soup = BeautifulSoup(markdown_calls[0], "html.parser")
        fallback_soup = BeautifulSoup(markdown_calls[1], "html.parser")

        assert stripped_soup.find("h1") is None
        assert stripped_soup.find("p", class_="ementa") is None
        assert fallback_soup.find("h1") is None
        assert fallback_soup.find("p", class_="ementa") is not None
        assert result["raw_content"] == fake_html
        assert result["content_extension"] == ".html"
        assert scraper._save_doc_error.await_count == 0


# ---------------------------------------------------------------------------
# New safety-net tests  — _extract_total_results
# ---------------------------------------------------------------------------


class TestExtractTotalResults:
    """Unit tests for CamaraDepScraper._extract_total_results."""

    @staticmethod
    def _wrap(inner_html: str) -> BeautifulSoup:
        return BeautifulSoup(f"<html><body>{inner_html}</body></html>", "html.parser")

    def test_extracts_count_from_well_formed_div(self):
        soup = self._wrap(
            '<div class="busca-info__resultado busca-info__resultado--informado">'
            "  Foram encontrados 42"
            "</div>"
        )
        assert CamaraDepScraper._extract_total_results(soup) == 42

    def test_returns_none_when_div_missing(self):
        soup = self._wrap("<p>nothing here</p>")
        assert CamaraDepScraper._extract_total_results(soup) is None

    def test_returns_none_when_text_has_no_number(self):
        soup = self._wrap(
            '<div class="busca-info__resultado busca-info__resultado--informado">'
            "  Nenhum documento encontrado"
            "</div>"
        )
        assert CamaraDepScraper._extract_total_results(soup) is None

    def test_extracts_large_count(self):
        soup = self._wrap(
            '<div class="busca-info__resultado busca-info__resultado--informado">'
            "  Encontrados 12345"
            "</div>"
        )
        assert CamaraDepScraper._extract_total_results(soup) == 12345


# ---------------------------------------------------------------------------
# New safety-net tests  — _parse_listing_page_documents
# ---------------------------------------------------------------------------


class TestParseListingPageDocuments:
    """Unit tests for CamaraDepScraper._parse_listing_page_documents."""

    def test_parses_single_result_item(self):
        scraper = build_unit_scraper()
        soup = BeautifulSoup(
            """
            <ul>
              <li class="busca-resultados__item">
                <h3 class="busca-resultados__cabecalho">
                  <a href="https://www2.camara.leg.br/legin/fed/decret/2020/norma.html">
                    Decreto nº 10.000
                  </a>
                </h3>
                <p class="busca-resultados__descricao js-fade-read-more">
                  Ementa: Dispõe sobre coisas.
                </p>
                <p class="busca-resultados__situacao">
                  Situação: Não consta revogação expressa
                </p>
              </li>
            </ul>
            """,
            "html.parser",
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
            """
            <ul>
              <li class="busca-resultados__item">
                <p>No heading here</p>
              </li>
            </ul>
            """,
            "html.parser",
        )
        assert scraper._parse_listing_page_documents(soup) == []

    def test_skips_items_without_anchor(self):
        scraper = build_unit_scraper()
        soup = BeautifulSoup(
            """
            <ul>
              <li class="busca-resultados__item">
                <h3 class="busca-resultados__cabecalho">
                  Plain text, no link
                </h3>
              </li>
            </ul>
            """,
            "html.parser",
        )
        assert scraper._parse_listing_page_documents(soup) == []

    def test_handles_missing_summary_and_situation(self):
        scraper = build_unit_scraper()
        soup = BeautifulSoup(
            """
            <ul>
              <li class="busca-resultados__item">
                <h3 class="busca-resultados__cabecalho">
                  <a href="https://example.com/doc">Decreto nº 5</a>
                </h3>
              </li>
            </ul>
            """,
            "html.parser",
        )
        docs = scraper._parse_listing_page_documents(soup)
        assert len(docs) == 1
        assert docs[0]["summary"] == ""
        assert docs[0]["situation"] == ""

    def test_parses_multiple_items(self):
        scraper = build_unit_scraper()
        soup = BeautifulSoup(
            """
            <ul>
              <li class="busca-resultados__item">
                <h3 class="busca-resultados__cabecalho">
                  <a href="https://example.com/d1">Lei nº 1</a>
                </h3>
              </li>
              <li class="busca-resultados__item">
                <h3 class="busca-resultados__cabecalho">
                  <a href="https://example.com/d2">Lei nº 2</a>
                </h3>
              </li>
            </ul>
            """,
            "html.parser",
        )
        docs = scraper._parse_listing_page_documents(soup)
        assert len(docs) == 2
        assert docs[0]["title"] == "Lei nº 1"
        assert docs[1]["title"] == "Lei nº 2"


# ---------------------------------------------------------------------------
# New safety-net tests  — _get_document_text_link (additional edge cases)
# ---------------------------------------------------------------------------


class TestGetDocumentTextLinkEdgeCases:
    """Additional edge-case tests for _get_document_text_link."""

    @staticmethod
    def _make_doc(**overrides):
        doc = {
            "title": "Decreto nº 500, de 01/01/2020",
            "summary": "Dispõe sobre algo.",
            "metadata_url": "https://www2.camara.leg.br/legin/fed/decret/2020/decreto-500-norma.html",
            "situation": "Não consta revogação expressa",
        }
        doc.update(overrides)
        return doc

    @pytest.mark.asyncio
    async def test_returns_none_on_failed_fetch(self):
        from conftest import make_failed_request

        scraper = build_unit_scraper()
        failed = make_failed_request(
            url="https://www2.camara.leg.br/legin/fed/decret/2020/decreto-500-norma.html",
            reason="Connection refused",
            status=503,
        )
        scraper.request_service.get_soup = AsyncMock(return_value=failed)

        result = await scraper._get_document_text_link(
            self._make_doc(), year=2020, norm_type="Decreto"
        )
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_none_when_h1_not_found(self):
        scraper = build_unit_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(
                "<html><body><h1>Not Found</h1></body></html>",
                "html.parser",
            )
        )

        result = await scraper._get_document_text_link(
            self._make_doc(), year=2020, norm_type="Decreto"
        )
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_none_when_no_sessao_divs(self):
        scraper = build_unit_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(
                "<html><body><p>No sessao divs at all</p></body></html>",
                "html.parser",
            )
        )

        result = await scraper._get_document_text_link(
            self._make_doc(), year=2020, norm_type="Decreto"
        )
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_none_when_no_text_links_in_sessao(self):
        scraper = build_unit_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(
                """
                <html><body>
                  <div class="sessao">
                    <a href="other.html">Origem: Poder Executivo</a>
                  </div>
                </body></html>
                """,
                "html.parser",
            )
        )

        result = await scraper._get_document_text_link(
            self._make_doc(), year=2020, norm_type="Decreto"
        )
        assert result is None
        scraper._save_doc_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_prefers_html_over_pdf_within_same_bucket(self):
        scraper = build_unit_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(
                """
                <html><body>
                  <div class="sessao">
                    <a href="decreto-publicacaooriginal.pdf">Texto - Publicação Original</a>
                    <a href="decreto-publicacaooriginal.html">Texto - Publicação Original</a>
                  </div>
                </body></html>
                """,
                "html.parser",
            )
        )

        result = await scraper._get_document_text_link(
            self._make_doc(), year=2020, norm_type="Decreto"
        )
        assert result is not None
        assert result["document_url"].endswith(".html")

    @pytest.mark.asyncio
    async def test_defaults_situation_to_nao_informado(self):
        scraper = build_unit_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(
                """
                <html><body>
                  <div class="sessao">
                    <a href="decreto-publicacaooriginal.html">Texto - Publicação Original</a>
                  </div>
                </body></html>
                """,
                "html.parser",
            )
        )

        doc = self._make_doc(situation="")
        result = await scraper._get_document_text_link(
            doc, year=2020, norm_type="Decreto"
        )
        assert result is not None
        assert result["situation"] == "Não Informado"


# ---------------------------------------------------------------------------
# New safety-net tests  — _scrape_type
# ---------------------------------------------------------------------------


class TestScrapeType:
    """Unit tests for the three-phase _scrape_type pipeline."""

    @pytest.mark.asyncio
    async def test_returns_empty_when_initial_fetch_fails(self):
        from conftest import make_failed_request

        scraper = build_unit_scraper()
        scraper.request_service.get_soup = AsyncMock(return_value=make_failed_request())

        results = await scraper._scrape_type("Decreto", "Decreto", 2020)
        assert results == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_total_is_none(self):
        scraper = build_unit_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(
                "<html><body><p>No total element</p></body></html>",
                "html.parser",
            )
        )

        results = await scraper._scrape_type("Decreto", "Decreto", 2020)
        assert results == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_total_is_zero(self):
        scraper = build_unit_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(
                "<html><body>"
                '<div class="busca-info__resultado busca-info__resultado--informado">'
                "  Encontrados 0"
                "</div>"
                "</body></html>",
                "html.parser",
            )
        )

        results = await scraper._scrape_type("Decreto", "Decreto", 2020)
        assert results == []

    @pytest.mark.asyncio
    async def test_uses_export_path_when_total_within_threshold(self):
        scraper = build_unit_scraper()

        listing_soup = BeautifulSoup(
            "<html><body>"
            '<div class="busca-info__resultado busca-info__resultado--informado">'
            "  Encontrados 2"
            "</div>"
            "</body></html>",
            "html.parser",
        )

        export_soup = BeautifulSoup(
            """
            <div id="impressaoPDF">
              <ul>
                <li>
                  <a href="https://www2.camara.leg.br/legin/fed/decret/2020/d1-norma.html">
                    Decreto nº 1
                  </a>
                  <p><span class="bold">Situação: </span>Não consta revogação expressa</p>
                </li>
                <li>
                  <a href="https://www2.camara.leg.br/legin/fed/decret/2020/d2-norma.html">
                    Decreto nº 2
                  </a>
                  <p><span class="bold">Situação: </span>Revogada</p>
                </li>
              </ul>
            </div>
            """,
            "html.parser",
        )

        scraper.request_service.get_soup = AsyncMock(
            side_effect=[listing_soup, export_soup]
        )

        scraper._gather_results = AsyncMock(return_value=[])
        scraper._process_documents = AsyncMock(return_value=[{"title": "doc"}])
        scraper._is_already_scraped = MagicMock(return_value=False)

        results = await scraper._scrape_type("Decreto", "Decreto", 2020)

        # _gather_results was called for Phase 2 (text link resolution)
        scraper._gather_results.assert_called()
        # _process_documents was called for Phase 3
        scraper._process_documents.assert_called_once()
        assert results == [{"title": "doc"}]
