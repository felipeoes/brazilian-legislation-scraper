from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from bs4 import BeautifulSoup

from src.scraper.base.scraper import StateScraper
from src.scraper.state_legislation.amapa import SITUATIONS, TYPES, AmapaAlapScraper
from src.services.request.service import FailedRequest

from base_tests import SituationsConstantTests, TypesConstantTests
from conftest import make_base_scraper, assert_resume_skips


def _make_scraper(**kwargs) -> AmapaAlapScraper:
    return make_base_scraper(
        AmapaAlapScraper,
        "https://al.ap.leg.br",
        "AMAPA",
        TYPES,
        situations={},
        max_workers=4,
        _global_scraped_keys=set(),
        _global_scraped_keys_loaded=False,
        _pending_flush_years=set(),
        _save_doc_error=AsyncMock(),
        **kwargs,
    )


def _listing_soup(rows_html: str, total: int) -> BeautifulSoup:
    return BeautifulSoup(
        f"""
        <html>
          <body>
            <h1>BUSCAR LEGISLAÇÕES ({total})</h1>
            <table>
              <tbody>{rows_html}</tbody>
            </table>
          </body>
        </html>
        """,
        "html.parser",
    )


def _doc_response(html: str) -> MagicMock:
    response = MagicMock()
    response.__bool__ = lambda self: True
    response.read = AsyncMock(return_value=html.encode("utf-8"))
    return response


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 5
    REQUIRED_KEYS = {
        "Decreto Legislativo",
        "Lei Complementar",
        "Lei Ordinária",
        "Resolução",
        "Emenda Constitucional",
    }
    REQUIRE_INT_VALUES = True


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_situations_contains_nao_consta(self):
        assert SITUATIONS["Não consta"] == "Não consta"


class TestFormatSearchUrl:
    def test_includes_year_and_page(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2020, 2)
        assert "especie_documento=" in url
        assert "ano=2020" in url
        assert "pagina=2" in url

    def test_supports_year_only_search(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2025, 1)
        assert "especie_documento=" in url
        assert "ano=2025" in url


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_parses_mixed_types_and_detail_rows(self):
        scraper = _make_scraper()
        rows_html = """
            <tr>
              <td><a href="pagina.php?pg=buscar_legislacao&especie_documento=13" title="Lei Ordinária">Lei Ordinária</a><br>2555, de 10/05/21</td>
              <td>Dispõe sobre algo importante.</td>
              <td>7414</td>
              <td>10/05/2021</td>
              <td><a href="pagina.php?pg=exibir_processo&iddocumento=100730"><strong>0011/20-AL</strong></a></td>
              <td><a href="ver_texto_lei.php?iddocumento=100730">texto integral</a></td>
            </tr>
            <tr>
              <td colspan="6"><strong>Alterações:</strong> Lei n° 3.311, de 29.09.2025 (revoga). <a href="ver_texto_consolidado.php?iddocumento=100730">Lei consolidada</a></td>
            </tr>
            <tr>
              <td colspan="6"><strong>Observações:</strong> Lei n° 3.311, de 29.09.2025, revoga por consolidação.</td>
            </tr>
            <tr>
              <td><a href="pagina.php?pg=buscar_legislacao&especie_documento=14" title="Decreto Legislativo">Decreto Legislativo</a><br>0971, de 31/03/2020</td>
              <td>Reconhece estado de calamidade pública.</td>
              <td>0995</td>
              <td>31/03/2020</td>
              <td><a href="pagina.php?pg=exibir_processo&iddocumento=101816"><strong>0016/20-AL</strong></a></td>
              <td></td>
            </tr>
            <tr>
              <td colspan="6"><strong>Alterações:</strong> Publicado no DOE/AP 7.135, de 01 de abril de 2020. <a href="ver_texto_consolidado.php?iddocumento=101816">Lei consolidada</a></td>
            </tr>
        """
        scraper.request_service.get_soup = AsyncMock(
            return_value=_listing_soup(rows_html, total=2)
        )

        docs = await scraper._get_docs_links("https://example.com", query_year=2020)

        assert len(docs) == 2
        assert docs[0]["type"] == "Lei Ordinária"
        assert docs[0]["year"] == 2021
        assert docs[0]["query_year"] == 2020
        assert (
            docs[0]["consolidated_link"]
            == "ver_texto_consolidado.php?iddocumento=100730"
        )
        assert "revoga" in docs[0]["alteracoes"]
        assert "consolidação" in docs[0]["observacoes"]

        assert docs[1]["type"] == "Decreto Legislativo"
        assert docs[1]["year"] == 2020
        assert docs[1]["html_link"] == "ver_texto_lei.php?iddocumento=101816"

    @pytest.mark.asyncio
    async def test_uses_title_date_when_publication_date_is_blank(self):
        scraper = _make_scraper()
        rows_html = """
            <tr>
              <td><a href="pagina.php?pg=buscar_legislacao&especie_documento=13" title="Lei Ordinária">Lei Ordinária</a><br>0651, de 05/03/2002</td>
              <td>Dispõe sobre algo antigo.</td>
              <td>1234</td>
              <td></td>
              <td><a href="pagina.php?pg=exibir_processo&iddocumento=651"><strong>0001/00-AL</strong></a></td>
              <td><a href="ver_texto_lei.php?iddocumento=651">texto integral</a></td>
            </tr>
        """
        scraper.request_service.get_soup = AsyncMock(
            return_value=_listing_soup(rows_html, total=1)
        )

        docs = await scraper._get_docs_links("https://example.com", query_year=2000)

        assert docs[0]["publication_date"] == ""
        assert docs[0]["year"] == 2002


class TestGetDocsLinksEdgeCases:
    @pytest.mark.asyncio
    async def test_returns_empty_when_page_has_zero_results_without_tbody(self):
        scraper = _make_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=BeautifulSoup(
                "<html><body><h1>BUSCAR LEGISLAÇÕES (0)</h1><p>Encontramos 0 resutados.</p></body></html>",
                "html.parser",
            )
        )

        docs = await scraper._get_docs_links("https://example.com", query_year=2025)
        assert docs == []

    @pytest.mark.asyncio
    async def test_failed_soup_raises_value_error(self):
        scraper = _make_scraper()
        scraper.request_service.get_soup = AsyncMock(
            return_value=FailedRequest(url="https://example.com", reason="boom")
        )
        with pytest.raises(ValueError):
            await scraper._get_docs_links("https://example.com", query_year=2025)


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_resume_skip_returns_none(self):
        await assert_resume_skips(
            _make_scraper(),
            {
                "title": "Lei Ordinária 2555, de 10/05/21",
                "year": 2021,
                "type": "Lei Ordinária",
                "situation": "Não consta",
                "html_link": "ver_texto_lei.php?iddocumento=100730",
            },
        )

    @pytest.mark.asyncio
    async def test_failed_request_logs_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper.request_service.make_request = AsyncMock(
            return_value=FailedRequest(url="https://example.com", reason="boom")
        )

        result = await scraper._get_doc_data(
            {
                "title": "Lei Ordinária 2555, de 10/05/21",
                "year": 2021,
                "type": "Lei Ordinária",
                "situation": "Não consta",
                "html_link": "ver_texto_lei.php?iddocumento=100730",
            }
        )

        assert result is None
        assert cast(AsyncMock, scraper._save_doc_error).await_count == 1

    @pytest.mark.asyncio
    async def test_falls_back_from_empty_consolidated_to_original_text(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_markdown = AsyncMock(
            return_value="# Lei\n\n" + "Texto da lei. " * 20
        )

        empty_consolidated = """
            <html><body>
              <table><tr><td><a class="texto_noticia3" href="#">imprimir</a></td></tr></table>
              <table><tr><td><img src="brasaoamapa.jpg"></td></tr></table>
              <table><tr><td><table><tr><td></td></tr></table></td></tr></table>
            </body></html>
        """
        original_html = """
            <html><body>
              <table><tr><td><a class="texto_noticia3" href="#">imprimir</a></td></tr></table>
              <table><tr><td><img src="brasaoamapa.jpg"></td></tr></table>
              <table><tr><td><table><tr><td>
                <p><strong>LEI Nº 2.548, DE 23 DE ABRIL DE 2021</strong></p>
                <p>Texto da lei. Texto da lei. Texto da lei. Texto da lei. Texto da lei.</p>
                <p>Texto da lei. Texto da lei. Texto da lei. Texto da lei. Texto da lei.</p>
                <p>Texto da lei. Texto da lei. Texto da lei. Texto da lei. Texto da lei.</p>
                <p>Texto da lei. Texto da lei. Texto da lei. Texto da lei. Texto da lei.</p>
              </td></tr></table></td></tr></table>
            </body></html>
        """

        async def make_request(url: str):
            if "consolidado" in url:
                return _doc_response(empty_consolidated)
            return _doc_response(original_html)

        scraper.request_service.make_request = AsyncMock(side_effect=make_request)

        result = await scraper._get_doc_data(
            {
                "title": "Lei Ordinária 2548, de 23/04/21",
                "year": 2021,
                "type": "Lei Ordinária",
                "situation": "Não consta",
                "html_link": "ver_texto_lei.php?iddocumento=104262",
                "consolidated_link": "ver_texto_consolidado.php?iddocumento=104262",
            }
        )

        assert result is not None
        assert (
            result["document_url"]
            == "https://al.ap.leg.br/ver_texto_lei.php?iddocumento=104262"
        )
        assert (
            result["_mhtml_url"]
            == "https://al.ap.leg.br/ver_texto_lei.php?iddocumento=104262"
        )
        assert "Texto da lei." in result["text_markdown"]

    @pytest.mark.asyncio
    async def test_missing_text_link_logs_error_and_returns_none(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)

        result = await scraper._get_doc_data(
            {
                "title": "Lei Ordinária 2555, de 10/05/21",
                "year": 2021,
                "type": "Lei Ordinária",
                "situation": "Não consta",
            }
        )

        assert result is None
        assert cast(AsyncMock, scraper._save_doc_error).await_count == 1


class TestGlobalResume:
    @pytest.mark.asyncio
    async def test_load_scraped_keys_unions_all_saved_years_once(self, tmp_path: Path):
        (tmp_path / "2021").mkdir()
        (tmp_path / "2023").mkdir()

        saver = MagicMock(save_dir=tmp_path)
        saver.get_scraped_keys = AsyncMock(
            side_effect=[
                {("https://al.ap.leg.br/doc1", "Doc 1")},
                {("https://al.ap.leg.br/doc2", "Doc 2")},
            ]
        )
        scraper = _make_scraper(saver=saver)

        await scraper._load_scraped_keys(2025)
        await scraper._load_scraped_keys(2024)

        assert scraper._scraped_keys == {
            ("https://al.ap.leg.br/doc1", "Doc 1"),
            ("https://al.ap.leg.br/doc2", "Doc 2"),
        }
        assert saver.get_scraped_keys.await_count == 2

    @pytest.mark.asyncio
    async def test_save_doc_result_updates_global_keys(self, monkeypatch):
        scraper = _make_scraper()

        async def fake_save(self, doc_result):
            return {
                "document_url": "https://al.ap.leg.br/doc",
                "title": "Doc",
                "year": 2024,
            }

        monkeypatch.setattr(StateScraper, "_save_doc_result", fake_save, raising=False)

        saved = await scraper._save_doc_result({"title": "Doc"})

        assert saved == {
            "document_url": "https://al.ap.leg.br/doc",
            "title": "Doc",
            "year": 2024,
        }
        assert ("https://al.ap.leg.br/doc", "Doc") in scraper._global_scraped_keys
        assert scraper._pending_flush_years == {2024}


class TestScrapeYear:
    @pytest.mark.asyncio
    async def test_year_only_flow_processes_docs_and_flushes_touched_years(self):
        saver = MagicMock()
        scraper = _make_scraper(saver=saver)
        docs = [{"title": "Doc", "html_link": "ver_texto_lei.php?iddocumento=1"}]
        scraper._get_year_documents = AsyncMock(return_value=docs)
        scraper._process_documents = AsyncMock(return_value=[{"title": "Doc"}])
        scraper._flush_touched_years = AsyncMock()

        result = await scraper._scrape_year(2025)

        assert result == [{"title": "Doc"}]
        scraper._get_year_documents.assert_called_once_with(2025)
        scraper._process_documents.assert_called_once()
        cast(AsyncMock, scraper._flush_touched_years).assert_awaited_once()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_docs_links_year_2025_returns_results():
    with TemporaryDirectory() as tmp_dir:
        scraper = AmapaAlapScraper(docs_save_dir=Path(tmp_dir), verbose=False)
        try:
            url = scraper._format_search_url(2025, 1)
            docs = await scraper._get_docs_links(url, query_year=2025)
            assert docs
            assert docs[0]["type"] in TYPES
            assert isinstance(docs[0]["year"], int)
        finally:
            await scraper.cleanup()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_doc_data_returns_valid_markdown():
    with TemporaryDirectory() as tmp_dir:
        scraper = AmapaAlapScraper(docs_save_dir=Path(tmp_dir), verbose=False)
        try:
            url = scraper._format_search_url(2025, 1)
            docs = await scraper._get_docs_links(url, query_year=2025)
            doc = next(doc for doc in docs if doc.get("html_link"))
            result = await scraper._get_doc_data(doc)
            assert result is not None
            assert len(result["text_markdown"]) > 50
        finally:
            await scraper.cleanup()
