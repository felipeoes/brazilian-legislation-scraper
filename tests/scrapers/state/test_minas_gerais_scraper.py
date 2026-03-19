"""Tests for MGAlmgScraper."""

import tempfile
from unittest.mock import AsyncMock, MagicMock

import pytest
from base_tests import ScraperClassTests, SituationsConstantTests, TypesConstantTests
from bs4 import BeautifulSoup
from conftest import make_base_scraper, make_failed_request

import src.scraper.state_legislation.minas_gerais as minas_gerais_module
from src.scraper.base.scraper import DEFAULT_VALID_SITUATION
from src.scraper.state_legislation.minas_gerais import (
    SITUATIONS,
    TYPES,
    MGAlmgScraper,
)


def _make_scraper(**kwargs) -> MGAlmgScraper:
    """Instantiate MGAlmgScraper bypassing __init__ (no network, no I/O)."""
    return make_base_scraper(
        MGAlmgScraper,
        "https://www.almg.gov.br",
        "MINAS_GERAIS",
        TYPES,
        situations=SITUATIONS,
        **kwargs,
    )


def _make_listing_soup(
    rows: list[tuple[str, str, str]],
    count_text: str | None = None,
    page_numbers: list[int] | None = None,
) -> BeautifulSoup:
    articles = ""
    for href, title, summary in rows:
        articles += f"""
        <article class="col-12 p-4">
            <div class="h4"><a href="{href}">{title}</a></div>
            {summary}<br/>
        </article>
        """

    count_html = (
        f'<div class="col-12 text-end small lh-1 my-2">{count_text}</div>'
        if count_text is not None
        else ""
    )
    pagination_html = ""
    if page_numbers:
        links = "".join(f'<a href="?pagina={page}">{page}</a>' for page in page_numbers)
        pagination_html = f'<ul class="pagination">{links}</ul>'

    return BeautifulSoup(
        f"<html><body>{count_html}{articles}{pagination_html}</body></html>",
        "html.parser",
    )


def _make_detail_soup(
    *,
    text_links: list[tuple[str, str]] | None = None,
    origin_text: str | None = "Executivo",
    hidden_origins: list[str] | None = None,
    situation: str | None = None,
    publication: str = "Publicação - Minas Gerais Diário do Executivo - 31/12/2022 Pág. 2 Col. 2",
    tags: str | None = None,
    subject: str | None = None,
) -> BeautifulSoup:
    text_links = text_links or [
        ("Texto original", "/legislacao-mineira/texto/DEC/1/2022/")
    ]

    if hidden_origins:
        origin_block = (
            '<div class="pb-5"><span class="text-gray-550 d-block">Origem</span>'
            + "".join(f'<h2 class="d-none">{item}</h2>' for item in hidden_origins)
            + "</div>"
        )
    else:
        origin_block = (
            '<div class="pb-5"><span class="text-gray-550 d-block">Origem</span>'
            f"{origin_text or ''}</div>"
        )

    accordion_parts = []
    if situation is not None:
        accordion_parts.append(
            f'<span class="text-gray-550 d-block">Situação</span>{situation}'
        )
        accordion_parts.append("<hr/>")

    accordion_parts.append(
        '<span class="text-gray-550 d-block">Fonte</span>'
        f'<div class="p-0">{publication}</div>'
    )
    if tags is not None:
        accordion_parts.append("<hr/>")
        accordion_parts.append(
            f'<span class="text-gray-550 d-block">Resumo</span>{tags}'
        )
    if subject is not None:
        accordion_parts.append("<hr/>")
        accordion_parts.append(
            f'<span class="text-gray-550 d-block">Assunto Geral</span>{subject}'
        )

    links_html = "".join(
        f'<div><a href="{href}">{label}</a></div>' for label, href in text_links
    )
    html = f"<html><body>{origin_block}<div class='accordion-body'>{''.join(accordion_parts)}</div>{links_html}</body></html>"
    return BeautifulSoup(html, "html.parser")


def _make_text_soup(
    text: str = "Texto da lei. " * 30,
    mediaserver_url: str | None = None,
) -> BeautifulSoup:
    inner = f"<p>{text}</p>"
    if mediaserver_url:
        inner = (
            "<p>OBSERVAÇÃO: A imagem da lei está disponível em:</p>"
            f'<a href="{mediaserver_url}">{mediaserver_url}</a>'
        )
    return BeautifulSoup(
        f'<html><body><span class="js_interpretarLinks textNorma">{inner}</span></body></html>',
        "html.parser",
    )


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 15
    REQUIRED_KEYS = {"Constituição Estadual", "Lei"}
    REQUIRE_INT_VALUES = True

    def test_expected_new_types_present(self):
        assert "Ato das Disposições Constitucionais Transitórias" in TYPES
        assert "Instrução Normativa" in TYPES

    def test_expected_core_types_present(self):
        for norm_type in [
            "Constituição Estadual",
            "Decreto",
            "Emenda Constitucional",
            "Lei",
            "Lei Complementar",
        ]:
            assert norm_type in TYPES


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_contains_expected_canonical_situations(self):
        assert DEFAULT_VALID_SITUATION in SITUATIONS
        assert "Revogada" in SITUATIONS
        assert "Declarada inconstitucional" in SITUATIONS
        assert "Tornada sem efeito" in SITUATIONS


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = MGAlmgScraper
    STATE_NAME = "Minas Gerais"

    def test_situations_available_in_instance(self):
        scraper = _make_scraper()
        assert scraper.situations == SITUATIONS


class TestBuildSearchUrl:
    def test_url_contains_ano(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2022, 1)
        assert "ano=2022" in url

    def test_url_contains_pagina(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2022, 3)
        assert "pagina=3" in url

    def test_url_starts_with_base_url(self):
        scraper = _make_scraper()
        assert scraper._build_search_url(2022, 1).startswith(scraper.base_url)

    def test_url_has_legislacao_mineira(self):
        scraper = _make_scraper()
        assert "legislacao-mineira" in scraper._build_search_url(2022, 1)

    def test_url_does_not_filter_by_type_or_situation(self):
        scraper = _make_scraper()
        url = scraper._build_search_url(2022, 1)
        assert "grupo=" not in url
        assert "sit=" not in url


class TestNormalizeType:
    def test_decreto_com_numeracao_especial_maps_to_decreto(self):
        scraper = _make_scraper()
        assert (
            scraper._normalize_type(
                "Decreto com Numeração Especial nº 864, de 30/12/2022"
            )
            == "Decreto"
        )

    def test_decreto_sem_numero_maps_to_decreto(self):
        scraper = _make_scraper()
        assert (
            scraper._normalize_type("Decreto sem Número nº 2.213, de 13/10/1831")
            == "Decreto"
        )

    def test_emenda_maps_to_canonical_label(self):
        scraper = _make_scraper()
        assert (
            scraper._normalize_type("Emenda à Constituição nº 111, de 29/06/2022")
            == "Emenda Constitucional"
        )

    def test_url_code_fallback_maps_idg_to_instrucao_normativa(self):
        scraper = _make_scraper()
        assert (
            scraper._normalize_type(
                "Norma desconhecida",
                "/legislacao-mineira/IDG/1/2016/",
            )
            == "Instrução Normativa"
        )

    def test_url_code_takes_precedence_over_title_prefix(self):
        scraper = _make_scraper()
        assert (
            scraper._normalize_type(
                "Norma desconhecida",
                "/legislacao-mineira/LCP/1/2016/",
            )
            == "Lei Complementar"
        )

    def test_unknown_prefix_falls_back_to_title_prefix(self):
        scraper = _make_scraper()
        assert (
            scraper._normalize_type("Norma Especial nº 1, de 2024") == "Norma Especial"
        )


class TestExtractTotalPages:
    def test_plural_count_uses_page_size_10(self):
        scraper = _make_scraper()
        soup = _make_listing_soup([], count_text="25 artigos encontrados")
        assert scraper._extract_total_pages(soup) == 3

    def test_singular_count_returns_one_page(self):
        scraper = _make_scraper()
        soup = _make_listing_soup(
            [("/lei/1", "Lei nº 1", "Resumo")],
            count_text="1 artigo encontrado",
        )
        assert scraper._extract_total_pages(soup) == 1

    def test_pagination_fallback_uses_largest_page_number(self):
        scraper = _make_scraper()
        soup = _make_listing_soup(
            [("/lei/1", "Lei nº 1", "Resumo")],
            count_text=None,
            page_numbers=[1, 2, 3, 4],
        )
        assert scraper._extract_total_pages(soup) == 4

    def test_empty_page_returns_zero(self):
        scraper = _make_scraper()
        soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        assert scraper._extract_total_pages(soup) == 0


class TestGetDocsLinks:
    @pytest.mark.asyncio
    async def test_happy_path_returns_mixed_docs_with_canonical_types(self):
        scraper = _make_scraper()
        soup = _make_listing_soup(
            [
                (
                    "/legislacao-mineira/DNE/864/2022/",
                    "Decreto com Numeração Especial nº 864, de 30/12/2022",
                    "Abre crédito suplementar no valor de R$14.500.000,00.",
                ),
                (
                    "/legislacao-mineira/LEI/24269/2022/",
                    "Lei nº 24.269, de 29/12/2022",
                    "Institui política pública estadual.",
                ),
            ],
            count_text="2 artigos encontrados",
        )
        scraper.request_service.get_soup = AsyncMock(return_value=soup)

        docs = await scraper._get_docs_links("http://example.com")

        assert len(docs) == 2
        assert docs[0]["type"] == "Decreto"
        assert docs[1]["type"] == "Lei"
        assert (
            docs[0]["summary"]
            == "Abre crédito suplementar no valor de R$14.500.000,00."
        )

    @pytest.mark.asyncio
    async def test_failed_soup_returns_empty_list(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(return_value=failed)

        assert await scraper._get_docs_links("http://example.com") == []


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_resume_skip_on_document_page_returns_none(self):
        scraper = _make_scraper()
        scraper.request_service.get_soup = AsyncMock(return_value=_make_detail_soup())
        scraper._is_already_scraped = MagicMock(return_value=True)

        result = await scraper._get_doc_data(
            {"title": "Decreto nº 1", "summary": "Resumo", "html_link": "/lei/1"}
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_failed_detail_soup_returns_none(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(return_value=failed)
        scraper._save_doc_error = AsyncMock()

        result = await scraper._get_doc_data(
            {"title": "Decreto nº 1", "summary": "Resumo", "html_link": "/lei/1"}
        )

        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_text_link_returns_none(self):
        scraper = _make_scraper()
        detail_soup = _make_detail_soup(text_links=[])
        scraper.request_service.get_soup = AsyncMock(return_value=detail_soup)
        scraper._save_doc_error = AsyncMock()

        result = await scraper._get_doc_data(
            {"title": "Lei nº 1", "summary": "Resumo", "html_link": "/lei/1"}
        )

        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_link_resolves_to_base_url_returns_none(self):
        scraper = _make_scraper()
        detail_soup = _make_detail_soup(text_links=[("Texto atualizado", "/")])
        scraper.request_service.get_soup = AsyncMock(return_value=detail_soup)
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()

        result = await scraper._get_doc_data(
            {"title": "Lei nº 1", "summary": "Resumo", "html_link": "/lei/1"}
        )

        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_text_norma_span_returns_none(self):
        scraper = _make_scraper()
        detail_soup = _make_detail_soup()
        text_soup = BeautifulSoup(
            "<html><body><div>no span here</div></body></html>", "html.parser"
        )
        scraper.request_service.get_soup = AsyncMock(return_value=detail_soup)
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(text_soup, b""))
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()

        result = await scraper._get_doc_data(
            {
                "title": "Lei nº 1",
                "summary": "Resumo",
                "html_link": "/lei/1",
                "type": "Lei",
            }
        )

        assert result is None
        scraper._save_doc_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_falls_back_to_original_text_when_updated_page_has_no_span(self):
        scraper = _make_scraper()
        detail_soup = _make_detail_soup(
            text_links=[
                ("Texto atualizado", "/legislacao-mineira/texto/LEI/1/2022/?cons=1"),
                ("Texto original", "/legislacao-mineira/texto/LEI/1/2022/"),
            ]
        )
        bad_updated = BeautifulSoup(
            "<html><body><div>sem texto</div></body></html>", "html.parser"
        )
        original_text = _make_text_soup()
        scraper.request_service.get_soup = AsyncMock(return_value=detail_soup)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            side_effect=[(bad_updated, b""), (original_text, b"fake-mhtml")]
        )
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_markdown = AsyncMock(return_value="# Lei\n\n" + "texto " * 30)

        result = await scraper._get_doc_data(
            {
                "title": "Lei nº 1, de 01/01/2022",
                "summary": "Resumo",
                "html_link": "/legislacao-mineira/LEI/1/2022/",
                "type": "Lei",
            }
        )

        assert result is not None
        assert result["document_url"].endswith("/legislacao-mineira/texto/LEI/1/2022/")

    @pytest.mark.asyncio
    async def test_retries_access_denied_text_page(self, monkeypatch):
        scraper = _make_scraper()
        detail_soup = _make_detail_soup(
            text_links=[("Texto original", "/legislacao-mineira/texto/LEI/1/2022/")]
        )
        denied_soup = BeautifulSoup(
            "<html><head><title>Acesso Proibido</title></head><body></body></html>",
            "html.parser",
        )
        text_soup = _make_text_soup()
        scraper.request_service.get_soup = AsyncMock(return_value=detail_soup)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            side_effect=[(denied_soup, b""), (text_soup, b"fake-mhtml")]
        )
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_markdown = AsyncMock(return_value="# Lei\n\n" + "texto " * 30)
        sleep_mock = AsyncMock()
        monkeypatch.setattr(minas_gerais_module.asyncio, "sleep", sleep_mock)

        result = await scraper._get_doc_data(
            {
                "title": "Lei nº 1, de 01/01/2022",
                "summary": "Resumo",
                "html_link": "/legislacao-mineira/LEI/1/2022/",
                "type": "Lei",
            }
        )

        assert result is not None
        assert sleep_mock.await_count == 1

    @pytest.mark.asyncio
    async def test_valid_html_doc_returns_correct_shape(self):
        scraper = _make_scraper()
        detail_soup = _make_detail_soup(
            hidden_origins=["PL PROJETO DE LEI 3293/2021"],
            tags="Utilidade Pública, Entidade, Município.",
            subject="Utilidade Pública.",
        )
        text_soup = _make_text_soup()
        scraper.request_service.get_soup = AsyncMock(return_value=detail_soup)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(text_soup, b"fake-mhtml")
        )
        scraper._is_already_scraped = MagicMock(return_value=False)
        valid_md = "# Lei\n\n" + ("Texto da lei. " * 20).strip()
        scraper._get_markdown = AsyncMock(return_value=valid_md)

        result = await scraper._get_doc_data(
            {
                "title": "Lei nº 24.269, de 29/12/2022",
                "summary": "Institui política pública estadual.",
                "html_link": "/legislacao-mineira/LEI/24269/2022/",
                "type": "Lei",
            }
        )

        assert result is not None
        assert result["type"] == "Lei"
        assert result["origin"] == "PL PROJETO DE LEI 3293/2021"
        assert result["situation"] == DEFAULT_VALID_SITUATION
        assert result["publication"].startswith(
            "Publicação - Minas Gerais Diário do Executivo"
        )
        assert result["tags"] == "Utilidade Pública, Entidade, Município."
        assert result["subject"] == "Utilidade Pública."
        assert result["text_markdown"] == valid_md
        assert result["document_url"].endswith("/legislacao-mineira/texto/DEC/1/2022/")
        assert result["_content_extension"] == ".mhtml"
        assert result["_raw_content"] == b"fake-mhtml"

    @pytest.mark.asyncio
    async def test_uppercase_situation_is_normalized(self):
        scraper = _make_scraper()
        detail_soup = _make_detail_soup(situation="DECLARADA INCONSTITUCIONAL")
        text_soup = _make_text_soup()
        scraper.request_service.get_soup = AsyncMock(return_value=detail_soup)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(text_soup, b"fake-mhtml")
        )
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_markdown = AsyncMock(return_value="# Norma\n\n" + "texto " * 30)

        result = await scraper._get_doc_data(
            {
                "title": "Lei nº 23.993, de 25/11/2021 (Declarada inconstitucional)",
                "summary": "Resumo",
                "html_link": "/legislacao-mineira/LEI/23993/2021/",
                "type": "Lei",
            }
        )

        assert result is not None
        assert result["situation"] == "Declarada inconstitucional"

    @pytest.mark.asyncio
    async def test_title_status_hint_is_used_when_accordion_has_no_situation(self):
        scraper = _make_scraper()
        detail_soup = _make_detail_soup(situation=None)
        text_soup = _make_text_soup()
        scraper.request_service.get_soup = AsyncMock(return_value=detail_soup)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(text_soup, b"fake-mhtml")
        )
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_markdown = AsyncMock(return_value="# Norma\n\n" + "texto " * 30)

        result = await scraper._get_doc_data(
            {
                "title": "Portaria nº 53, de 14/12/2022 (Revogada)",
                "summary": "Resumo",
                "html_link": "/legislacao-mineira/PRT/53/2022/",
                "type": "Portaria",
            }
        )

        assert result is not None
        assert result["situation"] == "Revogada"

    @pytest.mark.asyncio
    async def test_html_text_is_preferred_when_pdf_is_only_annex(self):
        scraper = _make_scraper()
        pdf_url = "https://mediaserver.almg.gov.br/acervo/498/24/2498024.pdf"
        detail_soup = _make_detail_soup(
            text_links=[("Texto original", "/legislacao-mineira/texto/PRT/67/2025/")]
        )
        text_soup = BeautifulSoup(
            """
            <html><body>
            <span class="js_interpretarLinks textNorma">
              <div>
                <p>Aprova o calendario de funcionamento.</p>
                <p>Art. 1 - Fica aprovado o calendario de funcionamento da Assembleia.</p>
                <p>Art. 2 - Esta portaria entra em vigor na data de sua publicacao.</p>
                <p>ANEXO</p>
                <p>(a que se refere o art. 1o da Portaria Psec/DGE no 67)</p>
                <p>Observacao: A imagem do anexo esta disponivel em: <a href="""
            + pdf_url
            + """">PDF</a></p>
              </div>
            </span>
            </body></html>
            """,
            "html.parser",
        )
        scraper.request_service.get_soup = AsyncMock(return_value=detail_soup)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(text_soup, b"fake-mhtml")
        )
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._download_and_convert = AsyncMock()
        scraper._get_markdown = AsyncMock(
            return_value="# Portaria\n\n" + "Art. 1 - texto suficiente. " * 10
        )

        result = await scraper._get_doc_data(
            {
                "title": "Portaria nº 67, de 04/12/2025",
                "summary": "Resumo",
                "html_link": "/legislacao-mineira/PRT/67/2025/",
                "type": "Portaria",
            }
        )

        assert result is not None
        assert result["document_url"].endswith("/legislacao-mineira/texto/PRT/67/2025/")
        assert result["_content_extension"] == ".mhtml"
        scraper._download_and_convert.assert_not_called()
        html_content = scraper._get_markdown.call_args.kwargs["html_content"]
        assert "Observacao" not in html_content
        assert "mediaserver" not in html_content
        assert "ANEXO" not in html_content

    @pytest.mark.asyncio
    async def test_pdf_image_doc_uses_download_and_convert(self):
        scraper = _make_scraper()
        pdf_url = "https://mediaserver.almg.gov.br/acervo/123.pdf"
        detail_soup = _make_detail_soup()
        text_soup = _make_text_soup(mediaserver_url=pdf_url)
        scraper.request_service.get_soup = AsyncMock(return_value=detail_soup)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(text_soup, b"fake-mhtml")
        )
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._download_and_convert = AsyncMock(
            return_value=("# PDF\n\n" + "texto suficiente " * 10, b"%PDF", ".pdf")
        )

        result = await scraper._get_doc_data(
            {
                "title": "Lei nº 301, de 04/09/1900",
                "summary": "Resumo",
                "html_link": "/legislacao-mineira/LEI/301/1900/",
                "type": "Lei",
            }
        )

        assert result is not None
        assert result["document_url"] == pdf_url
        assert result["_content_extension"] == ".pdf"
        assert result["text_markdown"].startswith("# PDF")

    @pytest.mark.asyncio
    async def test_malformed_share_link_after_timestamp_is_removed_from_html(self):
        scraper = _make_scraper()
        detail_soup = _make_detail_soup(
            text_links=[("Texto original", "/legislacao-mineira/texto/DCS/28/2025/")]
        )
        text_soup = BeautifulSoup(
            """
            <html><body>
            <span class="js_interpretarLinks textNorma">
              <div>
                <p>informou que (trecho a partir de 00:39:28s
                https://www.alma.gov.br/atividade-oarlamentar/comissoes/reuniao/?
                idTipo=1&amp;idCom=1&amp;dia=18&amp;mes=11&amp;ano=2025&amp;hr=14:00&amp;utm source=WhatsApp&amp;utm_medium=BtnCompartilhar&amp;utm_campaign=Compartilhar ):</p>
                <p>"(...) Aqui esta o texto normativo relevante."</p>
              </div>
            </span>
            </body></html>
            """,
            "html.parser",
        )
        scraper.request_service.get_soup = AsyncMock(return_value=detail_soup)
        scraper._fetch_soup_and_mhtml = AsyncMock(
            return_value=(text_soup, b"fake-mhtml")
        )
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._get_markdown = AsyncMock(return_value="# Decisao\n\n" + "texto " * 30)

        result = await scraper._get_doc_data(
            {
                "title": "Decisao nº 28, de 05/12/2025",
                "summary": "Resumo",
                "html_link": "/legislacao-mineira/DCS/28/2025/",
                "type": "Decisão",
            }
        )

        assert result is not None
        html_content = scraper._get_markdown.call_args.kwargs["html_content"]
        assert "utm_" not in html_content
        assert "BtnCompartilhar" not in html_content
        assert "https://www.alma.gov.br" not in html_content
        assert "(trecho a partir de 00:39:28s):" in html_content

    @pytest.mark.asyncio
    async def test_failed_detail_soup_passes_norm_type_to_error(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(return_value=failed)
        scraper._save_doc_error = AsyncMock()

        await scraper._get_doc_data(
            {"title": "Lei nº 1", "summary": "x", "html_link": "/lei/1", "type": "Lei"}
        )

        call_kwargs = scraper._save_doc_error.call_args.kwargs
        assert call_kwargs.get("norm_type") == "Lei"

    @pytest.mark.asyncio
    async def test_no_text_link_passes_norm_type_to_error(self):
        scraper = _make_scraper()
        detail_soup = _make_detail_soup(text_links=[])
        scraper.request_service.get_soup = AsyncMock(return_value=detail_soup)
        scraper._save_doc_error = AsyncMock()

        await scraper._get_doc_data(
            {"title": "Lei nº 1", "summary": "x", "html_link": "/lei/1", "type": "Lei"}
        )

        call_kwargs = scraper._save_doc_error.call_args.kwargs
        assert call_kwargs.get("norm_type") == "Lei"

    @pytest.mark.asyncio
    async def test_no_text_norma_span_passes_norm_type_to_error(self):
        scraper = _make_scraper()
        detail_soup = _make_detail_soup()
        text_soup = BeautifulSoup(
            "<html><body><div>no span here</div></body></html>", "html.parser"
        )
        scraper.request_service.get_soup = AsyncMock(return_value=detail_soup)
        scraper._fetch_soup_and_mhtml = AsyncMock(return_value=(text_soup, b""))
        scraper._is_already_scraped = MagicMock(return_value=False)
        scraper._save_doc_error = AsyncMock()

        await scraper._get_doc_data(
            {"title": "Lei nº 1", "summary": "x", "html_link": "/lei/1", "type": "Lei"}
        )

        call_kwargs = scraper._save_doc_error.call_args.kwargs
        assert call_kwargs.get("norm_type") == "Lei"


class TestScrapeYear:
    @pytest.mark.asyncio
    async def test_scrape_year_fetches_first_page_then_remaining_pages(self):
        scraper = _make_scraper()
        first_soup = _make_listing_soup(
            [("/legislacao-mineira/DEC/1/2022/", "Decreto nº 1", "Resumo 1")],
            count_text="21 artigos encontrados",
        )
        scraper.request_service.get_soup = AsyncMock(return_value=first_soup)
        scraper._get_docs_links = AsyncMock(
            side_effect=[
                [{"title": "doc1", "html_link": "/1", "type": "Decreto"}],
                [{"title": "doc2", "html_link": "/2", "type": "Lei"}],
                [{"title": "doc3", "html_link": "/3", "type": "Portaria"}],
            ]
        )
        scraper._process_documents = AsyncMock(return_value=[{"title": "saved"}])

        result = await scraper._scrape_year(2022)

        assert result == [{"title": "saved"}]
        assert scraper._get_docs_links.call_count == 3
        process_docs = scraper._process_documents.call_args.args[0]
        assert len(process_docs) == 3
        assert {doc["title"] for doc in process_docs} == {"doc1", "doc2", "doc3"}

    @pytest.mark.asyncio
    async def test_failed_first_page_returns_empty(self):
        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(return_value=failed)

        assert await scraper._scrape_year(2022) == []

    @pytest.mark.asyncio
    async def test_failed_first_page_logs_warning(self):
        from unittest.mock import patch

        scraper = _make_scraper()
        failed = make_failed_request()
        scraper.request_service.get_soup = AsyncMock(return_value=failed)

        with patch("src.scraper.state_legislation.minas_gerais.logger") as mock_log:
            await scraper._scrape_year(2022)

        mock_log.warning.assert_called_once()
        assert "Failed to fetch page 1" in mock_log.warning.call_args[0][0]


@pytest.mark.integration
async def test_get_docs_links_year_2022_returns_results():
    with tempfile.TemporaryDirectory() as tmp:
        scraper = MGAlmgScraper(docs_save_dir=tmp, verbose=False)
        try:
            url = scraper._build_search_url(2022, 1)
            docs = await scraper._get_docs_links(url)

            assert isinstance(docs, list)
            assert len(docs) > 0
            assert docs[0]["type"] in TYPES
            assert "title" in docs[0]
            assert "html_link" in docs[0]
        finally:
            await scraper.cleanup()


@pytest.mark.integration
async def test_get_doc_data_returns_valid_markdown():
    with tempfile.TemporaryDirectory() as tmp:
        scraper = MGAlmgScraper(docs_save_dir=tmp, verbose=False)
        try:
            url = scraper._build_search_url(2022, 1)
            docs = await scraper._get_docs_links(url)
            assert len(docs) > 0

            result = await scraper._get_doc_data(docs[0])

            assert result is not None
            assert result["type"] in TYPES
            assert result["text_markdown"] is not None
            assert len(result["text_markdown"]) > 50
        finally:
            await scraper.cleanup()
