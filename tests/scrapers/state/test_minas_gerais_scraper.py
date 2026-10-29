"""Tests for MGAlmgScraper."""

from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.core.scraper import DEFAULT_VALID_SITUATION
from src.scrapers.states.mg import SITUATIONS, TYPES, MGAlmgScraper


def _make_scraper(**kwargs) -> MGAlmgScraper:
    """Instantiate MGAlmgScraper with injected dependencies."""
    return make_scraper(MGAlmgScraper, **kwargs)


def _make_listing_soup(
    rows: list[tuple[str, str, str]],
    count_text: str | None = None,
    page_numbers: list[int] | None = None,
) -> BeautifulSoup:
    articles = ""
    for href, title, summary in rows:
        articles += f'\n        <article class="col-12 p-4">\n            <div class="h4"><a href="{href}">{title}</a></div>\n            {summary}<br/>\n        </article>\n        '
    count_html = (
        f'<div class="col-12 text-end small lh-1 my-2">{count_text}</div>'
        if count_text is not None
        else ""
    )
    pagination_html = ""
    if page_numbers:
        links = "".join(
            (f'<a href="?pagina={page}">{page}</a>' for page in page_numbers)
        )
        pagination_html = f'<ul class="pagination">{links}</ul>'
    return BeautifulSoup(
        f"<html><body>{count_html}{articles}{pagination_html}</body></html>", "lxml"
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
            + "".join((f'<h2 class="d-none">{item}</h2>' for item in hidden_origins))
            + "</div>"
        )
    else:
        origin_block = f"""<div class="pb-5"><span class="text-gray-550 d-block">Origem</span>{origin_text or ""}</div>"""
    accordion_parts = []
    if situation is not None:
        accordion_parts.append(
            f'<span class="text-gray-550 d-block">Situação</span>{situation}'
        )
        accordion_parts.append("<hr/>")
    accordion_parts.append(
        f'<span class="text-gray-550 d-block">Fonte</span><div class="p-0">{publication}</div>'
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
        (f'<div><a href="{href}">{label}</a></div>' for label, href in text_links)
    )
    html = f"<html><body>{origin_block}<div class='accordion-body'>{''.join(accordion_parts)}</div>{links_html}</body></html>"
    return BeautifulSoup(html, "lxml")


def _make_text_soup(
    text: str = "Texto da lei. " * 30, mediaserver_url: str | None = None
) -> BeautifulSoup:
    inner = f"<p>{text}</p>"
    if mediaserver_url:
        inner = f'<p>OBSERVAÇÃO: A imagem da lei está disponível em:</p><a href="{mediaserver_url}">{mediaserver_url}</a>'
    return BeautifulSoup(
        f'<html><body><span class="js_interpretarLinks textNorma">{inner}</span></body></html>',
        "lxml",
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
                "Norma desconhecida", "/legislacao-mineira/IDG/1/2016/"
            )
            == "Instrução Normativa"
        )

    def test_url_code_takes_precedence_over_title_prefix(self):
        scraper = _make_scraper()
        assert (
            scraper._normalize_type(
                "Norma desconhecida", "/legislacao-mineira/LCP/1/2016/"
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
            [("/lei/1", "Lei nº 1", "Resumo")], count_text="1 artigo encontrado"
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
        soup = BeautifulSoup("<html><body></body></html>", "lxml")
        assert scraper._extract_total_pages(soup) == 0
