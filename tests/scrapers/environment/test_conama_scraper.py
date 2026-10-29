"Parser and metadata regression tests for conama_scraper."

import re
from unittest.mock import AsyncMock, MagicMock

from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import TypesConstantTests

from src.scrapers.environment.conama import TYPES, ConamaScraper


def _make_scraper(**kwargs) -> ConamaScraper:
    """Instantiate ConamaScraper bypassing heavy __init__ (no network, no I/O)."""
    return make_scraper(
        ConamaScraper,
        params={"option": "com_sisconama", "order": "asc", "limit": 100},
        _situation_regex=re.compile("Revogad|Revogação", re.IGNORECASE),
        **kwargs,
    )


def _make_api_response(rows: list[dict], total: int | None = None) -> MagicMock:
    """Mock aiohttp response returning a CONAMA-shaped JSON payload."""
    resp = MagicMock()
    resp.__bool__ = lambda s: True
    payload = {
        "data": {"rows": rows, "total": total if total is not None else len(rows)}
    }
    resp.json = AsyncMock(return_value=payload)
    resp.content_type = "application/json"
    return resp


def _make_http_response(
    body: bytes, content_type: str = "application/pdf"
) -> MagicMock:
    """Mock aiohttp response for a document download."""
    resp = MagicMock()
    resp.__bool__ = lambda s: True
    resp.read = AsyncMock(return_value=body)
    resp.content_type = content_type
    return resp


def _sample_row(**overrides) -> dict:
    base = {
        "id": 1,
        "id_tipo_ato": 1,
        "numero": "265",
        "ano": 2000,
        "descricao": "Dispõe sobre algo.",
        "status": "Vigente",
        "nomeato": "Resolução",
        "aid": 263,
        "porigem": None,
        "palavra_chave": None,
    }
    base.update(overrides)
    return base


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 6
    REQUIRED_KEYS = {
        "Resolução",
        "Moção",
        "Recomendação",
        "Proposição",
        "Decisão",
        "Portaria",
    }
    REQUIRE_INT_VALUES = True

    def test_resolucao_is_type_1(self):
        assert TYPES["Resolução"] == 1

    def test_portaria_is_type_6(self):
        assert TYPES["Portaria"] == 6


class TestFormatSearchUrl:
    def test_contains_no_tipo_parameter(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(offset=0, year="2000")
        assert "tipo" not in url

    def test_contains_year(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(offset=0, year="2000")
        assert "ano=2000" in url

    def test_contains_offset(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(offset=100, year="2000")
        assert "offset=100" in url

    def test_contains_limit_100(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(offset=0, year="2000")
        assert "limit=100" in url

    def test_contains_correct_task(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(offset=0, year="1990")
        assert "task=atosnormativos.getList" in url

    def test_starts_with_base_url(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(offset=0, year="2005")
        assert url.startswith("https://conama.mma.gov.br/")

    def test_default_offset_is_zero(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(year="2010")
        assert "offset=0" in url


class TestCleanDouHtml:
    def _soup(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "lxml")

    def test_removes_script_tags(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><body><script>alert(1)</script><p>texto</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert soup.find("script") is None
        assert "texto" in soup.get_text()

    def test_removes_title_tag(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><head><title>DOU - Imprensa Nacional</title></head><body><p>conteúdo</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert soup.find("title") is None

    def test_removes_element_with_dou_garbage_string(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><body><p>DOU - Imprensa Nacional</p><p>conteúdo legal</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert "DOU - Imprensa Nacional" not in soup.get_text()
        assert "conteúdo legal" in soup.get_text()

    def test_does_not_remove_long_element_containing_garbage_string(self):
        """Elements >300 chars containing garbage should NOT be removed."""
        scraper = _make_scraper()
        long_text = "DOU - Imprensa Nacional " + "texto legal " * 30
        soup = self._soup(f"<html><body><p>{long_text}</p></body></html>")
        scraper._clean_dou_html(soup)
        assert "texto legal" in soup.get_text()

    def test_removes_publicado_em_pattern(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><body><p>Publicado em: 01/01/2000</p><p>Art. 1°</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert "Publicado em:" not in soup.get_text()
        assert "Art. 1°" in soup.get_text()

    def test_removes_orgao_pattern(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><body><span>Órgão: Ministério X</span><p>conteúdo</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert "Órgão:" not in soup.get_text()

    def test_removes_timestamp_pattern(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><body><span>25/03/2020, 10:30</span><p>conteúdo</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert "25/03/2020" not in soup.get_text()

    def test_removes_page_fraction_pattern(self):
        scraper = _make_scraper()
        soup = self._soup("<html><body><span>1/3</span><p>conteúdo</p></body></html>")
        scraper._clean_dou_html(soup)
        assert "1/3" not in soup.get_text()

    def test_removes_disclaimer_pattern(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><body><p>Este texto não substitui o publicado no DOU.</p><p>Art. 1°</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert "não substitui" not in soup.get_text()

    def test_removes_publicacao_dou_reference_element(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><body><p>Art. 1°</p><p>o publicado no Publicação DOU nº 063, de 01/04/1996, pág. 5439</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert "Publicação DOU" not in soup.get_text()
        assert "Art. 1°" in soup.get_text()

    def test_removes_boletim_servico_reference_element(self):
        scraper = _make_scraper()
        soup = self._soup(
            "<html><body><p>Art. 1°</p><p>Publicado no Boletim de Serviço nº 939, de 23/11/84, do Ministério do Interior.</p></body></html>"
        )
        scraper._clean_dou_html(soup)
        assert "Boletim de Serviço" not in soup.get_text()
        assert "Art. 1°" in soup.get_text()

    def test_unwraps_non_dou_links(self):
        scraper = _make_scraper()
        soup = self._soup('<html><body><a href="/outro">Art. 1°</a></body></html>')
        scraper._clean_dou_html(soup)
        assert soup.find("a") is None
        assert "Art. 1°" in soup.get_text()

    def test_removes_in_gov_br_links(self):
        scraper = _make_scraper()
        soup = self._soup(
            '<html><body><a href="https://in.gov.br/materia/123">link</a></body></html>'
        )
        scraper._clean_dou_html(soup)
        assert soup.find("a") is None
        assert "link" not in soup.get_text()

    def test_returns_soup_for_chaining(self):
        scraper = _make_scraper()
        soup = self._soup("<html><body><p>texto</p></body></html>")
        result = scraper._clean_dou_html(soup)
        assert result is soup


class TestCleanPdfMarkdown:
    def test_removes_form_feed(self):
        scraper = _make_scraper()
        result = scraper._clean_pdf_markdown("page1\x0cpage2")
        assert "\x0c" not in result

    def test_removes_sei_artefact(self):
        scraper = _make_scraper()
        text = "Ato 123 (456)  SEI ABCDE12345 / pg. 7\nConteúdo real."
        result = scraper._clean_pdf_markdown(text)
        assert "SEI ABCDE12345" not in result
        assert "Conteúdo real." in result

    def test_removes_dou_date_artefact(self):
        scraper = _make_scraper()
        text = "25/03/2020, 14:30\nArt. 1°"
        result = scraper._clean_pdf_markdown(text)
        assert "25/03/2020" not in result
        assert "Art. 1°" in result

    def test_removes_disclaimer_pattern(self):
        scraper = _make_scraper()
        text = "Art. 1°\nEste texto não substitui o publicado no DOU."
        result = scraper._clean_pdf_markdown(text)
        assert "não substitui" not in result
        assert "Art. 1°" in result

    def test_removes_in_gov_br_url(self):
        scraper = _make_scraper()
        text = "Veja em https://www.in.gov.br/materia/12345 para mais detalhes."
        result = scraper._clean_pdf_markdown(text)
        assert "in.gov.br" not in result

    def test_removes_publicacao_dou_suffix_on_same_block(self):
        scraper = _make_scraper()
        text = "Art. 1° Conteúdo legal completo.\n\no publicado no Publicação DOU nº 063, de 01/04/1996, pág. 5439"
        result = scraper._clean_pdf_markdown(text)
        assert "Publicação DOU" not in result
        assert "5439" not in result
        assert result == "Art. 1° Conteúdo legal completo."

    def test_removes_publicacao_dou_suffix_with_wrapped_page_number(self):
        scraper = _make_scraper()
        text = "Art. 1° Conteúdo legal completo.\n\no publicado na Publicação DOU nº 012, de 11/12/1995, pág.\n\n20391"
        result = scraper._clean_pdf_markdown(text)
        assert "Publicação DOU" not in result
        assert "20391" not in result
        assert result == "Art. 1° Conteúdo legal completo."

    def test_removes_trailing_dou_suffix_without_publicacao_label(self):
        scraper = _make_scraper()
        text = "Art. 3° Conteúdo legal completo.\n\n_ o publicado no DOU, de 22 de novembro de 1985._"
        result = scraper._clean_pdf_markdown(text)
        assert "publicado no DOU" not in result
        assert result == "Art. 3° Conteúdo legal completo."

    def test_removes_pre_1990_imprensa_nacional_notice(self):
        scraper = _make_scraper()
        text = "Art. 8° Conteúdo legal completo.\n\n** http://portal.imprensanacional.gov.br/** **devido a data**\n**de publicação ser anterior ao ano de 1990.**"
        result = scraper._clean_pdf_markdown(text)
        assert "portal.imprensanacional.gov.br" not in result
        assert "anterior ao ano de 1990" not in result
        assert result == "Art. 8° Conteúdo legal completo."

    def test_removes_publication_block_before_following_notes(self):
        scraper = _make_scraper()
        text = "Art. 4° Conteúdo legal completo.\n\n_ o publicado no DOU, de 23 de novembro de 1984._\n\n3\n4\n\nNota editorial."
        result = scraper._clean_pdf_markdown(text)
        assert "publicado no DOU" not in result
        assert "Nota editorial." in result

    def test_removes_boletim_servico_footer_with_ministry(self):
        scraper = _make_scraper()
        text = "Art. 5° Conteúdo legal completo.\n\no publicado no Boletim de Serviço nº 948, de 25/11/85, do Ministério do Interior"
        result = scraper._clean_pdf_markdown(text)
        assert "Boletim de Serviço" not in result
        assert "Ministério do Interior" not in result
        assert result == "Art. 5° Conteúdo legal completo."

    def test_removes_boletim_servico_footer_without_comma_after_number(self):
        scraper = _make_scraper()
        text = "Art. 6° Conteúdo legal completo.\n\no publicado no Boletim de Serviço nº 921 de 20/7/84, do Ministério do Interior"
        result = scraper._clean_pdf_markdown(text)
        assert "Boletim de Serviço" not in result
        assert result == "Art. 6° Conteúdo legal completo."

    def test_removes_boletim_servico_mi_footer_with_numeric_markers(self):
        scraper = _make_scraper()
        text = "Art. 7° Conteúdo legal completo.\n\no publicado no Boletim de Serviço/MI, de 1 de novembro de 1984.\n\n1\n2\n\nNota editorial."
        result = scraper._clean_pdf_markdown(text)
        assert "Boletim de Serviço/MI" not in result
        assert "\n\n1\n2\n\n" not in result
        assert "Nota editorial." in result

    def test_removes_local_file_footer_block(self):
        scraper = _make_scraper()
        text = "Art. 8° Conteúdo legal completo.\n\nfile:///C:/Users/97785458120/Desktop/res1384.html 1/1"
        result = scraper._clean_pdf_markdown(text)
        assert "file:///" not in result
        assert result == "Art. 8° Conteúdo legal completo."

    def test_preserves_legal_content(self):
        scraper = _make_scraper()
        legal = "Art. 1° Esta resolução estabelece critérios.\n§ 1° Aplica-se a todos."
        result = scraper._clean_pdf_markdown(legal)
        assert "Art. 1°" in result
        assert "§ 1°" in result

    def test_preserves_inline_publicacao_dou_reference_in_body(self):
        scraper = _make_scraper()
        text = "Art. 2° A nota registra que o publicado na Publicação DOU nº 012, de 11/12/1995, pág. 20391 deve ser consultado em conjunto com o anexo."
        result = scraper._clean_pdf_markdown(text)
        assert result == text

    def test_preserves_header_block_with_boletim_reference(self):
        scraper = _make_scraper()
        text = "**RESOLUÇÃO CONAMA nº 11, de 26 de setembro de 1984**\nPublicada no Boletim de Serviço/MI, de 1º de novembro de 1984"
        result = scraper._clean_pdf_markdown(text)
        assert result == text
