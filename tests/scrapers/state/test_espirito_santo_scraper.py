"Parser and metadata regression tests for espirito_santo_scraper."

import re

from bs4 import BeautifulSoup
from conftest import make_scraper
from parser_assertions import (
    ScraperClassTests,
    SituationsConstantTests,
    TypesConstantTests,
)

from src.scrapers.states.es import (
    _AUTH_FOOTER_RE,
    _DISCLAIMER_RE,
    INVALID_SITUATIONS,
    SITUATIONS,
    TYPES,
    VALID_SITUATIONS,
    ESAlesScraper,
    _clean_markdown,
)

_DIGITAL_SIGNATURE_RE = _AUTH_FOOTER_RE
_AUTHENTICATION_WATERMARK_RE = re.compile(
    "P[ÁáAa]GINA\\s+\\d+\\s*/\\s*\\d+\\s+PARA\\s+VERIFICAR\\s+A\\s+AUTENTICIDADE\\s+DESTE\\s+DOCUMENTO.*?(?:https?://\\S+)",
    re.IGNORECASE | re.DOTALL,
)


def _make_scraper(**kwargs) -> ESAlesScraper:
    """Instantiate ESAlesScraper with injected dependencies."""
    return make_scraper(ESAlesScraper, **kwargs)


def _make_listing_html(items: int = 2, has_next: bool = True) -> bytes:
    """Build a minimal year-only listing page with `items` document rows."""
    item_html = ""
    for i in range(1, items + 1):
        item_html += f'\n        <div class="kt-widget5__item">\n          <a class="kt-widget5__title">Lei Ordinária\n{i:03d}/2010</a>\n          <a class="kt-widget5__desc">Ementa {i}</a>\n          <div class="kt-widget5__info">\n            <span class="kt-font-info">01/01/2010</span>\n            <span class="kt-font-info">Em Vigor</span>\n          </div>\n          <div class="kt-widget5__info">\n            <span class="kt-font-info">Governador</span>\n          </div>\n          <a class="btn btn-sm btn-label-info btn-pill d-block" href="/pdf/lei{i}.pdf">Texto</a>\n        </div>'
    vs = '<input type="hidden" id="__VIEWSTATE" name="__VIEWSTATE" value="vs_val" />'
    ev = '<input type="hidden" id="__EVENTVALIDATION" name="__EVENTVALIDATION" value="ev_val" />'
    if has_next:
        next_btn = '<a id="ContentPlaceHolder1_lbNext" href="#">Próxima</a>'
    else:
        next_btn = '<a id="ContentPlaceHolder1_lbNext" class="aspNetDisabled" href="#">Próxima</a>'
    return f'<html><body>\n        {vs}\n        {ev}\n        <div class="kt-portlet__body">\n            {item_html}\n        </div>\n        {next_btn}\n    </body></html>'.encode()


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 13
    REQUIRED_KEYS = {
        "Lei Ordinária",
        "Lei Complementar",
        "Decreto Executivo",
        "Constituição Estadual",
    }
    REQUIRE_INT_VALUES = True

    def test_lei_ordinaria_id(self):
        assert TYPES["Lei Ordinária"] == 3

    def test_lei_complementar_id(self):
        assert TYPES["Lei Complementar"] == 4


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_valid_situations_has_em_vigor(self):
        assert "Em Vigor" in VALID_SITUATIONS

    def test_invalid_situations_has_revogada(self):
        assert "Revogada" in INVALID_SITUATIONS

    def test_situations_merges_valid_and_invalid(self):
        assert "Em Vigor" in SITUATIONS
        assert "Revogada" in SITUATIONS

    def test_situations_count(self):
        assert len(SITUATIONS) == len(VALID_SITUATIONS) + len(INVALID_SITUATIONS)


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = ESAlesScraper
    STATE_NAME = "Espirito Santo"

    def test_situations_empty_in_instance(self):
        scraper = _make_scraper()
        assert scraper.situations == {}


class TestFormatSearchUrl:
    def test_url_contains_year(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2010)
        assert "ano=2010" in url

    def test_url_starts_with_base_url(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2010)
        assert url.startswith(scraper.base_url)

    def test_url_contains_interno(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2010)
        assert "interno=1" in url

    def test_url_has_no_tipo_or_situacao(self):
        scraper = _make_scraper()
        url = scraper._format_search_url(2010)
        assert "tipo=" not in url
        assert "situacao=" not in url


class TestParseDocsFromSoup:
    def test_happy_path_returns_correct_fields(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_listing_html(2), "lxml")
        docs = scraper._parse_docs_from_soup(soup)
        assert len(docs) == 2
        assert "norm_type" in docs[0]
        assert "situation" in docs[0]
        assert "date" in docs[0]
        assert "authors" in docs[0]
        assert "title" in docs[0]
        assert "doc_link" in docs[0]

    def test_norm_type_extracted_from_title_first_line(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_listing_html(1), "lxml")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs[0]["norm_type"] == "Lei Ordinária"

    def test_situation_extracted_from_second_info_span(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_listing_html(1), "lxml")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs[0]["situation"] == "Em Vigor"

    def test_date_extracted_from_first_info_span(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(_make_listing_html(1), "lxml")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs[0]["date"] == "01/01/2010"

    def test_empty_container_returns_empty_list(self):
        scraper = _make_scraper()
        html = b"<html><body><div class='kt-portlet__body'></div></body></html>"
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs == []

    def test_missing_container_returns_empty_list(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(b"<html><body></body></html>", "lxml")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs == []

    def test_process_link_is_skipped(self):
        scraper = _make_scraper()
        html = b'<html><body>\n            <div class="kt-portlet__body">\n              <div class="kt-widget5__item">\n                <a class="kt-widget5__title">Lei Ordinaria\n001/2010</a>\n                <a class="kt-widget5__desc">Ementa</a>\n                <div class="kt-widget5__info">\n                  <span class="kt-font-info">01/01/2010</span>\n                  <span class="kt-font-info">Em Vigor</span>\n                </div>\n                <div class="kt-widget5__info"><span class="kt-font-info">Gov</span></div>\n                <a class="btn btn-sm btn-label-info btn-pill d-block" href="/processo.aspx?id=1">Process</a>\n              </div>\n            </div>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs == []

    def test_docx_link_is_skipped(self):
        scraper = _make_scraper()
        html = b'<html><body>\n            <div class="kt-portlet__body">\n              <div class="kt-widget5__item">\n                <a class="kt-widget5__title">Lei Ordinaria\n001/2010</a>\n                <a class="kt-widget5__desc">Ementa</a>\n                <div class="kt-widget5__info">\n                  <span class="kt-font-info">01/01/2010</span>\n                  <span class="kt-font-info">Em Vigor</span>\n                </div>\n                <div class="kt-widget5__info"><span class="kt-font-info">Gov</span></div>\n                <a class="btn btn-sm btn-label-info btn-pill d-block" href="/doc.docx">Doc</a>\n              </div>\n            </div>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs == []

    def test_no_btn_link_is_skipped(self):
        scraper = _make_scraper()
        html = b'<html><body>\n            <div class="kt-portlet__body">\n              <div class="kt-widget5__item">\n                <a class="kt-widget5__title">Lei Ordinaria\n001/2010</a>\n                <a class="kt-widget5__desc">Ementa</a>\n                <div class="kt-widget5__info">\n                  <span class="kt-font-info">01/01/2010</span>\n                  <span class="kt-font-info">Em Vigor</span>\n                </div>\n                <div class="kt-widget5__info"><span class="kt-font-info">Gov</span></div>\n              </div>\n            </div>\n        </body></html>'
        soup = BeautifulSoup(html, "lxml")
        docs = scraper._parse_docs_from_soup(soup)
        assert docs == []


class TestExtractTotalCount:
    def test_extracts_count_from_contagem_span(self):
        scraper = _make_scraper()
        html = '<html><body><span id="ContentPlaceHolder1_contagem">Localizada(s) 3730 resultados</span></body></html>'
        soup = BeautifulSoup(html, "lxml")
        assert scraper._extract_total_count(soup) == 3730

    def test_returns_none_when_tag_absent(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(b"<html><body></body></html>", "lxml")
        assert scraper._extract_total_count(soup) is None

    def test_handles_dot_thousands_separator(self):
        scraper = _make_scraper()
        html = '<html><body><span id="ContentPlaceHolder1_contagem">Localizada(s) 1.234 resultados</span></body></html>'
        soup = BeautifulSoup(html, "lxml")
        assert scraper._extract_total_count(soup) == 1234


class TestHasNextPage:
    def test_returns_true_when_lbnext_active(self):
        scraper = _make_scraper()
        html = b'<html><body><a id="ContentPlaceHolder1_lbNext" href="#">Next</a></body></html>'
        soup = BeautifulSoup(html, "lxml")
        assert scraper._has_next_page(soup) is True

    def test_returns_false_when_lbnext_disabled(self):
        scraper = _make_scraper()
        html = b'<html><body><a id="ContentPlaceHolder1_lbNext" class="aspNetDisabled" href="#">Next</a></body></html>'
        soup = BeautifulSoup(html, "lxml")
        assert scraper._has_next_page(soup) is False

    def test_returns_false_when_lbnext_absent(self):
        scraper = _make_scraper()
        soup = BeautifulSoup(b"<html><body></body></html>", "lxml")
        assert scraper._has_next_page(soup) is False


class TestCleanMarkdown:
    _AUTH_FOOTER = "Autenticar documento em https://www3.al.es.gov.br/autenticidade\ncom o identificador 3400340032003000300034003A00540052004100, Documento assinado\ndigitalmente conforme MP n° 2.200-2/2001, que institui a Infra-estrutura de Chaves Públicas Brasileira\n- ICP-Brasil."

    def test_clean_text_unchanged(self):
        text = "# Lei\n\nTexto da lei normal.\n\nPalavras legais."
        assert _clean_markdown(text) == text.strip()

    def test_summary_stripped_when_at_start(self):
        summary = "NOMEAR JOAO SILVA PARA O CARGO"
        text = summary + "\n\nPalácioDomingos."
        result = _clean_markdown(text, summary)
        assert not result.upper().startswith(summary.upper())
        assert "Paláci" in result

    def test_summary_not_stripped_when_far_from_start(self):
        summary = "NOMEAR JOAO SILVA"
        text = "A" * 400 + summary + "\n\nTexto final."
        result = _clean_markdown(text, summary)
        assert summary in result

    def test_long_summary_excerpt_inside_body_is_not_stripped(self):
        summary = "NOMEAR, NA FORMA DO ARTIGO 12, INCISO II, DA LEI COMPLEMENTAR Nº 46, DE 31 DE JANEIRO DE 1994, CAROLINE QUEIROZ DOS SANTOS, PARA EXERCER O CARGO EM COMISSÃO DE TÉCNICO SÊNIOR DE GABINETE DE REPRESENTAÇÃO PARLAMENTAR - II, NO GABINETE DO(A) DEPUTADO(A) HUDSON LEAL, POR SOLICITAÇÃO DO(A) PRÓPRIO DEPUTADO(A)."
        result = _clean_markdown(self._PAGE1_CONTENT, summary)
        assert result == self._PAGE1_CONTENT

    def test_empty_summary_does_nothing(self):
        text = "# Lei\n\nTexto."
        assert _clean_markdown(text, "") == text.strip()

    _PAGE1_CONTENT = "ATO Nº\n\nA MESA DIRETORA DA ASSEMBLEIA LEGISLATIVA DO ESTADO DO ESPÍRITO SANTO,\nlegais, e considerando o previsto no Processo nº\nusando de suas atribuições\n000028031/2025, resolve:\n\nNOMEAR, na forma do artigo 12, inciso II, da Lei Complementar nº 46, de 31 de janeiro\nde 1994, CAROLINE QUEIROZ DOS SANTOS, para exercer o cargo em comissão de TÉCNICO\nSÊNIOR DE GABINETE DE REPRESENTAÇÃO PARLAMENTAR -\nII, no gabinete do(a)\nDeputado(a) HUDSON LEAL, por solicitação do(a) próprio Deputado(a).\n\nPalácio Domingos Martins,\n\nMARCELO SANTOS\nPresidente"
    _MANIFESTO_BLOCK = "MANIFESTO DE\nASSINATURAS\n\n\n\nAssinado digitalmente por:\nALEXANDRE MARCELO COUTINHO SANTOS\nCPF: ***.507.277-**\nCertificado emitido por AC SyngularID Multipla\nData: 09/12/2025 19:07:20 -03:00"

    def test_pymupdf4llm_manifesto_stripped(self):
        """pymupdf4llm-style output: content + manifesto in a single block."""
        raw = self._PAGE1_CONTENT + "\n\n\n" + self._MANIFESTO_BLOCK
        result = _clean_markdown(raw)
        assert result == self._PAGE1_CONTENT

    def test_manifesto_content_not_in_output(self):
        """MANIFESTO DE ASSINATURAS and digital signature must not appear."""
        raw = self._PAGE1_CONTENT + "\n\n\n" + self._MANIFESTO_BLOCK
        result = _clean_markdown(raw)
        assert "MANIFESTO" not in result
        assert "ASSINATURAS" not in result
        assert "Assinado digitalmente" not in result

    def test_multi_section_content_preserved(self):
        """Legal text that spans multiple sections must be fully preserved."""
        page2_text = "Art. 3º As disposições anteriores aplicam-se subsidiariamente.\n\nArt. 4º Esta Lei entra em vigor na data de sua publicação."
        raw = (
            self._PAGE1_CONTENT + "\n\n" + page2_text + "\n\n\n" + self._MANIFESTO_BLOCK
        )
        result = _clean_markdown(raw)
        assert self._PAGE1_CONTENT in result
        assert page2_text in result
        assert "MANIFESTO" not in result

    def test_auth_footer_removed_from_single_page(self):
        text = (
            "RESOLUÇÃO Nº 1\n\nArt. 1º Fica concedida a medalha.\n\nMARCELO SANTOS\nPresidente\n\n"
            + self._AUTH_FOOTER
        )
        result = _clean_markdown(text)
        assert (
            result
            == "RESOLUÇÃO Nº 1\n\nArt. 1º Fica concedida a medalha.\n\nMARCELO SANTOS\nPresidente"
        )

    def test_auth_footer_and_page_number_removed_from_single_page(self):
        text = "Art. 1º Texto principal.\n\n" + self._AUTH_FOOTER + "\n\nPágina 1"
        result = _clean_markdown(text)
        assert result == "Art. 1º Texto principal."
        assert "Página 1" not in result

    def test_auth_footer_split_across_lines_removed(self):
        raw = "Art. 1º Texto da primeira página.\n\nAutenticar documento em https://www3.al.es.gov.br/autenticidade\ncom o identificador 3400340032003000300034003A00540052004100, Documento assinado\ndigitalmente conforme MP n° 2.200-2/2001, que institui a Infra-estrutura de Chaves Públicas Brasileira\n- ICP-Brasil.\n\nArt. 2º Texto da segunda página."
        result = _clean_markdown(raw)
        assert (
            result
            == "Art. 1º Texto da primeira página.\n\nArt. 2º Texto da segunda página."
        )

    def test_multi_section_content_preserved_when_auth_footer_repeats(self):
        page2 = "Art. 2º Esta Resolução entra em vigor na data de sua publicação."
        raw = (
            self._PAGE1_CONTENT
            + "\n\n"
            + self._AUTH_FOOTER
            + "\n\n"
            + page2
            + "\n\n"
            + self._AUTH_FOOTER
            + "\n\nPágina 2"
        )
        result = _clean_markdown(raw)
        assert self._PAGE1_CONTENT in result
        assert page2 in result
        assert "Autenticar documento em" not in result
        assert "ICP-Brasil" not in result
        assert "Página 2" not in result

    def test_html_disclaimer_removed(self):
        text = "Lei nº 1\n\nArt. 1º Texto da lei.\n\nEste texto não substitui o publicado no D.P.L. de 11/09/2025."
        result = _clean_markdown(text)
        assert result == "Lei nº 1\n\nArt. 1º Texto da lei."

    def test_html_disclaimer_removed_with_line_breaks_and_do_variant(self):
        text = "Lei nº 2\n\nArt. 1º Texto da lei.\n\nEste texto não substitui\no publicado no D.O. de 30/12/2025."
        result = _clean_markdown(text)
        assert result == "Lei nº 2\n\nArt. 1º Texto da lei."

    def test_inline_pagina_reference_is_preserved(self):
        text = "Art. 1º Consulte a Página 12 do Anexo I para detalhes."
        assert _clean_markdown(text) == text

    def test_inline_disclaimer_like_phrase_is_preserved(self):
        text = "O parecer registra a frase Este texto não substitui o publicado no D.O. de 30/12/2025. apenas como exemplo."
        assert _clean_markdown(text) == text

    def test_autenticidade_reference_outside_footer_is_preserved(self):
        text = "Art. 1º O sistema de autenticidade deverá permanecer disponível durante o horário de expediente."
        assert _clean_markdown(text) == text

    def test_watermark_regex_preserves_short_lines_with_spaces(self):
        """Lines with spaces/punctuation are NOT matched by \\w{0,2}, so they survive."""
        text = "# Lei\n\n" + "\n ".join(
            [
                "de",
                "em",
                "a",
                "o",
                "e",
                "da",
                "do",
                "na",
                "no",
                "as",
                "os",
                "um",
                "ao",
                "se",
                "ou",
                "por",
            ]
        )
        result = _clean_markdown(text)
        assert "de" in result
        assert "em" in result

    def test_digital_signature_footer_removed(self):
        """Full digital-signature authentication footer must be stripped."""
        footer = "\n\nAutenticar documento em https://www3.al.es.gov.br/autenticidade\ncom o identificador 3B0C9DBA75CAA64874C318, Documento assinado\ndigitalmente conforme MP n° 2.200-2/2001, que institui a Infra-estrutura de Chaves Públicas Brasileira\n- ICP-Brasil."
        text = "# Lei\n\nTexto da lei." + footer
        result = _clean_markdown(text)
        assert "Autenticar documento" not in result
        assert "Texto da lei." in result

    def test_duplicated_digital_signature_removed(self):
        """Duplicated-line variant of the footer must also be stripped."""
        footer = "\n\nAutenticar documento em https://www3.al.es.gov.br/autenticidade\ncom o identificador ABC123, Documento assinado\ndigitalmente conforme MP n° 2.200-2/2001, que institui a Infra-estrutura de Chaves Públicas Brasileira\n- ICP-Brasil.\n\nAutenticar documento em https://www3.al.es.gov.br/autenticidade\ncom o identificador ABC123, Documento assinado\ndigitalmente conforme MP n° 2.200-2/2001, que institui a Infra-estrutura de Chaves Públicas Brasileira\n- ICP-Brasil."
        text = "Conteúdo da norma." + footer
        result = _clean_markdown(text)
        assert "Autenticar documento" not in result
        assert "Conteúdo da norma." in result

    def test_content_before_footer_preserved(self):
        """Legitimate text before the footer must be kept intact."""
        body = "Art. 1º - Fica criado o Fundo Estadual.\n\nArt. 2º - Esta lei entra em vigor."
        footer = "\n\nAutenticar documento em https://www3.al.es.gov.br/autenticidade\ncom o identificador 1A2B3C, Documento assinado\ndigitalmente conforme MP n° 2.200-2/2001, que institui a Infra-estrutura de Chaves Públicas Brasileira\n- ICP-Brasil."
        text = body + footer
        result = _clean_markdown(text)
        assert result == body

    def test_multipage_content_after_footer_preserved(self):
        """Footer on page 1 must not eat pages 2–3 in multi-page PDFs."""
        page1 = "# Page 1\n\nArt. 1º - Texto da primeira página."
        footer = "\n\nAutenticar documento em https://www3.al.es.gov.br/autenticidade\ncom o identificador 3B0C9DBA, Documento assinado\ndigitalmente conforme MP n° 2.200-2/2001, que institui a Infra-estrutura de Chaves Públicas Brasileira\n- ICP-Brasil."
        page2 = "\n\n# Page 2\n\nArt. 2º - Texto da segunda página."
        page3 = "\n\n# Page 3\n\nArt. 3º - Texto da terceira página."
        text = page1 + footer + page2 + page3
        result = _clean_markdown(text)
        assert "Autenticar documento" not in result
        assert "Page 1" in result
        assert "Page 2" in result
        assert "Page 3" in result
        assert "segunda página" in result
        assert "terceira página" in result

    def test_digital_signature_regex_matches_real_pattern(self):
        footer = "Autenticar documento em https://www3.al.es.gov.br/autenticidade\ncom o identificador 3B0C, Documento assinado\ndigitalmente conforme MP n° 2.200-2/2001, que institui a Infra-estrutura de Chaves Públicas Brasileira\n- ICP-Brasil."
        assert _DIGITAL_SIGNATURE_RE.search(footer) is not None

    def test_disclaimer_do_removed(self):
        """D.O. variant of the disclaimer is stripped, body preserved."""
        text = "# Lei\n\nTexto da lei.\n\nEste texto não substitui o publicado no D.O. de 26/11/2025."
        result = _clean_markdown(text)
        assert "Este texto" not in result
        assert "Texto da lei." in result

    def test_disclaimer_dpl_removed(self):
        """D.P.L. variant of the disclaimer is stripped."""
        text = "Art. 1º - Conteúdo.\n\nEste texto não substitui o publicado no D.P.L. de 05/03/2024."
        result = _clean_markdown(text)
        assert "Este texto" not in result
        assert "Conteúdo." in result

    def test_disclaimer_with_linebreaks_removed(self):
        """Line breaks in the middle of the phrase are still matched."""
        text = "Corpo.\n\nEste texto\nnão substitui o\npublicado no D.O. de 26/11/2025."
        result = _clean_markdown(text)
        assert "Este texto" not in result
        assert "Corpo." in result

    def test_disclaimer_preserves_content_after(self):
        """ANEXO content after a \\n\\n boundary is kept."""
        text = "Art. 1º - Texto.\n\nEste texto não substitui o publicado no D.P.L. de 01/06/2023.\n\nANEXO\n\nTabela de valores."
        result = _clean_markdown(text)
        assert "Este texto" not in result
        assert "ANEXO" in result
        assert "Tabela de valores." in result

    def test_disclaimer_regex_matches_single_digit_day(self):
        """Single-digit day like 9/12/2025 is matched."""
        text = "Este texto não substitui o publicado no D.O. de 9/12/2025."
        assert _DISCLAIMER_RE.search(text) is not None

    def test_authentication_watermark_removed(self):
        """The PÁGINA X / Y PARA VERIFICAR watermark must be stripped."""
        text = "# Lei Ordinária\n\nArt. 1º - Texto.\n\nPÁGINA 1 / 3 PARA VERIFICAR A AUTENTICIDADE DESTE DOCUMENTO, ACESSE O ENDEREÇO: https://www3.al.es.gov.br/autenticidade\n\nArt. 2º - Mais texto."
        result = _clean_markdown(text)
        assert "VERIFICAR A AUTENTICIDADE" not in result
        assert "Art. 1º" in result
        assert "Art. 2º" in result

    def test_authentication_watermark_regex_matches(self):
        assert _AUTHENTICATION_WATERMARK_RE.search(
            "PÁGINA 2 / 5 PARA VERIFICAR A AUTENTICIDADE DESTE DOCUMENTO, ACESSE O ENDEREÇO: https://example.gov.br/token123"
        )

    def test_authentication_watermark_regex_case_insensitive(self):
        assert _AUTHENTICATION_WATERMARK_RE.search(
            "pagina 1 / 2 para verificar a autenticidade deste documento, acesse o endereço: https://www3.al.es.gov.br/x"
        )
