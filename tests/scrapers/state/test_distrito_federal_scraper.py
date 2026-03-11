"""Tests for DFSinjScraper (Distrito Federal)."""

import tempfile
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.scraper.state_legislation.distrito_federal import (
    DFSinjScraper,
    INVALID_SITUATIONS,
    SITUATIONS,
    TYPES,
    VALID_SITUATIONS,
)
from base_tests import ScraperClassTests, SituationsConstantTests, TypesConstantTests
from conftest import make_base_scraper, assert_resume_skips


def _make_scraper(**kwargs) -> DFSinjScraper:
    defaults = {
        "search_url": "https://www.sinj.df.gov.br/sinj/ashx/Datatable/ResultadoDePesquisaNormaDatatable.ashx",
        "_display_length": 5000,
        "max_workers": 20,
    }
    return make_base_scraper(
        DFSinjScraper,
        "https://www.sinj.df.gov.br/sinj",
        "DISTRITO_FEDERAL",
        TYPES,
        situations=SITUATIONS,
        **{**defaults, **kwargs},
    )


def _make_source(
    *,
    ch_norma: str = "67024",
    norm_type: str = "Decreto",
    number: str = "32712",
    date: str = "30/12/2010",
    situation: str = "Sem Revogação Expressa",
    summary: str = "Dispõe sobre a criação da faculdade.",
    file_id: str | None = "e25991dc-b67b-393e-83a5-b0a39fb1f6bc",
    mimetype: str = "text/html",
    fontes: list | None = None,
) -> dict:
    return {
        "ch_norma": ch_norma,
        "nm_tipo_norma": norm_type,
        "nr_norma": number,
        "dt_assinatura": date,
        "nm_situacao": situation,
        "ds_ementa": summary,
        "ar_atualizado": {
            "id_file": file_id,
            "filename": "document.html" if mimetype == "text/html" else "document.pdf",
            "mimetype": mimetype,
            "filesize": 1234,
            "uuid": None,
        },
        "fontes": fontes or [],
    }


def _make_search_response(*sources: dict, total: int | None = None) -> dict:
    return {
        "aaData": [{"_source": source} for source in sources],
        "iTotalDisplayRecords": total if total is not None else len(sources),
    }


def _make_html_text_body(
    title_line: str,
    summary: str = "",
    *,
    download_href: str = "./Norma/67028/document.html",
) -> bytes:
    summary_tag = f"\n<p>{summary}</p>" if summary else ""
    download_tag = (
        f'\n<a title="baixar arquivo" class="baixarArquivo" target="_blank"'
        f' href="{download_href}">baixar arquivo</a>'
        if download_href
        else ""
    )
    return f"""<html><body>{download_tag}<div id="div_texto">
Sistema Integrado de Normas Jurídicas do Distrito Federal - SINJ-DF
{title_line}{summary_tag}
Art. 1º Texto principal da norma com conteúdo suficiente para validação.
Art. 2º Disposições complementares da norma do Distrito Federal.
Este texto não substitui o publicado no DODF.
</div></body></html>""".encode("utf-8")


def _make_maintenance_body() -> bytes:
    return b"""<html><body><div class="titulo-container">
    <div style="text-align: center; font-weight: bold; width: 100%; font-size: 25px; margin-bottom: 5px;">
        <small style="color: black; text-decoration: underline;">
            Instru\xc3\xa7\xc3\xa3o Normativa N\xc2\xba 20, de 17 de fevereiro de 2025
        </small>
    </div>
    <br><h1>TEXTO EM</h1>
    <div class="underline-text">MANUTEN\xc3\x87\xc3\x83O</div>
    <h2>O TEXTO DA NORMA EST\xc3\x81 SENDO REVISADO OU</h2>
    <h2>ATUALIZADO POR CONTER INFORMA\xc3\x87\xc3\x95ES</h2>
    <h2>IMPRECISAS.</h2><br>
    <div class="ComDodf">
        <h3>O TEXTO DA NORMA PODE SER ACESSADO NA</h3>
        <h3>PUBLICA\xc3\x87\xc3\x83O DO DI\xc3\x81RIO OFICIAL, CLICANDO
            <a target="_blank" href="/sinj/BaixarArquivoDiario.aspx?id_file=ef4cb2b7-8d0d-31c1-b786-00e78a974a2c">AQUI.</a>
        </h3>
    </div>
    </div></body></html>"""


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 38
    REQUIRE_INT_VALUES = False

    def test_lei_maps_to_expected_value(self):
        assert TYPES["Lei"] == 46000000


class TestSituationsConstant(SituationsConstantTests):
    SITUATIONS = SITUATIONS
    EXPECTED_TYPE = dict
    EXPECTED_EMPTY = False

    def test_situations_count_matches_valid_plus_invalid(self):
        assert len(SITUATIONS) == len(VALID_SITUATIONS) + len(INVALID_SITUATIONS)

    def test_known_situations_present(self):
        assert "Revogado" in SITUATIONS
        assert "Sem Revogação Expressa" in SITUATIONS


class TestClassAttributes(ScraperClassTests):
    SCRAPER_CLS = DFSinjScraper
    STATE_NAME = "Distrito Federal"
    EXPECT_ITERATE_SITUATIONS = False


class TestBuildPayload:
    def test_year_only_payload(self):
        scraper = _make_scraper()
        payload = scraper._build_payload(2010, offset=5000)
        payload_dict = dict(payload)

        assert payload_dict["bbusca"] == "sinj_norma"
        assert payload_dict["tipo_pesquisa"] == "avancada"
        assert payload_dict["iDisplayStart"] == 5000
        assert payload_dict["iDisplayLength"] == 5000
        assert any(k == "argumento" and "2010" in str(v) for k, v in payload)


class TestHelpers:
    def test_pick_file_info_prefers_ar_atualizado(self):
        scraper = _make_scraper()
        source = _make_source(
            file_id="primary-id",
            fontes=[
                {
                    "ar_fonte": {
                        "id_file": "secondary-id",
                        "filename": "secondary.html",
                        "mimetype": "text/html",
                    }
                }
            ],
        )

        assert scraper._pick_file_info(source)["id_file"] == "primary-id"

    def test_pick_file_info_falls_back_to_fontes(self):
        scraper = _make_scraper()
        source = _make_source(
            file_id=None,
            fontes=[
                {
                    "ar_fonte": {
                        "id_file": "fonte-id",
                        "filename": "fonte.html",
                        "mimetype": "text/html",
                    }
                }
            ],
        )

        assert scraper._pick_file_info(source)["id_file"] == "fonte-id"

    def test_clean_extracted_text_removes_header_and_disclaimer(self):
        scraper = _make_scraper()
        raw = """Legislação correlata - Decreto 28007 de 30/05/2007
DECRETO Nº 32.712, DE 30 DE DEZEMBRO DE 2010.
Art. 1º Conteúdo válido da norma.
Este texto não substitui o publicado no DODF.
## img-0000 ##"""

        cleaned = scraper._clean_extracted_text(
            raw,
            {"type": "Decreto", "number": "32712"},
        )

        assert cleaned.startswith("DECRETO Nº 32.712")
        assert "Legislação correlata" not in cleaned
        assert "Este texto não substitui" not in cleaned
        assert "## img-0000 ##" not in cleaned

    def test_remove_summary_element_handles_raw_text_nodes(self):
        from bs4 import BeautifulSoup

        scraper = _make_scraper()
        summary = "Dispõe sobre a alteração da estrutura administrativa."
        html = (
            '<div id="div_texto">'
            "DECRETO Nº 48.124, DE 31 DE DEZEMBRO DE 2025\n"
            f" {summary}\n"
            " O GOVERNADOR DO DISTRITO FEDERAL, DECRETA:\n"
            " Art. 1º Texto principal."
            "</div>"
        )
        soup = BeautifulSoup(html, "html.parser")
        root = soup.find("div", id="div_texto")
        scraper._remove_summary_element(root, summary)
        text = root.get_text(" ", strip=True)
        assert summary not in text
        assert "Art. 1º" in text
        assert "DECRETO Nº 48.124" in text

    def test_clean_extracted_text_does_not_jump_to_late_adi_reference(self):
        scraper = _make_scraper()
        raw = """EMENTA: Ação direta de inconstitucionalidade relevante.
I. Caso em exame.
1. Texto principal do julgamento.
ADI 5598 MC/DF (não aplicável ao caso concreto)."""

        cleaned = scraper._clean_extracted_text(
            raw,
            {"type": "ADI", "number": "0713444-19.2025.8.07.0000"},
        )

        assert not cleaned.startswith("ADI 5598")
        assert cleaned.startswith("Ação direta de inconstitucionalidade")
        assert "ADI 5598" in cleaned

    def test_clean_pdf_fallback_text_trims_bulletin_noise(self):
        scraper = _make_scraper()
        raw = """PORTARIA Nº 692, DE 10 DE DEZEMBRO DE 2025
Texto anterior de outro ato.

PORTARIA Nº 708, DE 17 DE DEZEMBRO DE 2025
Prorroga a vigência do Plano Estratégico Institucional.
Art. 1º Esta Portaria entra em vigor na data de sua publicação.
Documento assinado digitalmente por autoridade competente.
ATOS DA SUBSECRETARIA-GERAL DE ADMINISTRAÇÃO
ORDEM DE SERVIÇO Nº 18, DE 04 DE ABRIL DE 2025
Texto seguinte."""

        cleaned = scraper._clean_pdf_fallback_text(
            raw,
            {"type": "Portaria", "number": "708"},
        )

        assert cleaned.startswith("PORTARIA Nº 708")
        assert "PORTARIA Nº 692" not in cleaned
        assert "ATOS DA SUBSECRETARIA" not in cleaned
        assert "ORDEM DE SERVIÇO Nº 18" not in cleaned

    def test_remove_summary_element_decomposes_matching_tag(self):
        from bs4 import BeautifulSoup

        scraper = _make_scraper()
        html = """<div id="div_texto">
        <p>DECRETO Nº 100, DE 01 DE JANEIRO DE 2025</p>
        <p>Dispõe sobre a criação da faculdade.</p>
        <p>Art. 1º Texto principal.</p>
        </div>"""
        soup = BeautifulSoup(html, "html.parser")
        root = soup.find("div", id="div_texto")
        scraper._remove_summary_element(root, "Dispõe sobre a criação da faculdade.")
        text = root.get_text(" ", strip=True)
        assert "Dispõe sobre a criação da faculdade" not in text
        assert "Art. 1º" in text

    def test_remove_summary_element_skips_tag_with_art(self):
        from bs4 import BeautifulSoup

        scraper = _make_scraper()
        html = """<div id="div_texto">
        <p>Dispõe sobre a criação. Art. 1º Texto.</p>
        </div>"""
        soup = BeautifulSoup(html, "html.parser")
        root = soup.find("div", id="div_texto")
        scraper._remove_summary_element(root, "Dispõe sobre a criação.")
        text = root.get_text(" ", strip=True)
        assert "Dispõe sobre a criação" in text

    def test_strip_summary_text_removes_from_pdf(self):
        scraper = _make_scraper()
        text = (
            "DECRETO Nº 100, DE 01 DE JANEIRO DE 2025\n"
            "Dispõe sobre a criação da faculdade.\n"
            "Art. 1º Texto principal da norma."
        )
        result = scraper._strip_summary_text(
            text, "Dispõe sobre a criação da faculdade."
        )
        assert "Dispõe sobre a criação" not in result
        assert result.startswith("DECRETO Nº 100")
        assert "Art. 1º" in result

    def test_detects_maintenance_placeholder(self):
        from bs4 import BeautifulSoup

        scraper = _make_scraper()
        soup = BeautifulSoup(_make_maintenance_body(), "html.parser")
        assert scraper._is_maintenance_soup(soup) is True


class TestGetDocsLinks:
    def test_infer_norm_type_uses_title_prefix(self):
        scraper = _make_scraper()

        assert scraper._infer_norm_type("Portaria 708 de 14/01/2025") == "Portaria"

    @pytest.mark.asyncio
    async def test_returns_docs_with_any_type_and_situation(self):
        scraper = _make_scraper()
        response = MagicMock()
        response.__bool__ = lambda s: True
        response.json = AsyncMock(
            return_value=_make_search_response(
                _make_source(norm_type="Tipo Inesperado", situation="Situação Nova")
            )
        )
        scraper.request_service.make_request = AsyncMock(return_value=response)

        docs = await scraper._get_docs_links(
            scraper.search_url, scraper._build_payload(2010)
        )

        assert len(docs) == 1
        assert docs[0]["type"] == "Tipo Inesperado"
        assert docs[0]["situation"] == "Situação Nova"
        assert docs[0]["file_id"] == "e25991dc-b67b-393e-83a5-b0a39fb1f6bc"
        assert docs[0]["document_url"].endswith("/Norma/67024/arquivo")

    @pytest.mark.asyncio
    async def test_uses_fontes_file_id_when_current_file_missing(self):
        scraper = _make_scraper()
        response = MagicMock()
        response.__bool__ = lambda s: True
        response.json = AsyncMock(
            return_value=_make_search_response(
                _make_source(
                    file_id=None,
                    fontes=[
                        {
                            "ar_fonte": {
                                "id_file": "fonte-id",
                                "filename": "fonte.html",
                                "mimetype": "text/html",
                            }
                        }
                    ],
                )
            )
        )
        scraper.request_service.make_request = AsyncMock(return_value=response)

        docs = await scraper._get_docs_links(
            scraper.search_url, scraper._build_payload(2010)
        )

        assert docs[0]["file_id"] == "fonte-id"


class TestGetDocData:
    @pytest.mark.asyncio
    async def test_resume_skip_returns_none(self):
        scraper = _make_scraper()
        await assert_resume_skips(
            scraper,
            {
                "title": "Decreto 32712 de 30/12/2010",
                "document_url": f"{scraper.base_url}/Norma/67024/arquivo",
                "ch_norma": "67024",
                "file_name": "document.html",
            },
        )

    @pytest.mark.asyncio
    async def test_prefers_textoarquivo_html(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        html_response = MagicMock()
        html_response.__bool__ = lambda s: True
        html_response.read = AsyncMock(
            return_value=_make_html_text_body(
                "PORTARIA Nº 300, DE 30 DE DEZEMBRO DE 2010."
            )
        )
        scraper.request_service.make_request = AsyncMock(return_value=html_response)
        scraper._capture_mhtml = AsyncMock(return_value=b"fake-mhtml")

        result = await scraper._get_doc_data(
            {
                "title": "Portaria 300 de 30/12/2010",
                "type": "Portaria",
                "number": "300",
                "situation": "Sem Revogação Expressa",
                "year": 2010,
                "document_url": f"{scraper.base_url}/Norma/67028/arquivo",
                "file_id": "text-id",
                "file_name": "document.html",
                "ch_norma": "67028",
            }
        )

        assert result is not None
        assert result["document_url"].endswith("/Norma/67028/document.html")
        assert result["_content_extension"] == ".mhtml"
        assert result["text_markdown"].startswith("PORTARIA Nº 300")
        assert "Sistema Integrado" not in result["text_markdown"]
        assert "Este texto não substitui" not in result["text_markdown"]
        assert scraper.request_service.make_request.await_count == 1

    @pytest.mark.asyncio
    async def test_html_endpoint_strips_summary_from_markdown(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        summary = "Dispõe sobre a alteração da estrutura administrativa."
        html_response = MagicMock()
        html_response.__bool__ = lambda s: True
        html_response.read = AsyncMock(
            return_value=_make_html_text_body(
                "PORTARIA Nº 300, DE 30 DE DEZEMBRO DE 2010.",
                summary=summary,
            )
        )
        scraper.request_service.make_request = AsyncMock(return_value=html_response)
        scraper._capture_mhtml = AsyncMock(return_value=b"fake-mhtml")

        result = await scraper._get_doc_data(
            {
                "title": "Portaria 300 de 30/12/2010",
                "type": "Portaria",
                "number": "300",
                "situation": "Sem Revogação Expressa",
                "year": 2010,
                "summary": summary,
                "document_url": f"{scraper.base_url}/Norma/67028/arquivo",
                "file_id": "text-id",
                "file_name": "document.html",
                "ch_norma": "67028",
            }
        )

        assert result is not None
        assert summary not in result["text_markdown"]
        assert result["text_markdown"].startswith("PORTARIA Nº 300")

    @pytest.mark.asyncio
    async def test_prefers_text_url_when_file_is_pdf(self):
        """When file_name is a PDF, document_url should be text_url, not the PDF download."""
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        html_response = MagicMock()
        html_response.__bool__ = lambda s: True
        html_response.read = AsyncMock(
            return_value=_make_html_text_body(
                "PORTARIA Nº 300, DE 30 DE DEZEMBRO DE 2010.",
                download_href="./Norma/67028/document.pdf",
            )
        )
        scraper.request_service.make_request = AsyncMock(return_value=html_response)
        scraper._capture_mhtml = AsyncMock(return_value=b"fake-mhtml")

        result = await scraper._get_doc_data(
            {
                "title": "Portaria 300 de 30/12/2010",
                "type": "Portaria",
                "number": "300",
                "situation": "Sem Revogação Expressa",
                "year": 2010,
                "document_url": f"{scraper.base_url}/Norma/67028/arquivo",
                "file_id": "text-id",
                "file_name": "document.pdf",
                "ch_norma": "67028",
            }
        )

        assert result is not None
        # Should use text_url (TextoArquivoNorma.aspx), NOT the PDF download link
        assert "TextoArquivoNorma.aspx" in result["document_url"]
        assert not result["document_url"].endswith(".pdf")
        assert result["_content_extension"] == ".mhtml"
        assert scraper.request_service.make_request.await_count == 1

    @pytest.mark.asyncio
    async def test_falls_back_to_raw_pdf_when_text_endpoint_is_invalid(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        invalid_html_response = MagicMock()
        invalid_html_response.__bool__ = lambda s: True
        invalid_html_response.read = AsyncMock(
            return_value=b"<html><body>sem div_texto</body></html>"
        )

        pdf_response = MagicMock()
        pdf_response.__bool__ = lambda s: True
        pdf_response.read = AsyncMock(return_value=b"%PDF-1.4 fake pdf")
        pdf_response.content_type = "application/pdf"
        pdf_response.headers = {
            "Content-Disposition": 'inline; filename="document.pdf"'
        }

        scraper.request_service.make_request = AsyncMock(
            side_effect=[invalid_html_response, pdf_response]
        )
        scraper.request_service.detect_content_info = MagicMock(
            return_value=("document.pdf", "application/pdf")
        )
        scraper._fetch_diary_pdf_fallback = AsyncMock(return_value=None)
        scraper._get_markdown = AsyncMock(
            return_value="# Norma\n\nArt. 1 Texto suficiente. " * 8
        )

        result = await scraper._get_doc_data(
            {
                "title": "Portaria 300 de 30/12/2010",
                "type": "Portaria",
                "number": "300",
                "situation": "Sem Revogação Expressa",
                "year": 2010,
                "document_url": f"{scraper.base_url}/Norma/67028/arquivo",
                "file_id": "text-id",
                "file_name": "document.pdf",
                "ch_norma": "67028",
            }
        )

        assert result is not None
        assert result["_content_extension"] == ".pdf"
        assert result["document_url"].endswith("/Norma/67028/arquivo")

    @pytest.mark.asyncio
    async def test_skips_norm_when_text_is_in_maintenance(self):
        scraper = _make_scraper()
        scraper._is_already_scraped = MagicMock(return_value=False)
        response = MagicMock()
        response.__bool__ = lambda s: True
        response.read = AsyncMock(return_value=_make_maintenance_body())
        scraper.request_service.make_request = AsyncMock(return_value=response)
        scraper._save_doc_error = AsyncMock()

        result = await scraper._get_doc_data(
            {
                "title": "Instrução Normativa 20 de 17/02/2025",
                "type": "Instrução Normativa",
                "number": "20",
                "situation": "Sem Revogação Expressa",
                "year": 2025,
                "document_url": f"{scraper.base_url}/Norma/abc123/arquivo",
                "file_id": "text-id",
                "file_name": "document.html",
                "ch_norma": "abc123",
            }
        )

        assert result is None
        scraper._save_doc_error.assert_awaited_once()
        assert scraper.request_service.make_request.await_count == 1


class TestScrapeYear:
    @pytest.mark.asyncio
    async def test_scrape_year_fetches_extra_pages_and_processes_in_batches(self):
        scraper = _make_scraper(_display_length=2)
        page1 = [
            {"title": "Doc 1", "type": "Lei", "situation": "A", "document_url": "u1"},
            {"title": "Doc 2", "type": "Lei", "situation": "A", "document_url": "u2"},
        ]
        page2 = [
            {
                "title": "Doc 3",
                "type": "Portaria",
                "situation": "B",
                "document_url": "u3",
            }
        ]

        # First call returns page1 with total=3; second call (offset=2) returns page2.
        scraper._fetch_search_page = AsyncMock(side_effect=[(page1, 3), (page2, 3)])

        async def fake_process(documents, **kwargs):
            for doc in documents:
                assert doc["year"] == 2025
            return documents

        scraper._process_documents = fake_process

        results = await scraper._scrape_year(2025)

        assert len(results) == 3
        assert scraper._fetch_search_page.await_count == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_docs_links_year_2010_returns_results():
    with tempfile.TemporaryDirectory() as tmp:
        scraper = DFSinjScraper(docs_save_dir=tmp, verbose=False)
        docs = await scraper._get_docs_links(
            scraper.search_url,
            scraper._build_payload(2010, limit=5),
        )

        assert isinstance(docs, list)
        assert len(docs) > 0
        assert "document_url" in docs[0]
        assert "type" in docs[0]
        assert "situation" in docs[0]

        await scraper.cleanup()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_doc_data_prefers_html_text_endpoint():
    with tempfile.TemporaryDirectory() as tmp:
        scraper = DFSinjScraper(docs_save_dir=tmp, verbose=False)
        docs = await scraper._get_docs_links(
            scraper.search_url,
            scraper._build_payload(2010, limit=20),
        )
        doc = next(d for d in docs if d.get("file_id"))
        doc["year"] = 2010

        result = await scraper._get_doc_data(doc)

        assert result is not None
        assert result["_content_extension"] == ".html"
        assert len(result["text_markdown"]) > 50
        assert "Este texto não substitui" not in result["text_markdown"]
        assert "Sistema Integrado de Normas Jurídicas" not in result["text_markdown"]

        await scraper.cleanup()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_doc_data_skips_maintenance_norm():
    with tempfile.TemporaryDirectory() as tmp:
        scraper = DFSinjScraper(docs_save_dir=tmp, verbose=False)

        result = await scraper._get_doc_data(
            {
                "title": "Instrução Normativa 20 de 17/02/2025",
                "type": "Instrução Normativa",
                "number": "20",
                "situation": "Sem Revogação Expressa",
                "year": 2025,
                "document_url": (
                    "https://www.sinj.df.gov.br/sinj/Norma/"
                    "7174ec3536864fc78c138192eb924868/arquivo"
                ),
                "file_id": "",
                "ch_norma": "7174ec3536864fc78c138192eb924868",
            }
        )

        assert result is None
        assert scraper.error_count >= 1

        await scraper.cleanup()
