"Parser and metadata regression tests for alagoas_scraper."

import base64
from unittest.mock import AsyncMock, MagicMock

from conftest import make_scraper
from parser_assertions import TypesConstantTests

from src.scrapers.states.al import TYPES, AlagoasSefazScraper


def _make_scraper(**kwargs) -> AlagoasSefazScraper:
    """Instantiate AlagoasSefazScraper with injected dependencies."""
    return make_scraper(
        AlagoasSefazScraper,
        view_doc_url="https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/documentos/visualizarDocumento?",
        **kwargs,
    )


def _make_api_response(
    rows: list[dict], total: int | None = None, per_page: int = 10
) -> MagicMock:
    """Mock aiohttp response returning a SEFAZ-shaped JSON payload."""
    resp = MagicMock()
    resp.__bool__ = lambda s: True
    payload = {
        "registrosTotais": total if total is not None else len(rows),
        "registrosPorPagina": per_page,
        "documentos": rows,
    }
    resp.json = AsyncMock(return_value=payload)
    return resp


def _make_doc_response(
    pdf_bytes: bytes, filename: str = "decreto_001.pdf"
) -> MagicMock:
    """Mock aiohttp response for a document download (base64 JSON payload)."""
    resp = MagicMock()
    resp.__bool__ = lambda s: True
    payload = {
        "arquivo": {
            "base64": base64.b64encode(pdf_bytes).decode(),
            "nomeArquivo": filename,
        }
    }
    resp.json = AsyncMock(return_value=payload)
    return resp


def _sample_row(**overrides) -> dict:
    base = {
        "setor": None,
        "categoria": {"descricao": "Gabinete Civil"},
        "tipoDocumento": {"descricao": "Decreto"},
        "numeroDocumento": 107,
        "dataPublicacao": "1942-12-31",
        "localPublicacaoDocumento": None,
        "dataDocumento": None,
        "descricaoCabecalho": None,
        "textoEmenta": "DISPÕE SOBRE ALGO.",
        "dataAlteracao": None,
        "listaParticaoDocumento": None,
        "listaImagemDocumento": None,
        "link": {"acess": "1", "key": "dDpCfi0+I1c=", "highlight": None},
        "generoDocumento": None,
        "imagemInicial": None,
        "_year": 1942,
    }
    base.update(overrides)
    return base


class TestTypesConstant(TypesConstantTests):
    TYPES = TYPES
    EXPECTED_COUNT = 8
    REQUIRED_KEYS = {"Constituição Estadual", "Decreto"}
    REQUIRE_INT_VALUES = False

    def test_typo_is_fixed(self):
        """'Constituição' must be spelled correctly, not 'Consituição'."""
        assert "Consituição Estadual" not in TYPES
        assert "Constituição Estadual" in TYPES

    def test_decreto_maps_to_tip002(self):
        assert TYPES["Decreto"] == "TIP002"

    def test_lei_ordinaria_maps_to_tip043(self):
        assert TYPES["Lei Ordinária"] == "TIP043"


class TestBuildParams:
    def test_no_especieLegislativa_key(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert "especieLegislativa" not in params

    def test_periodo_inicial_contains_year(self):
        scraper = _make_scraper()
        params = scraper._build_params(1942)
        assert "1942-01-01" in params["periodoInicial"]

    def test_periodo_final_contains_year(self):
        scraper = _make_scraper()
        params = scraper._build_params(1942)
        assert "1942-12-31" in params["periodoFinal"]

    def test_codigo_categoria_is_cat017(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert params["codigoCategoria"] == "CAT017"

    def test_numero_is_none(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert params["numero"] is None

    def test_codigo_setor_is_none(self):
        scraper = _make_scraper()
        params = scraper._build_params(2000)
        assert params["codigoSetor"] is None


class TestBuildUrl:
    def test_page_1_returns_bare_base_url(self):
        scraper = _make_scraper()
        assert scraper._build_url(1) == scraper.base_url

    def test_default_page_is_1(self):
        scraper = _make_scraper()
        assert scraper._build_url() == scraper.base_url

    def test_page_2_appends_pagina_2(self):
        scraper = _make_scraper()
        url = scraper._build_url(2)
        assert url == scraper.base_url + "?pagina=2"

    def test_page_20_appends_pagina_20(self):
        scraper = _make_scraper()
        url = scraper._build_url(20)
        assert url.endswith("?pagina=20")

    def test_no_pagina_param_for_page_1(self):
        scraper = _make_scraper()
        assert "pagina" not in scraper._build_url(1)
