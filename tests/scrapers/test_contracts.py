"""Exercise the new injected listing/fetch contract with buffered responses."""

from unittest.mock import AsyncMock

import pytest
from bs4 import BeautifulSoup
from conftest import make_mock_json_response, make_mock_response, make_scraper
from test_store import document

from src.core.document import Document
from src.core.http import HttpError
from src.core.scraper import DEFAULT_VALID_SITUATION
from src.registry import SCRAPERS
from src.scrapers.environment.conama import ConamaScraper
from src.scrapers.environment.icmbio import ICMBioScraper
from src.scrapers.states.ac import AcreLegisScraper
from src.scrapers.states.am import LegislaAMScraper
from src.scrapers.states.ap import AmapaAlapScraper
from src.scrapers.states.ba import BahiaLegislaScraper
from src.scrapers.states.ce import CearaAleceScraper
from src.scrapers.states.df import DFSinjScraper
from src.scrapers.states.es import ESAlesScraper
from src.scrapers.states.ms import MSAlemsScraper
from src.scrapers.states.pr import ParanaCVScraper
from src.scrapers.states.rs import RSAlrsScraper
from src.scrapers.states.sp import SaoPauloAlespScraper

TEXT = (
    "Art. 1º Esta lei estabelece regras de organização da administração pública. " * 5
)


@pytest.mark.parametrize("spec", SCRAPERS, ids=lambda s: s.cls.name)
async def test_all_scrapers_record_listing_transport_failures(spec):
    scraper = make_scraper(spec.cls, years=range(2024, 2025), workers=1)
    scraper.http.request = AsyncMock(
        side_effect=HttpError("listing", 503, "unavailable")
    )
    scraper.fetch = AsyncMock(return_value=document())
    async with scraper.store:
        result = await scraper.run()
        assert result.errors > 0
        assert (
            scraper.store.connection.execute("SELECT count(*) FROM errors").fetchone()[
                0
            ]
            > 0
        )


@pytest.mark.parametrize(
    ("cls", "html", "extra"),
    [
        (
            AcreLegisScraper,
            f'<div id="body-law"><p>{TEXT}</p></div>',
            {"html_link": "https://example.org/law"},
        ),
        (
            LegislaAMScraper,
            f'<div class="materia rounded"><p>{TEXT}</p></div>',
            {"html_link": "/law"},
        ),
        (
            AmapaAlapScraper,
            f"<table><tr><td><p>{TEXT}</p></td></tr></table>",
            {"html_link": "/law"},
        ),
        (
            BahiaLegislaScraper,
            f'<div class="field--name-body"><p>{TEXT}</p></div>',
            {"html_link": "/law"},
        ),
        (CearaAleceScraper, f"<main><p>{TEXT}</p></main>", {"type": "Resolução"}),
        (ESAlesScraper, f"<p>{TEXT}</p>", {"doc_link": "/law", "norm_type": "Lei"}),
        (MSAlemsScraper, f"<p>{TEXT}</p>", {"html_link": "https://example.org/law"}),
        (
            ParanaCVScraper,
            f'<form name="pesquisarAtoForm"><p>{TEXT}</p></form>',
            {"id": "1"},
        ),
        (
            SaoPauloAlespScraper,
            f"<p>{TEXT}</p>",
            {
                "html_link": "https://example.org/law",
                "norm_link": "https://example.org/details",
            },
        ),
        (ICMBioScraper, f'<div class="texto-dou"><p>{TEXT}</p></div>', {}),
    ],
)
async def test_html_fetch_returns_validated_document(cls, html, extra):
    scraper = make_scraper(cls)
    body = f"<html><body>{html}</body></html>"
    scraper.http.request = AsyncMock(return_value=make_mock_response(body))
    scraper.browser.snapshot = AsyncMock(
        return_value=(BeautifulSoup(body, "lxml"), b"mhtml")
    )
    ref = {**document().model_dump(), **extra}
    ref.pop("text_markdown")
    result = await scraper.fetch(ref)
    assert isinstance(result, Document)
    assert "Art." in result.text_markdown
    assert result.year == 2024 and result.type and result.situation
    assert result.raw and result.raw_ext in {".html", ".mhtml"}


async def test_rs_single_page_without_pagination_yields_records():
    scraper = make_scraper(RSAlrsScraper)
    html = """<table class="TableResultado"><tr><td>Lei</td><td><a onclick='open("123")'>123</a></td><td>01/01/2024</td><td>Summary</td></tr></table>"""
    scraper.http.request = AsyncMock(return_value=make_mock_response(html))
    refs = [r async for r in scraper.list_documents(2024)]
    laws = [r for r in refs if not r.get("constitution")]
    assert len(laws) == 1 and laws[0]["year"] == 2024
    assert "123" in laws[0]["document_url"]
    scraper.http.request.assert_awaited_once()
    scraper.http.request.return_value = make_mock_response("<html>Error</html>")
    with pytest.raises(ValueError, match="results table"):
        [r async for r in scraper.list_documents(2024)]


async def test_df_offsets_follow_page_size_even_if_first_page_is_short():
    scraper = make_scraper(DFSinjScraper, _display_length=3)
    scraper.http.request = AsyncMock(
        side_effect=[
            make_mock_json_response(
                {
                    "aaData": [
                        {
                            "_source": {
                                "ch_norma": "1",
                                "nm_tipo_norma": "Lei",
                                "nr_norma": "1",
                                "nm_situacao": "",
                            }
                        }
                    ],
                    "iTotalDisplayRecords": 7,
                }
            ),
            make_mock_json_response({"aaData": [], "iTotalDisplayRecords": 7}),
            make_mock_json_response({"aaData": [], "iTotalDisplayRecords": 7}),
        ]
    )
    refs = [r async for r in scraper.list_documents(2024)]
    assert len(refs) == 1
    offsets = [
        dict(c.kwargs["data"])["iDisplayStart"]
        for c in scraper.http.request.await_args_list
    ]
    assert sorted(offsets) == [0, 3, 6]
    assert refs[0]["situation"]


async def test_conama_html_response_is_fetched_once():
    scraper = make_scraper(ConamaScraper)
    scraper.http.request = AsyncMock(
        return_value=make_mock_response(f'<div class="texto-dou"><p>{TEXT}</p></div>')
    )
    doc = await scraper.fetch(
        {"aid": 1, "numero": "1", "nomeato": "Resolução", "ano": 2024}
    )
    assert isinstance(doc, Document) and doc.raw_ext == ".html"
    scraper.http.request.assert_awaited_once()


@pytest.mark.parametrize("status", ["", "<p>Situação: Vigente</p>"])
def test_parana_article_revocation_does_not_revoke_document(status):
    soup = BeautifulSoup(
        f"{status}<p>Revogado pelo artigo 2: apenas o dispositivo anterior.</p>", "lxml"
    )
    assert ParanaCVScraper._infer_situation(soup) == DEFAULT_VALID_SITUATION


async def test_amazonas_error_page_raises():
    scraper = make_scraper(LegislaAMScraper)
    scraper.http.request = AsyncMock(
        return_value=make_mock_response('<div id="container"><h1>Error</h1></div>')
    )
    with pytest.raises(ValueError, match="error page"):
        await scraper._get_docs_links("url")


@pytest.mark.parametrize("alias", ["PB", "PI", "RR", "RN", "RO", "PA", "MA", "TO"])
async def test_pdf_fetch_contracts(alias):
    from test_pdf_pipeline import _make_digital_pdf

    spec = next(spec for spec in SCRAPERS if alias.lower() in spec.names)
    scraper = make_scraper(spec.cls)
    pdf = _make_digital_pdf(TEXT)
    scraper.http.request = AsyncMock(
        return_value=make_mock_response(pdf, content_type="application/pdf")
    )
    ref = {**document().model_dump(), "pdf_link": "https://example.org/law.pdf"}
    ref.pop("text_markdown")
    result = await scraper.fetch(ref)
    assert isinstance(result, Document) and result.raw == pdf
    assert result.raw_ext == ".pdf" and "Art." in result.text_markdown


@pytest.mark.parametrize(
    ("alias", "html", "extra"),
    [
        ("DF", f'<div id="div_texto"><p>{TEXT}</p></div>', {}),
        (
            "MG",
            f'<a href="/texto">Texto atualizado</a><span class="textNorma"><p>{TEXT}</p></span>',
            {"html_link": "/details"},
        ),
        ("MT", f"<turbo-frame><p>{TEXT}</p></turbo-frame>", {"norm_link": "/lei/1"}),
        (
            "RJ",
            f'<div id="divConteudo"><p>{TEXT}</p></div>',
            {"_html_link": "https://example.org/doc"},
        ),
        ("PE", f'<div class="WordSection1"><p>{TEXT}</p></div>', {}),
        ("SC", f"<p>{TEXT}</p>", {}),
        (
            "CamaraDep",
            f'<div class="sessao"><a href="/texto">Texto - Publicação Original</a></div><div id="content"><p>{TEXT}</p></div>',
            {"metadata_url": "https://example.org/details"},
        ),
    ],
)
async def test_other_html_fetch_contracts(alias, html, extra):
    spec = next(spec for spec in SCRAPERS if alias.lower() in spec.names)
    scraper = make_scraper(spec.cls)
    body = f"<html><body>{html}</body></html>"
    scraper.http.request = AsyncMock(return_value=make_mock_response(body))
    scraper.browser.snapshot = AsyncMock(
        return_value=(BeautifulSoup(body, "lxml"), b"mhtml")
    )
    ref = {**document().model_dump(), **extra}
    ref.pop("text_markdown")
    result = await scraper.fetch(ref)
    assert isinstance(result, Document) and result.raw
    assert result.type and result.situation and result.year == 2024
    assert "Art." in result.text_markdown


@pytest.mark.parametrize("alias", ["AL", "GO", "SE"])
async def test_api_fetch_contracts(alias):
    import base64

    from test_pdf_pipeline import _make_digital_pdf

    scraper = make_scraper(
        next(spec.cls for spec in SCRAPERS if alias.lower() in spec.names)
    )
    ref = document().model_dump()
    ref.pop("text_markdown")
    if alias == "AL":
        ref.update(
            link={"key": "k", "acess": "a"},
            numeroDocumento="1",
            tipoDocumento={"descricao": "Lei Ordinária"},
            _year=2024,
        )
        data = {
            "arquivo": {
                "nomeArquivo": "lei.pdf",
                "base64": base64.b64encode(_make_digital_pdf(TEXT)).decode(),
            }
        }
    elif alias == "GO":
        ref.update(
            id=1,
            numero="1",
            ano=2024,
            tipo_legislacao={"id": 2, "nome": "Lei Ordinária"},
        )
        data = {
            **ref,
            "conteudo": f"<p>{TEXT}</p>",
            "estado_legislacao": {"nome": "Vigente"},
        }
    else:
        ref["doc_id"] = 1
        data = {"content": [{"conteudo": TEXT}]}
    scraper.http.request = AsyncMock(return_value=make_mock_json_response(data))
    result = await scraper.fetch(ref)
    assert isinstance(result, Document) and result.raw
    assert result.year == 2024 and "Art." in result.text_markdown


async def test_rs_fetch_defaults_status_and_resolves_relative_pdf():
    from test_pdf_pipeline import _make_digital_pdf

    scraper = make_scraper(RSAlrsScraper)
    pdf = _make_digital_pdf(TEXT)
    scraper.http.request = AsyncMock(
        side_effect=[
            make_mock_response(
                '<table><tr><td>Links:</td><td><a href="text.asp">Texto</a></td></tr></table>'
            ),
            make_mock_response('<iframe src="../files/lei.pdf"></iframe>'),
            make_mock_response(pdf, content_type="application/pdf"),
        ]
    )
    ref = {
        **document().model_dump(),
        "html_link": "https://www.al.rs.gov.br/legis/M010/details.asp",
    }
    ref.pop("text_markdown")
    result = await scraper.fetch(ref)
    assert result.situation == DEFAULT_VALID_SITUATION
    assert result.document_url == "https://www.al.rs.gov.br/legis/files/lei.pdf"


@pytest.mark.parametrize(
    ("status", "expected"),
    [("", DEFAULT_VALID_SITUATION), ('<p class="situacao">Revogada</p>', "Revogada")],
)
async def test_rj_status_scoped_before_cleaning(status, expected):
    from src.scrapers.states.rj import RJAlerjScraper

    scraper = make_scraper(RJAlerjScraper)
    soup = BeautifulSoup(
        f'<div id="divConteudo">{status}<p>{TEXT}</p><p>Revogado apenas o artigo anterior.</p></div>',
        "lxml",
    )
    scraper.browser.snapshot = AsyncMock(return_value=(soup, b"mhtml"))
    ref = {**document().model_dump(), "_html_link": "https://example.org/doc"}
    ref.pop("text_markdown")
    assert (await scraper.fetch(ref)).situation == expected
