from __future__ import annotations

import re
from typing import TYPE_CHECKING, cast
from urllib.parse import urlencode, urljoin

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument

import aiohttp
from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.scraper.base.converter import calc_pages, valid_markdown, wrap_html
from src.scraper.base.scraper import StateScraper

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument

TYPES = {
    "Constituição Estadual": {"id": 12, "url_suffix": "constituicao-estadual"},
    "Emenda Constitucional": {"id": 13, "url_suffix": "emenda-constitucional"},
    "Lei Complementar": {"id": 1, "url_suffix": "lei-complementar"},
    "Lei Ordinária": {"id": 2, "url_suffix": "lei"},
    "Lei Delegada": {"id": 4, "url_suffix": "lei-delegada"},
    "Decreto Lei": {"id": 8, "url_suffix": "decreto-lei"},
    "Decreto Numerado": {"id": 3, "url_suffix": "decreto"},
    "Decreto Orçamentário": {"id": 5, "url_suffix": "decreto-orcamentario"},
    "Portaria Orçamentária": {"id": 6, "url_suffix": "portaria-orcamentaria"},
    "Resolução": {"id": 7, "url_suffix": "resolucao"},
}

# situations are gotten from doc data while scraping
SITUATIONS = {}

_SITE_URL = "https://legisla.casacivil.go.gov.br"
_DETAIL_API_URL = f"{_SITE_URL}/api/v2/pesquisa/legislacoes"
_SEARCH_PATH = "/api/v2/pesquisa/legislacoes"
_REDIRECT_MARKER = "Clique no link abaixo para acessar a:"
_PDF_STUB_MIN_BODY_LEN = 150
_TYPE_ID_TO_SUFFIX = {v["id"]: v["url_suffix"] for v in TYPES.values()}

# Pre-compiled for the content-threshold check in _get_doc_data
_NON_ALPHA_RE = re.compile(r"[^a-zA-ZÀ-ÿ]")
_SPECIAL_RE = re.compile(r"[^a-zA-ZÀ-ÿ0-9\s]")
_SPACE_RE = re.compile(r"\s+")


def _remove_summary_from_markdown(md: str, summary: str) -> str:
    """Remove the summary paragraph from text_markdown to avoid duplication.

    Brazilian legislation documents include the ementa as a standalone paragraph
    near the top of the body. Since the summary is already stored separately, it
    is stripped here to prevent verbatim repetition inside ``text_markdown``.
    Only exact standalone-paragraph matches (after normalizing whitespace and
    stripping markdown formatting chars) are removed; summary text embedded
    inside article body text is left untouched.
    """
    if not summary:
        return md
    summary_norm = re.sub(r"\s+", " ", summary).strip()
    paragraphs = md.split("\n\n")
    filtered = [
        p
        for p in paragraphs
        if re.sub(r"\s+", " ", re.sub(r"[*_`]+", "", p)).strip() != summary_norm
    ]
    if len(filtered) == len(paragraphs):
        return md  # summary not found as a standalone paragraph
    return "\n\n".join(filtered)


class LegislaGoias(StateScraper):
    """Webscraper for Goias state legislation website (https://legisla.casacivil.go.gov.br)

    Year start (earliest on source): 1887

    Example search request: https://legisla.casacivil.go.gov.br/api/v2/pesquisa/legislacoes?ano=1887&ordenarPor=data&page=1&qtd_por_pagina=100
    """

    def __init__(
        self,
        base_url: str = f"{_SITE_URL}{_SEARCH_PATH}",
        **kwargs,
    ):
        super().__init__(
            base_url, name="GOIAS", types=TYPES, situations=SITUATIONS, **kwargs
        )

    @staticmethod
    def _resolve_norm_type(doc: dict, doc_detail: dict) -> str:
        """Resolve the most reliable norm type from detail or search payloads."""
        detail_type = (
            (doc_detail.get("tipo_legislacao") or {}).get("nome") or ""
        ).strip()
        if detail_type:
            return detail_type

        search_type = ((doc.get("tipo_legislacao") or {}).get("nome") or "").strip()
        if search_type:
            return search_type

        return ""

    def _build_search_url(
        self, year: int, page: int = 1, norm_type_id: int | None = None
    ) -> str:
        """Build search URL from arguments (no shared state mutation)."""
        params = {
            "ano": year,
            "ordenarPor": "data",
            "qtd_por_pagina": 100,
            "page": page,
        }
        if norm_type_id is not None:
            params["tipo_legislacao"] = norm_type_id
        return f"{self.base_url}?{urlencode(params)}"

    def _build_document_url(
        self, doc_id: int | str, tipo_id: int | None, numero: str
    ) -> str:
        """Build the canonical public document URL used for resume keys."""
        norm_url_suffix = _TYPE_ID_TO_SUFFIX.get(tipo_id, "")
        if norm_url_suffix == "constituicao-estadual":
            return f"{_SITE_URL}/pesquisa_legislacao/{doc_id}/{norm_url_suffix}"
        return f"{_SITE_URL}/pesquisa_legislacao/{doc_id}/{norm_url_suffix}-{numero}"

    def _clean_markdown(self, text_markdown: str) -> str:
        """Clean markdown text."""
        return text_markdown.replace("javascript:print()", "").strip()

    def _normalize_for_compare(self, text: str) -> str:
        """Normalize text for body-vs-summary comparison."""
        normalized = _SPECIAL_RE.sub("", text.lower())
        return _SPACE_RE.sub("", normalized).strip()

    def _ensure_html_document(self, html_content: str) -> str:
        """Return a standalone HTML document string."""
        html_string = html_content.strip()
        if re.search(r"<(?:!doctype|html|body)\b", html_string, re.IGNORECASE):
            return html_string
        return wrap_html(html_string)

    def _extract_pdf_link(self, soup: BeautifulSoup | Tag) -> str:
        """Extract a downloadable PDF link from the HTML, if present."""
        pdf_link = ""

        for a_tag in soup.find_all("a", href=True):
            img = a_tag.find("img", src=re.compile(r"/assets/ver_lei\.jpg$"))
            if img:
                pdf_link = urljoin(_SITE_URL, cast(str, a_tag["href"]))
                a_tag.decompose()
                break

        if pdf_link:
            return pdf_link

        baixar_div = soup.find("div", class_="botao-baixar")
        if baixar_div:
            a_tag = baixar_div.find("a", href=True)
            if a_tag:
                pdf_link = urljoin(_SITE_URL, cast(str, a_tag["href"]))
            baixar_div.decompose()

        return pdf_link

    def _prepare_inline_html(self, html_content: str) -> tuple[str, str, bool]:
        """Clean inline HTML and return ``(clean_html, pdf_link, redirect)``."""
        raw_html = self._ensure_html_document(html_content)
        soup = BeautifulSoup(raw_html, "html.parser")

        if _REDIRECT_MARKER.lower() in soup.get_text(" ", strip=True).lower():
            return "", "", True

        pdf_link = self._extract_pdf_link(soup)
        root = soup.body or soup

        for tag_name in ["meta", "link", "title", "input", "label"]:
            for tag in root.find_all(tag_name):
                tag.decompose()

        for tag in root.find_all(id=re.compile(r"ficha-tecnica", re.IGNORECASE)):
            tag.decompose()
        for tag in root.find_all(class_=re.compile(r"ficha-tecnica", re.IGNORECASE)):
            tag.decompose()

        header_table = root.find("table")
        if (
            header_table
            and "GOVERNO DO ESTADO DE GOIÁS"
            in header_table.get_text(" ", strip=True).upper()
        ):
            header_table.decompose()

        self._clean_norm_soup(
            root,
            unwrap_links=True,
            remove_disclaimers=True,
            remove_images=True,
            remove_empty_tags=True,
            unwrap_fonts=True,
            strip_styles=True,
            remove_style_tags=True,
            remove_script_tags=True,
        )

        return self._ensure_html_document(str(root).strip()), pdf_link, False

    def _should_use_pdf_fallback(
        self, text_markdown: str, summary: str, *, has_pdf_link: bool
    ) -> bool:
        """Return True when inline HTML is likely only a summary/download stub."""
        if not has_pdf_link:
            return False

        body_text = self._normalize_for_compare(text_markdown)
        summary_text = self._normalize_for_compare(summary)
        if summary_text:
            body_text = body_text.replace(summary_text, "").strip()

        return len(body_text) < _PDF_STUB_MIN_BODY_LEN

    def _build_doc_info(
        self, doc: dict, doc_detail: dict, *, norm_type: str, situation: str
    ) -> dict:
        """Build the normalized document metadata dict."""
        title_type = norm_type or (doc_detail.get("tipo_legislacao") or {}).get(
            "nome", "Norma"
        )

        return {
            "id": doc_detail["id"],
            "norm_number": doc_detail.get("numero", ""),
            "year": doc_detail.get("ano", doc.get("ano", "")),
            "type": norm_type,
            "situation": situation,
            "date": doc_detail.get("data_legislacao", ""),
            "title": (
                f"{title_type} {doc_detail.get('numero', '')} "
                f"de {doc_detail.get('ano', doc.get('ano', ''))}"
            ).strip(),
            "summary": (doc_detail.get("ementa") or "").strip(),
        }

    async def _process_pdf_link(
        self, link: str, doc_id: str | int, doc_info: dict
    ) -> dict | None:
        """Download a PDF fallback, convert it to markdown, and populate doc_info."""
        pdf_link = urljoin(_SITE_URL, link).strip()
        if not pdf_link:
            logger.error(f"Missing PDF link for doc ID: {doc_id}")
            return None

        original_document_url = doc_info.get("document_url", "")
        text_markdown, raw_content, content_ext = await self._download_and_convert(
            pdf_link
        )
        text_markdown = self._clean_markdown(text_markdown)

        result = await self._process_doc(
            doc_info,
            pdf_link,
            text_markdown,
            raw_content,
            content_ext or ".pdf",
            error_prefix="Failed to process PDF",
        )
        if result is None:
            logger.error(f"Failed to extract text from PDF for doc ID: {doc_id}")
            return None

        summary = doc_info.get("summary", "")
        if summary and result.get("text_markdown"):
            result.text_markdown = _remove_summary_from_markdown(
                result["text_markdown"], summary
            )

        if original_document_url:
            result.document_url = original_document_url
            result.pdf_link = pdf_link
        else:
            result.document_url = pdf_link

        return result

    async def _get_doc_data(self, doc: dict) -> ScrapedDocument | None:
        """Get document info from the detail API and choose HTML or PDF content."""
        doc_id = doc["id"]
        numero = doc.get("numero", "")
        tipo_legislacao = doc.get("tipo_legislacao", {})
        tipo_nome = tipo_legislacao.get("nome", "")
        tipo_id = tipo_legislacao.get("id")
        ano = doc.get("ano", "")
        title = f"{tipo_nome} {numero} de {ano}"
        html_link = self._build_document_url(doc_id, tipo_id, numero)

        if self._is_already_scraped(html_link, title):
            return None

        api_url = f"{_DETAIL_API_URL}/{doc_id}"
        response = await self.request_service.make_request(api_url)
        if not response:
            logger.error(f"Error getting detailed data for doc ID: {doc_id}")
            await self._save_doc_error(
                title=f"Doc ID {doc_id}",
                html_link=api_url,
                error_message="Failed to fetch document detail from API",
            )
            return None

        doc_detail = await cast(aiohttp.ClientResponse, response).json()
        situation = (doc_detail.get("estado_legislacao") or {}).get("nome", "") or (
            doc.get("estado_legislacao") or {}
        ).get("nome", "")
        norm_type = self._resolve_norm_type(doc, doc_detail)
        doc_info = self._build_doc_info(
            doc,
            doc_detail,
            norm_type=norm_type,
            situation=situation,
        )
        doc_info["document_url"] = html_link

        pdf_link = ""
        html_reason = ""

        if doc_detail.get("conteudo"):
            clean_html, pdf_link, has_redirect = self._prepare_inline_html(
                doc_detail["conteudo"]
            )
            if has_redirect:
                await self._save_doc_error(
                    title=doc_info.get("title", f"Doc ID {doc_id}"),
                    year=doc_detail.get("ano", ""),
                    situation=doc_info.get("situation", ""),
                    norm_type=doc_detail.get("tipo_legislacao", {}).get("nome", ""),
                    html_link=api_url,
                    error_message=(
                        "Document redirects via 'Clique no link abaixo' "
                        "(content not inline)"
                    ),
                )
                return None

            text_markdown = self._clean_markdown(
                await self._get_markdown(html_content=clean_html)
            )
            html_valid, html_reason = valid_markdown(text_markdown)
            if html_valid and not self._should_use_pdf_fallback(
                text_markdown,
                doc_info["summary"],
                has_pdf_link=bool(pdf_link),
            ):
                doc_info["text_markdown"] = _remove_summary_from_markdown(
                    text_markdown, doc_info["summary"]
                )
                doc_info["_raw_content"] = self._ensure_html_document(
                    doc_detail["conteudo"]
                ).encode("utf-8")
                doc_info["_content_extension"] = ".html"

        if not doc_info.get("text_markdown"):
            if pdf_link:
                doc_info = await self._process_pdf_link(pdf_link, doc_id, doc_info)
            else:
                await self._save_doc_error(
                    title=doc_info.get("title", f"Doc ID {doc_id}"),
                    year=doc_detail.get("ano", ""),
                    situation=doc_info.get("situation", ""),
                    norm_type=doc_detail.get("tipo_legislacao", {}).get("nome", ""),
                    html_link=api_url,
                    error_message=(
                        html_reason or "No usable inline HTML content or PDF fallback"
                    ),
                )
                return None

            if not doc_info:
                await self._save_doc_error(
                    title=f"Doc ID {doc_id}",
                    year=doc_detail.get("ano", ""),
                    norm_type=doc_detail.get("tipo_legislacao", {}).get("nome", ""),
                    html_link=pdf_link if pdf_link else api_url,
                    error_message="PDF processing failed (fallback)",
                )
                return None

        error_msg = "doesn't work properly without JavaScript enabled"
        if error_msg.lower() in doc_info["text_markdown"].lower():
            logger.warning(f"Invalid doc ID: {doc_id}. Year: {doc_detail['ano']}")
            await self._save_doc_error(
                title=doc_info.get("title", f"Doc ID {doc_id}"),
                year=doc_detail.get("ano", ""),
                situation=doc_info.get("situation", ""),
                norm_type=doc_detail.get("tipo_legislacao", {}).get("nome", ""),
                html_link=doc_info.get("document_url", api_url),
                error_message="Document contains JavaScript error message",
            )
            return None

        from src.scraper.base.schemas import ScrapedDocument

        return ScrapedDocument(**doc_info)

    async def _fetch_search_page(self, year: int, page: int) -> list[dict]:
        """Fetch a specific page from the search API."""
        url = self._build_search_url(year, page=page)
        response = await self.request_service.make_request(url)
        if not response:
            logger.error(f"Error fetching search page {page} for year {year}")
            return []

        data = await cast(aiohttp.ClientResponse, response).json()
        return data.get("resultados", [])

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape norms for a specific year (all types in one API call)."""
        url = self._build_search_url(year, page=1)
        response = await self.request_service.make_request(url)

        if not response:
            logger.error(f"Error getting data for Year: {year}")
            return []

        data = await cast(aiohttp.ClientResponse, response).json()
        total_results = data["total_resultados"]
        if total_results == 0:
            return []

        docs = data.get("resultados", [])
        docs.extend(
            await self._fetch_all_pages(
                lambda page: self._fetch_search_page(year, page),
                calc_pages(total_results, 100),
                context={"year": year, "type": "NA", "situation": "NA"},
                desc=f"GOIAS | year {year} page metadata",
            )
        )

        if not docs:
            return []

        return await self._process_documents(
            documents=docs,
            year=year,
            norm_type="NA",
            situation="NA",
            desc=f"GOIAS | year {year}",
        )
