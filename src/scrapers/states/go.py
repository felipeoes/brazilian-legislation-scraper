import re
from collections.abc import AsyncIterator
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.convert import (
    calc_pages,
    clean_norm_soup,
    remove_summary_from_markdown,
    valid_markdown,
    wrap_html,
)
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

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
SITUATIONS = {}
_SITE_URL = "https://legisla.casacivil.go.gov.br"
_DETAIL_API_URL = f"{_SITE_URL}/api/v2/pesquisa/legislacoes"
_SEARCH_PATH = "/api/v2/pesquisa/legislacoes"
_REDIRECT_MARKER = "Clique no link abaixo para acessar a:"
_PDF_STUB_MIN_BODY_LEN = 150
_TYPE_ID_TO_SUFFIX = {v["id"]: v["url_suffix"] for v in TYPES.values()}
_NON_ALPHA_RE = re.compile("[^a-zA-ZÀ-ÿ]")
_SPECIAL_RE = re.compile("[^a-zA-ZÀ-ÿ0-9\\s]")
_SPACE_RE = re.compile("\\s+")


class LegislaGoias(BaseScraper):
    """Webscraper for Goias state legislation website (https://legisla.casacivil.go.gov.br)

    Year start (earliest on source): 1887

    Example search request: https://legisla.casacivil.go.gov.br/api/v2/pesquisa/legislacoes?ano=1887&ordenarPor=data&page=1&qtd_por_pagina=100
    """

    name = "GOIAS"
    types = TYPES
    situations = SITUATIONS
    base_url = f"{_SITE_URL}{_SEARCH_PATH}"

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
        if re.search("<(?:!doctype|html|body)\\b", html_string, re.IGNORECASE):
            return html_string
        return wrap_html(html_string)

    def _extract_pdf_link(self, soup: BeautifulSoup | Tag) -> str:
        """Extract a downloadable PDF link from the HTML, if present."""
        pdf_link = ""
        for a_tag in soup.find_all("a", href=True):
            img = a_tag.find("img", src=re.compile("/assets/ver_lei\\.jpg$"))
            if img:
                pdf_link = urljoin(_SITE_URL, a_tag["href"])
                a_tag.decompose()
                break
        if pdf_link:
            return pdf_link
        baixar_div = soup.find("div", class_="botao-baixar")
        if baixar_div:
            a_tag = baixar_div.find("a", href=True)
            if a_tag:
                pdf_link = urljoin(_SITE_URL, a_tag["href"])
            baixar_div.decompose()
        return pdf_link

    def _prepare_inline_html(self, html_content: str) -> tuple[str, str, bool]:
        """Clean inline HTML and return ``(clean_html, pdf_link, redirect)``."""
        raw_html = self._ensure_html_document(html_content)
        soup = BeautifulSoup(raw_html, "lxml")
        if _REDIRECT_MARKER.lower() in soup.get_text(" ", strip=True).lower():
            return ("", "", True)
        pdf_link = self._extract_pdf_link(soup)
        root = soup.body or soup
        for tag_name in ["meta", "link", "title", "input", "label"]:
            for tag in root.find_all(tag_name):
                tag.decompose()
        for tag in root.find_all(id=re.compile("ficha-tecnica", re.IGNORECASE)):
            tag.decompose()
        for tag in root.find_all(class_=re.compile("ficha-tecnica", re.IGNORECASE)):
            tag.decompose()
        header_table = root.find("table")
        if (
            header_table
            and "GOVERNO DO ESTADO DE GOIÁS"
            in header_table.get_text(" ", strip=True).upper()
        ):
            header_table.decompose()
        clean_norm_soup(
            root,
            unwrap_links=True,
            remove_disclaimers=True,
            remove_images=False,
            remove_empty_tags=True,
            unwrap_fonts=True,
            strip_styles=True,
            remove_style_tags=True,
            remove_script_tags=True,
        )
        return (self._ensure_html_document(str(root).strip()), pdf_link, False)

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
            "title": f"{title_type} {doc_detail.get('numero', '')} de {doc_detail.get('ano', doc.get('ano', ''))}".strip(),
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
        text_markdown, raw, content_ext = await self.download(pdf_link)
        text_markdown = self._clean_markdown(text_markdown)
        result = Document(
            **{
                **doc_info,
                "document_url": pdf_link,
                "text_markdown": text_markdown,
                "raw": raw,
                "raw_ext": content_ext or ".pdf",
            }
        )
        if result is None:
            logger.error(f"Failed to extract text from PDF for doc ID: {doc_id}")
            return None
        summary = doc_info.get("summary", "")
        if summary and result.text_markdown:
            result.text_markdown = remove_summary_from_markdown(
                result.text_markdown, summary
            )
        if original_document_url:
            result.document_url = original_document_url
            result.pdf_link = pdf_link
        else:
            result.document_url = pdf_link
        return result

    async def fetch(self, doc: dict) -> Document | None:
        """Get document info from the detail API and choose HTML or PDF content."""
        doc_id = doc["id"]
        numero = doc.get("numero", "")
        tipo_legislacao = doc.get("tipo_legislacao", {})
        tipo_id = tipo_legislacao.get("id")
        html_link = self._build_document_url(doc_id, tipo_id, numero)
        api_url = f"{_DETAIL_API_URL}/{doc_id}"
        response = await self.http.request(api_url)
        doc_detail = response.json()
        situation = (doc_detail.get("estado_legislacao") or {}).get("nome", "") or (
            doc.get("estado_legislacao") or {}
        ).get("nome", "")
        norm_type = self._resolve_norm_type(doc, doc_detail)
        doc_info = self._build_doc_info(
            doc, doc_detail, norm_type=norm_type, situation=situation
        )
        doc_info["document_url"] = html_link
        pdf_link = ""
        html_reason = ""
        if doc_detail.get("conteudo"):
            clean_html, pdf_link, has_redirect = self._prepare_inline_html(
                doc_detail["conteudo"]
            )
            if has_redirect:
                raise ValueError(
                    "Document redirects via 'Clique no link abaixo' (content not inline)"
                )
            text_markdown = self._clean_markdown(await self.markdown(html=clean_html))
            html_valid, html_reason = valid_markdown(text_markdown)
            if html_valid and (
                not self._should_use_pdf_fallback(
                    text_markdown, doc_info["summary"], has_pdf_link=bool(pdf_link)
                )
            ):
                doc_info["text_markdown"] = remove_summary_from_markdown(
                    text_markdown, doc_info["summary"]
                )
                doc_info["raw"] = self._ensure_html_document(
                    doc_detail["conteudo"]
                ).encode("utf-8")
                doc_info["raw_ext"] = ".html"
        if not doc_info.get("text_markdown"):
            if pdf_link:
                return await self._process_pdf_link(pdf_link, doc_id, doc_info)
            else:
                raise ValueError(
                    html_reason or "No usable inline HTML content or PDF fallback"
                )
            if not doc_info:
                raise ValueError("PDF processing failed (fallback)")
        error_msg = "doesn't work properly without JavaScript enabled"
        if error_msg.lower() in doc_info["text_markdown"].lower():
            logger.warning(f"Invalid doc ID: {doc_id}. Year: {doc_detail['ano']}")
            raise ValueError("Document contains JavaScript error message")
        from src.core.document import Document

        return Document(**doc_info)

    async def _fetch_search_page(self, year: int, page: int) -> list[dict]:
        """Fetch a specific page from the search API."""
        url = self._build_search_url(year, page=page)
        response = await self.http.request(url)
        data = response.json()
        return data.get("resultados", [])

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        data = (await self.http.request(self._build_search_url(year, page=1))).json()

        async def fetch_page(page: int) -> list[dict]:
            return (
                data.get("resultados", [])
                if page == 1
                else await self._fetch_search_page(year, page)
            )

        async for ref in self.paginate(
            fetch_page, calc_pages(data["total_resultados"], 100)
        ):
            norm_type = ref.get("tipo_legislacao", {})
            yield {
                **ref,
                "year": year,
                "type": norm_type.get("nome", ""),
                "title": f"{norm_type.get('nome', '')} {ref.get('numero', '')} de {ref.get('ano', year)}",
                "document_url": self._build_document_url(
                    ref["id"], norm_type.get("id"), ref.get("numero", "")
                ),
                "situation": DEFAULT_VALID_SITUATION,
            }
