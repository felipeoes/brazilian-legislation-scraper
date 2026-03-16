from __future__ import annotations

import re
from io import BytesIO
from typing import TYPE_CHECKING

from bs4 import BeautifulSoup
from loguru import logger
from urllib.parse import urlencode

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

_TYPE_ID_TO_SUFFIX = {v["id"]: v["url_suffix"] for v in TYPES.values()}


class LegislaGoias(StateScraper):
    """Webscraper for Goias state legislation website (https://legisla.casacivil.go.gov.br)

    Year start (earliest on source): 1798

    Example search request: https://legisla.casacivil.go.gov.br/api/v2/pesquisa/legislacoes?ano=1798&ordenarPor=data&page=1&qtd_por_pagina=100
    """

    def __init__(
        self,
        base_url: str = "https://legisla.casacivil.go.gov.br/api/v2/pesquisa/legislacoes",
        **kwargs,
    ):
        super().__init__(
            base_url, name="GOIAS", types=TYPES, situations=SITUATIONS, **kwargs
        )
        self._special_regex = re.compile(r"[^a-zA-ZÀ-ÿ\s]")
        self._space_regex = re.compile(r"\s+")

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

    def _clean_markdown(self, text_markdown: str) -> str:
        """Clean markdown text"""

        return text_markdown.replace("javascript:print()", "").strip()

    async def _process_pdf_link(
        self, link: str, doc_id: str, doc_info: dict
    ) -> dict | None:
        response = await self.request_service.make_request(link)
        if not response:
            logger.error(f"Error fetching PDF for doc ID: {doc_id} | Link: {link}")
            return None

        raw_content = await response.read()
        text_markdown = await self._get_markdown(stream=BytesIO(raw_content))
        valid, reason = valid_markdown(text_markdown)
        if not valid:
            logger.error(
                f"Failed to extract text from PDF for doc ID: {doc_id}: {reason}"
            )
            return None
        doc_info["text_markdown"] = text_markdown
        doc_info["_raw_content"] = raw_content
        doc_info["_content_extension"] = ".pdf"
        if not doc_info.get("document_url"):
            doc_info["document_url"] = link
        else:
            doc_info["pdf_link"] = link

        doc_info["text_markdown"] = self._clean_markdown(doc_info["text_markdown"])

        return doc_info

    async def _get_doc_info(self, doc: dict) -> ScrapedDocument | None:
        """Get document info from given doc data using API"""
        doc_id = doc["id"]
        numero = doc.get("numero", "")
        tipo_legislacao = doc.get("tipo_legislacao", {})
        tipo_nome = tipo_legislacao.get("nome", "")
        tipo_id = tipo_legislacao.get("id")
        ano = doc.get("ano", "")
        norm_url_suffix = _TYPE_ID_TO_SUFFIX.get(tipo_id, "")
        title = f"{tipo_nome} {numero} de {ano}"

        # Build canonical URL for resume check
        if norm_url_suffix == "constituicao-estadual":
            html_link = f"https://legisla.casacivil.go.gov.br/pesquisa_legislacao/{doc_id}/{norm_url_suffix}"
        else:
            html_link = f"https://legisla.casacivil.go.gov.br/pesquisa_legislacao/{doc_id}/{norm_url_suffix}-{numero}"

        if self._is_already_scraped(html_link, title):
            return None

        # Fetch detail API
        api_url = (
            f"https://legisla.casacivil.go.gov.br/api/v2/pesquisa/legislacoes/{doc_id}"
        )
        response = await self.request_service.make_request(api_url)

        if not response:
            logger.error(f"Error getting detailed data for doc ID: {doc_id}")
            await self._save_doc_error(
                title=f"Doc ID {doc_id}",
                html_link=api_url,
                error_message="Failed to fetch document detail from API",
            )
            return None

        doc_detail = await response.json()

        # Use detail API situation, fall back to search result situation
        situation = (doc_detail.get("estado_legislacao") or {}).get("nome", "") or (
            doc.get("estado_legislacao") or {}
        ).get("nome", "")
        norm_type = self._resolve_norm_type(doc, doc_detail)

        title_type = norm_type or (doc_detail.get("tipo_legislacao") or {}).get(
            "nome", "Norma"
        )

        doc_info = {
            "id": doc_detail["id"],
            "norm_number": doc_detail["numero"],
            "type": norm_type,
            "situation": situation,
            "date": doc_detail["data_legislacao"],
            "title": f"{title_type} {doc_detail['numero']} de {doc_detail['ano']}",
            "summary": doc_detail["ementa"].strip(),
        }

        pdf_link = ""

        # Check if we have formatted content (HTML)
        if doc_detail.get("conteudo"):
            html_content = doc_detail["conteudo"]

            # Parse HTML with BeautifulSoup to clean it up
            soup = BeautifulSoup(html_content, "html.parser")

            # check if "Clique no link abaixo para acessar a:" in soup and skip document
            if (
                "Clique no link abaixo para acessar a:".lower()
                in str(soup.prettify()).lower()
            ):
                await self._save_doc_error(
                    title=doc_info.get("title", f"Doc ID {doc_id}"),
                    year=doc_detail.get("ano", ""),
                    situation=doc_info.get("situation", ""),
                    norm_type=doc_detail.get("tipo_legislacao", {}).get("nome", ""),
                    html_link=api_url,
                    error_message="Document redirects via 'Clique no link abaixo' (content not inline)",
                )
                return None

            # remove header table, if it contains GOVERNO DO ESTADO DE GOIÁS
            header_table = soup.find("table")
            if (
                header_table
                and "GOVERNO DO ESTADO DE GOIÁS".lower() in header_table.text.lower()
            ):
                header_table.decompose()

            # remove a tag if it has <img src="/assets/ver_lei.jpg"> and extract pdf link
            for a_tag in soup.find_all("a"):
                img = a_tag.find("img", src="/assets/ver_lei.jpg")
                if img:
                    pdf_link = a_tag["href"]
                    a_tag.decompose()

            # If no ver_lei.jpg link, try baixar_div as secondary source
            if not pdf_link:
                baixar_div = soup.find("div", class_="botao-baixar")
                if baixar_div:
                    a_tag = baixar_div.find("a", href=True)
                    if a_tag:
                        pdf_link = a_tag["href"]
                    baixar_div.decompose()

            html_string = soup.prettify().strip()

            if not html_string.startswith("<html"):
                html_string = wrap_html(html_string)

            # Convert HTML to markdown using direct HTML content
            text_markdown = await self._get_markdown(html_content=html_string)
            valid, reason = valid_markdown(text_markdown)
            if valid:
                text_markdown = text_markdown.strip()
                doc_info["text_markdown"] = text_markdown
                try:
                    doc_info["_raw_content"] = await self._capture_mhtml(html_link)
                    doc_info["_content_extension"] = ".mhtml"
                except Exception as exc:
                    logger.warning(
                        f"MHTML capture failed for {html_link}: {exc} — falling back to PDF"
                    )
                    doc_info["text_markdown"] = None  # force PDF fallback below

                new_text = self._special_regex.sub("", text_markdown.lower())
                new_text = self._space_regex.sub("", new_text).strip()
                compare_summary = self._special_regex.sub(
                    "", doc_info["summary"].lower()
                )
                compare_summary = self._space_regex.sub("", compare_summary).strip()

                new_text = new_text.replace(compare_summary, "").strip()
                if (
                    len(new_text) < 150
                ):  # threshold for substantial content (based on experimentation with goias norms)
                    # set text_markdown to None so that we can fall back to PDF fetching below
                    doc_info["text_markdown"] = None

            doc_info["document_url"] = html_link

        # If we don't have HTML content or markdown conversion failed, try PDF
        if not doc_info.get("text_markdown"):
            doc_info = await self._process_pdf_link(pdf_link, doc_id, doc_info)
            if not doc_info:
                logger.error(f"Failed to process PDF for doc ID: {doc_id}")
                await self._save_doc_error(
                    title=f"Doc ID {doc_id}",
                    year=doc_detail.get("ano", ""),
                    norm_type=doc_detail.get("tipo_legislacao", {}).get("nome", ""),
                    html_link=pdf_link if pdf_link else api_url,
                    error_message="PDF processing failed (fallback)",
                )
                return None

        # clean text_markdown (some docs may have the "javascript:print()" string at the end of the document)
        doc_info["text_markdown"] = self._clean_markdown(doc_info["text_markdown"])

        # check for error msg
        error_msg = "doesn't work properly without JavaScript enabled"
        if error_msg.lower() in doc_info["text_markdown"].lower():
            logger.warning(f"Invalid  doc ID: {doc_id}. Year: {doc_detail['ano']}")
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

        return ScrapedDocument(year=doc_detail.get("ano"), **doc_info)

    async def _fetch_search_page(self, year: int, page: int) -> list[dict]:
        """Fetch a specific page from the search API."""
        url = self._build_search_url(year, page=page)
        response = await self.request_service.make_request(url)
        if not response:
            logger.error(f"Error fetching search page {page} for year {year}")
            return []

        data = await response.json()
        return data.get("resultados", [])

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape norms for a specific year (all types in one API call)."""
        url = self._build_search_url(year, page=1)
        response = await self.request_service.make_request(url)

        if not response:
            logger.error(f"Error getting data for Year: {year}")
            return []

        data = await response.json()
        total_results = data["total_resultados"]

        if total_results == 0:
            return []

        docs = data.get("resultados", [])
        pages = calc_pages(total_results, 100)

        if pages > 1:
            tasks = [
                self._fetch_search_page(year, page) for page in range(2, pages + 1)
            ]
            page_results = await self._gather_results(
                tasks,
                desc=f"GOIAS | year {year} page metadata",
            )
            for pr in page_results:
                if isinstance(pr, list):
                    docs.extend(pr)
                elif isinstance(pr, Exception):
                    logger.error(
                        f"Failed to fetch a search page for year {year} | Error: {pr}"
                    )

        if not docs:
            return []

        return await self._process_documents(
            documents=docs,
            year=year,
            norm_type="NA",
            situation="NA",
            desc=f"GOIAS | year {year}",
            doc_data_fn=self._get_doc_info,
        )
