import re
from collections.abc import AsyncIterator
from typing import Any

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.convert import bytes_to_markdown, calc_pages, valid_markdown
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

TYPES = {"Lei Ordinária": "ordinaria", "Lei Complementar": "complementar"}
VALID_SITUATIONS = ["Não consta"]
INVALID_SITUATIONS = []
SITUATIONS = {s: s for s in VALID_SITUATIONS + INVALID_SITUATIONS}


class TocantinsScraper(BaseScraper):
    """Webscraper for Tocantins state legislation website (https://www.al.to.leg.br/)

    Year start (earliest on source): 1989

    Example search request: POST to https://www.al.to.leg.br/legislacaoEstadual
    """

    types = TYPES
    situations = SITUATIONS
    name = "TOCANTINS"
    base_url = "https://www.al.to.leg.br"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.search_url = f"{self.base_url}/legislacaoEstadual"

    def _format_search_url(self, _norm_type_id: str, _year: int, _page: int = 1) -> str:
        """Format url for search request - returns the search URL"""
        return self.search_url

    def _format_search_payload(
        self, norm_type_id: str, year: int, page: int = 1
    ) -> dict:
        """Format payload for search request"""
        return {
            "pagPaginaAtual": str(page),
            "documento.texto": "",
            "documento.numero": "",
            "documento.ano": str(year),
            "documento.dataInicio": "",
            "documento.dataFinal": "",
            "documento.tipo": norm_type_id,
        }

    async def _get_docs_links_page(
        self, norm_type_id: str, year: int, page: int = 1
    ) -> list[dict]:
        """Get document links from a single page"""
        payload = self._format_search_payload(norm_type_id, year, page)
        response = await self.http.request(self.search_url, method="POST", data=payload)
        client_response = response
        return self._extract_docs_from_soup(client_response.content)

    def _extract_docs_from_soup(self, html_content: bytes) -> list[dict]:
        """Extract document information from HTML content"""
        soup = BeautifulSoup(html_content, "lxml")
        docs = []
        rows = soup.find_all("div", class_="row")
        for row in rows:
            try:
                title_link = row.find("h4")
                if not title_link or not isinstance(title_link, Tag):
                    continue
                link_tag = title_link.find("a")
                if not link_tag or not isinstance(link_tag, Tag):
                    continue
                title = link_tag.get_text(strip=True)
                doc_link = link_tag.get("href", "")
                if not isinstance(doc_link, str):
                    continue
                if not doc_link.startswith("http"):
                    doc_link = f"{self.base_url}{doc_link}"
                date_text = ""
                small_tags = row.find_all("small")
                for small in small_tags:
                    if isinstance(small, Tag):
                        text = small.get_text(strip=True)
                        if "Data:" in text:
                            date_text = text.replace("Data:", "").strip()
                            date_text = date_text.replace("|", "").strip()
                            break
                summary = ""
                em_tag = row.find("em")
                if em_tag and isinstance(em_tag, Tag):
                    strong_tag = em_tag.find("strong")
                    if strong_tag and isinstance(strong_tag, Tag):
                        summary = strong_tag.get_text(strip=True)
                pdf_link = ""
                pdf_link_tag = row.find("a", {"title": "Download"})
                if pdf_link_tag and isinstance(pdf_link_tag, Tag):
                    pdf_href = pdf_link_tag.get("href", "")
                    if isinstance(pdf_href, str):
                        if not pdf_href.startswith("http"):
                            pdf_link = f"{self.base_url}{pdf_href}"
                        else:
                            pdf_link = pdf_href
                doc = {
                    "title": title,
                    "summary": summary,
                    "date": date_text,
                    "situation": VALID_SITUATIONS[0],
                    "pdf_link": pdf_link,
                }
                docs.append(doc)
            except Exception as e:
                logger.error(f"Error extracting document from box: {e}")
                continue
        return docs

    @staticmethod
    def _extract_total_count(soup: BeautifulSoup) -> int | None:
        """Extract the total result count from the search results banner."""
        for div in soup.find_all("div"):
            text = div.get_text(" ", strip=True)
            if "Registros encontrados" not in text:
                continue
            strong_tag = div.find("strong")
            if not strong_tag or not isinstance(strong_tag, Tag):
                continue
            digits = re.sub("\\D", "", strong_tag.get_text(strip=True))
            if digits:
                return int(digits)
        return None

    @staticmethod
    def _has_table_artifacts(text_markdown: str) -> bool:
        """Detect html-to-markdown outputs that are mostly broken pipe tables."""
        if not text_markdown:
            return False
        pipe_count = text_markdown.count("|")
        pipe_lines = sum(
            (1 for line in text_markdown.splitlines() if line.lstrip().startswith("|"))
        )
        return pipe_count >= 80 or pipe_lines >= 8

    @staticmethod
    def _normalize_table_markdown(text_markdown: str) -> str:
        """Collapse broken markdown tables into plain text lines."""
        cleaned_lines = []
        for raw_line in text_markdown.splitlines():
            line = raw_line.strip()
            if not line:
                cleaned_lines.append("")
                continue
            if re.fullmatch("\\|?(?:\\s*:?-{3,}:?\\s*\\|)+\\s*:?-{3,}:?\\s*\\|?", line):
                continue
            if "|" in line:
                cells = [cell.strip() for cell in line.strip("|").split("|")]
                cells = [cell for cell in cells if cell and set(cell) != {"-"}]
                if cells:
                    cleaned_lines.append(" ".join(cells))
                continue
            cleaned_lines.append(raw_line)
        normalized = "\n".join(cleaned_lines)
        normalized = re.sub("\\n{3,}", "\n\n", normalized)
        return normalized.strip()

    async def _get_total_pages(self, norm_type_id: str, year: int) -> int:
        """Get total number of pages for a search"""
        payload = self._format_search_payload(norm_type_id, year, 1)
        response = await self.http.request(self.search_url, method="POST", data=payload)
        client_response = response
        html_content = client_response.content
        soup = BeautifulSoup(html_content, "lxml")
        total_count = self._extract_total_count(soup)
        first_page_docs = self._extract_docs_from_soup(html_content)
        first_page_size = len(first_page_docs)
        if total_count and first_page_size:
            return max(1, calc_pages(total_count, first_page_size))
        nav = soup.find("nav", {"aria-label": "Grupo paginação"})
        if not nav or not isinstance(nav, Tag):
            return 1
        pagination_links = nav.find_all("a", class_="page-link")
        max_page = 1
        for link in pagination_links:
            if not isinstance(link, Tag):
                continue
            text = link.get_text(strip=True)
            if text.isdigit():
                max_page = max(max_page, int(text))
        return max_page

    async def fetch(self, doc_info: dict) -> Document | None:
        """Get document data by downloading PDF and converting to markdown"""
        pdf_link = doc_info.get("pdf_link")
        title = doc_info.get("title", "")
        if not pdf_link:
            raise ValueError("Missing PDF link")
        try:
            pdf_response = await self.http.request(pdf_link)
            pdf_client_response = pdf_response
            pdf_content = pdf_client_response.content
            text_markdown = await bytes_to_markdown(
                pdf_content, "", self.base_url, self.http, self.ocr
            )
            if text_markdown and self._has_table_artifacts(text_markdown):
                text_markdown = self._normalize_table_markdown(text_markdown)
            valid, reason = valid_markdown(text_markdown)
            if not valid:
                raise ValueError(f"Invalid markdown: {reason}")
            doc_info.pop("pdf_link", None)
            doc_info.update(
                {
                    "text_markdown": text_markdown,
                    "document_url": pdf_link,
                    "raw": pdf_content,
                    "raw_ext": ".pdf",
                }
            )
            return Document(**doc_info)
        except Exception as e:
            logger.error(f"Error processing document {title}: {e}")
            raise ValueError(f"Exception processing document: {e}")

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        if year == self.years.start:
            url = f"{self.base_url}/arquivos/documento_68367.PDF#dados"
            yield {
                "year": 1989,
                "type": "Constituição Estadual",
                "title": "Constituição Estadual de Tocantins",
                "document_url": url,
                "pdf_link": url,
                "situation": DEFAULT_VALID_SITUATION,
            }
        for norm_type, norm_type_id in self.types.items():
            pages = await self._get_total_pages(norm_type_id, year)

            async def fetch_page(page: int) -> list[dict]:
                return await self._get_docs_links_page(norm_type_id, year, page)

            async for ref in self.paginate(fetch_page, pages):
                yield {
                    "year": year,
                    "type": norm_type,
                    "situation": DEFAULT_VALID_SITUATION,
                    **ref,
                    "document_url": ref["pdf_link"],
                }
