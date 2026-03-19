from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument
import re
from io import BytesIO
from typing import Any, cast

import aiohttp
from bs4 import BeautifulSoup, Tag
from loguru import logger
from src.scraper.base.converter import calc_pages, valid_markdown
from src.scraper.base.scraper import StateScraper


# Type mappings for Tocantins
TYPES = {
    "Lei Ordinária": "ordinaria",
    "Lei Complementar": "complementar",
}

# For Tocantins, we cannot determine situation
VALID_SITUATIONS = ["Não consta"]

INVALID_SITUATIONS = []

SITUATIONS = {s: s for s in VALID_SITUATIONS + INVALID_SITUATIONS}


class TocantinsScraper(StateScraper):
    """Webscraper for Tocantins state legislation website (https://www.al.to.leg.br/)

    Year start (earliest on source): 1989

    Example search request: POST to https://www.al.to.leg.br/legislacaoEstadual
    """

    def __init__(
        self,
        base_url: str = "https://www.al.to.leg.br",
        **kwargs: Any,
    ):
        super().__init__(
            base_url, types=TYPES, situations=SITUATIONS, name="TOCANTINS", **kwargs
        )
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

        response = await self.request_service.make_request(
            self.search_url, method="POST", payload=payload
        )
        if not response:
            return []

        client_response = cast(aiohttp.ClientResponse, response)
        return self._extract_docs_from_soup(await client_response.read())

    def _extract_docs_from_soup(self, html_content: bytes) -> list[dict]:
        """Extract document information from HTML content"""
        soup = BeautifulSoup(html_content, "html.parser")

        docs = []
        # Find all document boxes
        rows = soup.find_all("div", class_="row")

        for row in rows:
            try:
                # Extract title and link from h4 > a
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

                # Extract date from the small text
                date_text = ""
                small_tags = row.find_all("small")
                for small in small_tags:
                    if isinstance(small, Tag):
                        text = small.get_text(strip=True)
                        if "Data:" in text:
                            date_text = text.replace("Data:", "").strip()
                            # Clean up any extra characters like "|"
                            date_text = date_text.replace("|", "").strip()
                            break

                # Extract summary from em > strong
                summary = ""
                em_tag = row.find("em")
                if em_tag and isinstance(em_tag, Tag):
                    strong_tag = em_tag.find("strong")
                    if strong_tag and isinstance(strong_tag, Tag):
                        summary = strong_tag.get_text(strip=True)

                # Extract PDF download link
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

            digits = re.sub(r"\D", "", strong_tag.get_text(strip=True))
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
            1 for line in text_markdown.splitlines() if line.lstrip().startswith("|")
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

            if re.fullmatch(r"\|?(?:\s*:?-{3,}:?\s*\|)+\s*:?-{3,}:?\s*\|?", line):
                continue

            if "|" in line:
                cells = [cell.strip() for cell in line.strip("|").split("|")]
                cells = [cell for cell in cells if cell and set(cell) != {"-"}]
                if cells:
                    cleaned_lines.append(" ".join(cells))
                continue

            cleaned_lines.append(raw_line)

        normalized = "\n".join(cleaned_lines)
        normalized = re.sub(r"\n{3,}", "\n\n", normalized)
        return normalized.strip()

    async def _get_total_pages(self, norm_type_id: str, year: int) -> int:
        """Get total number of pages for a search"""
        payload = self._format_search_payload(norm_type_id, year, 1)

        response = await self.request_service.make_request(
            self.search_url, method="POST", payload=payload
        )
        if not response:
            return 1

        client_response = cast(aiohttp.ClientResponse, response)
        html_content = await client_response.read()
        soup = BeautifulSoup(html_content, "html.parser")

        total_count = self._extract_total_count(soup)
        first_page_docs = self._extract_docs_from_soup(html_content)
        first_page_size = len(first_page_docs)

        if total_count and first_page_size:
            return max(1, calc_pages(total_count, first_page_size))

        # Look for pagination navigation with "Grupo paginação"
        nav = soup.find("nav", {"aria-label": "Grupo paginação"})
        if not nav or not isinstance(nav, Tag):
            return 1

        # Find pagination links
        pagination_links = nav.find_all("a", class_="page-link")
        max_page = 1

        for link in pagination_links:
            if not isinstance(link, Tag):
                continue
            text = link.get_text(strip=True)
            if text.isdigit():
                max_page = max(max_page, int(text))

        return max_page

    async def _get_docs_links(self, norm_type_id: str, year: int) -> list[dict]:
        """Get all document links for a type and year using async processing"""
        # Get total pages first
        total_pages = await self._get_total_pages(norm_type_id, year)

        if total_pages <= 1:
            # Single page, process directly
            return await self._get_docs_links_page(norm_type_id, year, 1)

        all_docs = []

        # Process all pages concurrently
        tasks = [
            self._get_docs_links_page(norm_type_id, year, page)
            for page in range(1, total_pages + 1)
        ]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": "NA", "situation": "NA"},
            desc="TOCANTINS | get_docs_links",
        )
        for result in valid_results:
            if result:
                all_docs.extend(result)

        return all_docs

    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        """Get document data by downloading PDF and converting to markdown"""
        pdf_link = doc_info.get("pdf_link")
        title = doc_info.get("title", "")
        if not pdf_link:
            await self._save_doc_error(
                title=title,
                year=doc_info.get("year", ""),
                html_link="",
                error_message="Missing PDF link",
            )
            return None

        if self._is_already_scraped(pdf_link, title):
            return None

        try:
            # Download PDF
            pdf_response = await self.request_service.make_request(pdf_link)
            if not pdf_response:
                await self._save_doc_error(
                    title=title,
                    year=doc_info.get("year", ""),
                    html_link=pdf_link,
                    error_message="Failed to download PDF",
                )
                return None

            pdf_client_response = cast(aiohttp.ClientResponse, pdf_response)
            pdf_content = await pdf_client_response.read()

            # Convert PDF to markdown
            text_markdown = await self._get_markdown(stream=BytesIO(pdf_content))

            if text_markdown and self._has_table_artifacts(text_markdown):
                text_markdown = self._normalize_table_markdown(text_markdown)

            valid, reason = valid_markdown(text_markdown)
            if not valid:
                await self._save_doc_error(
                    title=title,
                    year=doc_info.get("year", ""),
                    html_link=pdf_link,
                    error_message=f"Invalid markdown: {reason}",
                )
                return None

            # Remove pdf_link from doc_info and add processed data
            doc_info.pop("pdf_link", None)
            doc_info.update(
                {
                    "text_markdown": text_markdown,
                    "document_url": pdf_link,
                    "_raw_content": pdf_content,
                    "_content_extension": ".pdf",
                }
            )

            return doc_info

        except Exception as e:
            logger.error(f"Error processing document {title}: {e}")
            await self._save_doc_error(
                title=title,
                year=doc_info.get("year", ""),
                html_link=pdf_link,
                error_message=f"Exception processing document: {e}",
            )
            return None

    async def _fetch_constitution(self):
        """Fetch the Tocantins state constitution"""
        await self._fetch_and_save_constitution(
            url=f"{self.base_url}/arquivos/documento_68367.PDF#dados",
            title="Constituição Estadual de Tocantins",
            year=1989,
            summary="Constituição do Estado do Tocantins",
            date="05/10/1989",
        )

    async def _scrape_type(
        self, norm_type: str, norm_type_id: str, year: int
    ) -> list[dict]:
        """Scrape norms for a specific type and year"""
        try:
            # Get all documents for this type and year
            documents = await self._get_docs_links(norm_type_id, year)

            if not documents:
                return []

            for doc in documents:
                doc["year"] = year
            return await self._process_documents(
                [doc.copy() for doc in documents],
                year=year,
                norm_type=norm_type,
            )

        except Exception as e:
            logger.error(
                f"Error scraping Year: {year} | Type: {norm_type} | Error: {e}"
            )
            return []

    async def _before_scrape(self) -> None:
        await self._fetch_constitution()
