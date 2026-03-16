from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

from bs4 import Tag
from src.scraper.base.scraper import StateScraper


TYPES = {
    "Decreto-Lei": "declei",
    "Lei Complementar": "leicomp",
    "Lei Ordinária": "leiord",
    "Decreto Numerado": "decnum",
}

# Cannot determine revocation status from the website, so the situation is hardcoded as "Não consta"
SITUATIONS = []


class RondoniaCotelScraper(StateScraper):
    """Webscraper for Rondônia state legislation website (http://ditel.casacivil.ro.gov.br/)

    Year start (earliest on source): 1981

    Example search request: http://ditel.casacivil.ro.gov.br/COTEL/Livros/listdeclei.aspx?ano=2025
    """

    def __init__(
        self,
        base_url: str = "http://ditel.casacivil.ro.gov.br/COTEL",
        **kwargs: Any,
    ):
        super().__init__(
            base_url, types=TYPES, situations=SITUATIONS, name="RONDONIA", **kwargs
        )

    def _format_search_url(self, norm_type_id: str, year: int) -> str:
        """Format url for search request"""
        return f"{self.base_url}/Livros/list{norm_type_id}.aspx?ano={year}"

    async def _get_docs_links(self, url: str) -> list:
        """Get document links from search request."""
        soup = await self.request_service.get_soup(url)
        docs = []

        if not soup:
            return []

        # Find the main table with id="ContentPlaceHolder1_DataList1"
        table = soup.find("table", {"id": "ContentPlaceHolder1_DataList1"})
        if not table or not isinstance(table, Tag):
            return []

        tbody = table.find("tbody")
        if tbody and isinstance(tbody, Tag):
            rows = tbody.find_all("tr")
        else:
            rows = table.find_all("tr")

        for row in rows:
            if not isinstance(row, Tag):
                continue

            cell = row.find("td")
            if not cell or not isinstance(cell, Tag):
                continue

            div = cell.find("div")
            if not div or not isinstance(div, Tag):
                continue

            # Extract title and norm number from the main link
            title_links = div.find_all("a")
            title_link = None
            pdf_link = None

            for link in title_links:
                if not isinstance(link, Tag):
                    continue

                href = link.get("href")
                if href and isinstance(href, str):
                    if "detalhes.aspx" in href:
                        title_link = link
                    elif href.endswith(".pdf"):
                        pdf_link = link

            if not title_link or not pdf_link:
                continue

            title = title_link.get_text(strip=True)

            pdf_href = pdf_link.get("href")
            if pdf_href and isinstance(pdf_href, str):
                if not pdf_href.startswith("http"):
                    pdf_href = urljoin(self.base_url, pdf_href)
            else:
                continue

            # Extract summary (ementa)
            summary_spans = div.find_all("span")
            summary = ""
            doc_id = ""

            for span in summary_spans:
                if not isinstance(span, Tag):
                    continue

                span_id = span.get("id")
                if span_id and isinstance(span_id, str):
                    if "ementadoc" in span_id:
                        summary = span.get_text(strip=True)
                    elif "coddocLabel" in span_id:
                        doc_id = span.get_text(strip=True)

            # remove invalid docs (summary equals NÃO UTILIZADO)
            if summary.lower() == "não utilizado":
                continue

            # get last part of pdf (filename) to join
            pdf_href = pdf_href.split("/")[-1]
            doc = {
                "id": doc_id,
                "title": title,
                "situation": "Não consta",  # we cannot determine revocation status
                "summary": summary,
                "pdf_link": f"{self.base_url}/Livros/Files/{pdf_href}",
            }
            docs.append(doc)

        return docs

    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        """Get document data."""
        return await self._process_pdf_doc(doc_info)

    async def _fetch_constitution(self):
        """Fetch the state constitution if available."""
        await self._fetch_and_save_constitution(
            url=f"{self.base_url}/Livros/CE1989-2014.pdf",
            title="Constituição do Estado de Rondônia",
            year=datetime.now().year,
            norm_number="CE1989-2014",
            summary="Constituição do Estado de Rondônia",
        )

    async def _scrape_type(
        self, norm_type: str, norm_type_id: str, year: int
    ) -> list[dict]:
        """Scrape norms for a specific type and year"""
        url = self._format_search_url(norm_type_id, year)
        documents = await self._get_docs_links(url)

        if not documents:
            return []

        for doc in documents:
            doc["year"] = year
        return await self._process_documents(
            documents,
            year=year,
            norm_type=norm_type,
        )

    async def _before_scrape(self) -> None:
        await self._fetch_constitution()
