from collections.abc import AsyncIterator
from urllib.parse import urljoin

from bs4 import Tag

from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

TYPES = {
    "Decreto-Lei": "declei",
    "Lei Complementar": "leicomp",
    "Lei Ordinária": "leiord",
    "Decreto Numerado": "decnum",
}
SITUATIONS = []


class RondoniaCotelScraper(BaseScraper):
    """Webscraper for Rondônia state legislation website (http://ditel.casacivil.ro.gov.br/)

    Year start (earliest on source): 1981

    Example search request: http://ditel.casacivil.ro.gov.br/COTEL/Livros/listdeclei.aspx?ano=2025
    """

    types = TYPES
    situations = SITUATIONS
    name = "RONDONIA"
    base_url = "http://ditel.casacivil.ro.gov.br/COTEL"

    def _format_search_url(self, norm_type_id: str, year: int) -> str:
        """Format url for search request"""
        return f"{self.base_url}/Livros/list{norm_type_id}.aspx?ano={year}"

    async def _get_docs_links(self, url: str) -> list:
        """Get document links from search request."""
        soup = (await self.http.request(url)).soup()
        docs = []
        if not soup:
            return []
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
            if summary.lower() == "não utilizado":
                continue
            pdf_href = pdf_href.split("/")[-1]
            doc = {
                "id": doc_id,
                "title": title,
                "situation": "Não consta",
                "summary": summary,
                "pdf_link": f"{self.base_url}/Livros/Files/{pdf_href}",
            }
            docs.append(doc)
        return docs

    async def fetch(self, doc_info: dict) -> Document | None:
        """Get document data."""
        return await self.fetch_pdf(doc_info)

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        if year == self.years.start:
            url = f"{self.base_url}/Livros/CE1989-2014.pdf"
            yield {
                "year": 1989,
                "type": "Constituição Estadual",
                "title": "Constituição do Estado de Rondônia",
                "document_url": url,
                "pdf_link": url,
                "situation": DEFAULT_VALID_SITUATION,
            }
        for norm_type, norm_type_id in self.types.items():
            for ref in await self._get_docs_links(
                self._format_search_url(norm_type_id, year)
            ):
                yield {
                    "year": year,
                    "type": norm_type,
                    "situation": DEFAULT_VALID_SITUATION,
                    **ref,
                    "document_url": ref["pdf_link"],
                }
