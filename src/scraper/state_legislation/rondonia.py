from datetime import datetime
from io import BytesIO
from typing import Any, Optional
from urllib.parse import urljoin

from bs4 import Tag
from loguru import logger
from src.scraper.base.scraper import BaseScraper, STATE_LEGISLATION_SAVE_DIR


TYPES = {
    "Decreto-Lei": "declei",
    "Lei Complementar": "leicomp",
    "Lei Ordinária": "leiord",
    "Decreto Numerado": "decnum",
}

# Cannot determine revocation status from the website, so the situation is hardcoded as "Não consta"
SITUATIONS = []


class RondoniaCotelScraper(BaseScraper):
    """Webscraper for Rondônia state legislation website (http://ditel.casacivil.ro.gov.br/)

    Example search request: http://ditel.casacivil.ro.gov.br/COTEL/Livros/listdeclei.aspx?ano=2025
    """

    def __init__(
        self,
        base_url: str = "http://ditel.casacivil.ro.gov.br/COTEL",
        **kwargs: Any,
    ):
        if STATE_LEGISLATION_SAVE_DIR:
            kwargs.setdefault("docs_save_dir", STATE_LEGISLATION_SAVE_DIR)
        super().__init__(
            base_url, types=TYPES, situations=SITUATIONS, name="RONDONIA", **kwargs
        )
        self._constitution_fetched = False

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

    async def _process_pdf(
        self, pdf_link: str, pdf_len_threshold: int = 99
    ) -> Optional[dict]:
        """Process PDF and return text markdown."""
        response = await self.request_service.make_request(pdf_link)
        if not response:
            return None

        content = await response.read()
        if not content:
            return None

        text_markdown = await self._get_markdown(response=response)

        if text_markdown:
            text_markdown = text_markdown.strip()
            if len(text_markdown) > pdf_len_threshold:
                return {
                    "text_markdown": text_markdown,
                    "document_url": pdf_link,
                }

        text_markdown = await self._get_markdown(stream=BytesIO(content))
        text_markdown = text_markdown.strip() if text_markdown else ""
        if not text_markdown or not len(text_markdown) > pdf_len_threshold:
            return None

        return {
            "text_markdown": text_markdown,
            "document_url": pdf_link,
        }

    async def _get_doc_data(self, doc_info: dict) -> Optional[dict]:
        """Get document data"""
        pdf_link = doc_info.pop("pdf_link")
        processed_pdf = await self._process_pdf(pdf_link)

        if processed_pdf is None:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=pdf_link,
                error_message="Failed to process PDF",
            )
            return None

        doc_info.update(processed_pdf)
        return doc_info

    async def _fetch_constitution(self):
        """Fetch the state constitution if available."""
        pdf_url = f"{self.base_url}/Livros/CE1989-2014.pdf"

        text_markdown = await self._get_markdown(url=pdf_url)

        doc_info = {
            "year": datetime.now().year,
            "type": "Constituição Estadual",
            "title": "Constituição do Estado de Rondônia",
            "norm_number": "CE1989-2014",
            "situation": "Não consta revogação expressa",
            "summary": "Constituição do Estado de Rondônia",
            "text_markdown": text_markdown,
            "document_url": pdf_url,
        }

        await self.saver.save([doc_info])
        self.results.append(doc_info)
        if self.verbose:
            logger.info("Scraped state constitution")

    async def _scrape_type(
        self, norm_type: str, norm_type_id: str, year: int
    ) -> list[dict]:
        """Scrape norms for a specific type and year"""
        url = self._format_search_url(norm_type_id, year)

        try:
            documents = await self._get_docs_links(url)

            if not documents:
                return []

            # Process documents with asyncio
            doc_data_tasks = [self._get_doc_data(doc_info) for doc_info in documents]
            valid_results = await self._gather_results(
                doc_data_tasks,
                context={"year": year, "type": norm_type, "situation": "N/A"},
                desc=f"RONDONIA | {norm_type}",
            )
            results = []
            for result in valid_results:
                if result:
                    queue_item = {"year": year, "type": norm_type, **result}
                    results.append(queue_item)

            if self.verbose:
                logger.info(
                    f"Finished scraping for Year: {year} | Type: {norm_type} | Results: {len(results)}"
                )

            return results

        except Exception as e:
            logger.error(
                f"Error scraping Year: {year} | Type: {norm_type} | Error: {e}"
            )
            return []

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape norms for a specific year, fetching constitution on first call."""
        if not self._constitution_fetched:
            await self._fetch_constitution()
            self._constitution_fetched = True
        return await super()._scrape_year(year)
