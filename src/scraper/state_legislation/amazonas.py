from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.scraper import StateScraper

_PT_MONTHS = {
    "janeiro": "01",
    "fevereiro": "02",
    "março": "03",
    "abril": "04",
    "maio": "05",
    "junho": "06",
    "julho": "07",
    "agosto": "08",
    "setembro": "09",
    "outubro": "10",
    "novembro": "11",
    "dezembro": "12",
}


def _parse_pt_date(text: str) -> str | None:
    """Parse 'Publicada em DD de mês de YYYY' → 'YYYY-MM-DD'."""
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", text, re.IGNORECASE)
    if not m:
        return None
    day = m.group(1).zfill(2)
    month = _PT_MONTHS.get(m.group(2).lower())
    year = m.group(3)
    return f"{year}-{month}-{day}" if month else None


TYPES = {
    "Decreto Legislativo": 41535,
    "Decreto": 41536,
    "Emendas Constitucionais": 41533,
    "Lei Complementar": 10,
    "Lei Delegada": 11,
    "Lei Ordinária": 12,
    "Lei Promulgada": 41534,
    "Regimento Interno": 41538,
    "Constituição Estadual": "12/1989/10/746",  # texto completo, modificar a lógica no scraper
}

SITUATIONS = {"Não consta": "Não consta"}


class LegislaAMScraper(StateScraper):
    """Webscraper for Amazonas state legislation website (https://legisla.imprensaoficial.am.gov.br/)

    Year start (earliest on source): 1956

    Example search request: https://legisla.imprensaoficial.am.gov.br/diario_am/41535/2022?page=1
    """

    def __init__(
        self,
        base_url: str = "https://legisla.imprensaoficial.am.gov.br",
        **kwargs,
    ):
        super().__init__(
            base_url, name="AMAZONAS", types=TYPES, situations={}, **kwargs
        )
        self._scraped_constitution = False

    def _format_search_url(self, norm_type_id: str, year: int, page: int) -> str:
        """Format url for search request"""
        return f"{self.base_url}/diario_am/{norm_type_id}/{year}?page={page}"

    async def _get_docs_links(self, url: str) -> tuple[list, bool]:
        """Get documents html links from given page.
        Returns a tuple of (docs, reached_end) where docs is a list of dicts
        with keys 'title', 'summary', 'html_link' and reached_end indicates
        there are no more pages."""
        soup = await self.request_service.get_soup(url)
        if not soup:
            return [], True

        # check if the page is empty (error)
        container = soup.find("div", id="container")
        if container:
            error = container.find("h1")
            if error and error.text == "Error":
                return [], True

        items = soup.find_all("li", class_="item-li")
        if not items:
            return [], True

        docs = []
        for item in items:
            title = item.find("h5").text
            html_link = item.find("a")
            if not html_link:  # some norms do not have a link to text, skip them
                continue
            html_link = html_link.get("href")

            date_tag = item.find("p")
            date_str = _parse_pt_date(date_tag.text) if date_tag else None

            docs.append(
                {
                    "title": title,
                    "summary": "",  # legislaAM does not provide a summary
                    "html_link": html_link,
                    "date": date_str,
                }
            )

        return docs, False

    def _get_norm_text(self, soup: BeautifulSoup) -> BeautifulSoup:
        """Get norm text from given document soup"""
        norm_element = soup.find("div", class_="materia rounded")
        if norm_element is None:
            return None

        norm_text = norm_element.text

        # check if norm_text length is less than 50 characters, if so, it is an invalid norm (doesn't have any text, just a title)
        if len(norm_text) < 70:
            return None

        # Remove the "Este texto não substitui..." disclaimer and other common artifacts.
        self._clean_norm_soup(norm_element)

        # add html tags to the text
        empty_soup = BeautifulSoup(
            "<html><head></head><body></body></html>", "html.parser"
        )
        empty_soup.body.append(norm_element)
        return empty_soup

    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        """Get document data from given document dict"""
        doc_info = dict(doc_info)
        html_link = doc_info.pop("html_link")
        url = urljoin(self.base_url, html_link)

        if self._is_already_scraped(url, doc_info.get("title", "")):
            return None

        try:
            soup, mhtml = await self._fetch_soup_and_mhtml(url)
        except Exception as exc:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=url,
                error_message=f"Failed to fetch page: {exc}",
            )
            return None

        html_content = self._get_norm_text(soup)
        if html_content is None:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=url,
                error_message="No norm text found in page",
            )
            return None

        return await self._process_html_doc(doc_info, str(html_content), url, mhtml)

    async def _scrape_type(self, norm_type: str, norm_type_id, year: int) -> list[dict]:
        """Scrape norms for a specific type and year"""
        situation = "Não consta"
        if not self._scraped_constitution and norm_type == "Constituição Estadual":
            url = f"{self.base_url}/diario_am/{norm_type_id}"
            doc_info = {
                "year": 1989,
                "date": "1989-10-05",
                "situation": situation,
                "type": norm_type,
                "title": "Constituição Estadual",
                "summary": "",
                "html_link": url,
            }

            doc_info = await self._get_doc_data(doc_info)
            if doc_info is None:
                return []

            self._scraped_constitution = True
            logger.info("Scraped state constitution")
            await self._save_doc_result(doc_info)
            return [doc_info]

        ctx = {"year": year, "type": norm_type, "situation": situation}
        documents = await self._paginate_until_end(
            make_task=lambda p: self._get_docs_links(
                self._format_search_url(norm_type_id, year, p)
            ),
            context=ctx,
            desc=f"AMAZONAS | {norm_type} | get_docs_links",
            initial_batch=30,
            batch_growth=10,
        )

        # Get document data
        return await self._process_documents(
            documents,
            year=year,
            norm_type=norm_type,
            situation=situation,
        )
