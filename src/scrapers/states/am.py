import re
from collections.abc import AsyncIterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.core.convert import clean_norm_soup
from src.core.document import Document
from src.core.http import HttpError
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

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
    m = re.search("(\\d{1,2})\\s+de\\s+(\\w+)\\s+de\\s+(\\d{4})", text, re.IGNORECASE)
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
    "Constituição Estadual": "12/1989/10/746",
}
SITUATIONS = {"Não consta": "Não consta"}


class LegislaAMScraper(BaseScraper):
    """Webscraper for Amazonas state legislation website (https://legisla.imprensaoficial.am.gov.br/)

    Year start (earliest on source): 1956

    Example search request: https://legisla.imprensaoficial.am.gov.br/diario_am/41535/2022?page=1
    """

    name = "AMAZONAS"
    types = TYPES
    situations = {}
    base_url = "https://legisla.imprensaoficial.am.gov.br"

    def _format_search_url(self, norm_type_id: str, year: int, page: int) -> str:
        """Format url for search request"""
        return f"{self.base_url}/diario_am/{norm_type_id}/{year}?page={page}"

    async def _get_docs_links(self, url: str) -> tuple[list, bool]:
        """Get documents html links from given page.
        Returns a tuple of (docs, reached_end) where docs is a list of dicts
        with keys 'title', 'summary', '_html_link' and reached_end indicates
        there are no more pages."""
        soup = (await self.http.request(url)).soup()
        if not soup:
            return ([], True)
        container = soup.find("div", id="container")
        if container:
            error = container.find("h1")
            if error and error.text == "Error":
                raise ValueError("Amazonas listing returned an error page")
        items = soup.find_all("li", class_="item-li")
        if not items:
            return ([], True)
        docs = []
        for item in items:
            title = item.find("h5").text
            html_link = item.find("a")
            if not html_link:
                continue
            html_link = html_link.get("href")
            date_tag = item.find("p")
            date_str = _parse_pt_date(date_tag.text) if date_tag else None
            docs.append(
                {
                    "title": title,
                    "summary": "",
                    "_html_link": html_link,
                    "date": date_str,
                }
            )
        return (docs, False)

    def _get_norm_text(self, soup: BeautifulSoup) -> BeautifulSoup:
        """Get norm text from given document soup"""
        norm_element = soup.find("div", class_="materia rounded")
        if norm_element is None:
            return None
        norm_text = norm_element.text
        if len(norm_text) < 70:
            return None
        clean_norm_soup(norm_element)
        empty_soup = BeautifulSoup("<html><head></head><body></body></html>", "lxml")
        empty_soup.body.append(norm_element)
        return empty_soup

    async def fetch(self, doc_info: dict) -> Document | None:
        """Get document data from given document dict"""
        doc_info = dict(doc_info)
        html_link = doc_info.pop("_html_link")
        url = urljoin(self.base_url, html_link)
        response = await self.http.request(url)
        html_content = self._get_norm_text(response.soup())
        if html_content is None:
            raise ValueError("No norm text found in page")
        return await self.fetch_html(
            doc_info, str(html_content), url, response.content, raw_ext=".html"
        )

    async def list_type(
        self, year: int, norm_type: str, norm_type_id: str
    ) -> AsyncIterator[dict]:
        if norm_type == "Constituição Estadual":
            if year == self.years.start:
                url = f"{self.base_url}/diario_am/{norm_type_id}"
                yield {
                    "year": 1989,
                    "type": norm_type,
                    "title": norm_type,
                    "document_url": url,
                    "_html_link": url,
                    "situation": DEFAULT_VALID_SITUATION,
                }
            return
        page = 1
        while True:
            try:
                refs, reached_end = await self._get_docs_links(
                    self._format_search_url(norm_type_id, year, page)
                )
            except HttpError as exc:
                # The site answers 404 for the page after the last one.
                if exc.status == 404 and page > 1:
                    break
                raise
            for ref in refs:
                yield {
                    **ref,
                    "year": year,
                    "type": norm_type,
                    "situation": DEFAULT_VALID_SITUATION,
                    "document_url": urljoin(self.base_url, ref["_html_link"]),
                }
            if reached_end:
                break
            page += 1
