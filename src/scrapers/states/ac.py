import re
from collections.abc import AsyncIterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.core.convert import clean_norm_soup
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

TYPES = {
    "Lei Ordinária": "lei_ordinarias",
    "Lei Complementar": "lei_complementares",
    "Constituição Estadual": "detalhar_constituicao",
    "Decreto": "lei_decretos",
    "Emenda Constitucional": "emendas",
}
SITUATIONS = {"Não consta revogação expressa": "Não consta revogação expressa"}


class AcreLegisScraper(BaseScraper):
    """Webscraper for Legis - Acre website (https://legis.ac.gov.br)

    Year start (earliest on source): 1963

    Example search request: https://legis.ac.gov.br/principal/1
    """

    name = "ACRE"
    yearless = True
    year_regex = re.compile("\\d{4}")
    types = TYPES
    situations = SITUATIONS
    base_url = "https://legis.ac.gov.br/principal"
    _mhtml_wait_until = "domcontentloaded"

    def _format_search_url(self, norm_type_id: str) -> str:
        """Format url for search request"""
        return f"{self.base_url}/{norm_type_id}"

    async def _get_docs_links(self, soup: BeautifulSoup, norm_type_id: str) -> list:
        """Get documents html links from soup object.
        Returns a list of dicts with keys 'title', 'year', 'summary' and 'html_link'
        """
        trs = (
            soup.find("div", id=norm_type_id)
            .find("table")
            .find_all("tr", {"class": "visaoQuadrosTr"})
        )
        html_links = []
        for tr in trs:
            a = tr.find("a")
            title = a.text.strip()
            html_link = a["href"]
            summary = tr.find("td").find_next("td").text.strip()
            m = self.year_regex.search(title.split(",")[-1])
            year = m.group() if m else "0000"
            html_links.append(
                {
                    "title": title,
                    "year": year,
                    "summary": summary,
                    "html_link": html_link,
                }
            )
        return html_links

    def _clean_acre_html(self, soup: "BeautifulSoup") -> str:
        """Extract and clean HTML content from an Acre legislation page."""
        html_tag = soup.find("div", id="body-law") or soup.find("div", id="exportacao")
        if not html_tag:
            return ""
        for row in html_tag.find_all("div", class_="row"):
            row.decompose()
        doe_span = html_tag.find("span", id="texto_publicado_doe")
        if doe_span:
            doe_span.decompose()
        topo = html_tag.find("div", class_="topo-lei")
        if topo:
            topo.decompose()
        first_table = html_tag.find("table")
        if first_table:
            first_td = first_table.find("td")
            if first_td and (not first_td.get_text(strip=True)):
                first_table.decompose()
        clean_norm_soup(html_tag, unwrap_links=True, remove_disclaimers=True)
        return str(html_tag)

    async def fetch(self, doc_info: dict) -> Document | None:
        """Get document data from given html link"""
        doc_html_link = doc_info["html_link"]
        try:
            soup, mhtml = await self.browser.snapshot(
                doc_html_link, wait_until="domcontentloaded"
            )
        except Exception as exc:
            raise ValueError(f"Failed to fetch document page: {exc}")
        html_string = self._clean_acre_html(soup)
        doc_data = {k: v for k, v in doc_info.items() if k != "html_link"}
        return await self.fetch_html(doc_data, html_string, doc_html_link, mhtml)

    async def list_documents(self, year: int | None) -> AsyncIterator[dict]:
        soup = (await self.http.request(self._format_search_url(1))).soup()
        for norm_type, norm_type_id in self.types.items():
            if norm_type == "Constituição Estadual":
                url = "https://legis.ac.gov.br/detalhar_constituicao"
                yield {
                    "year": 1989,
                    "type": norm_type,
                    "title": norm_type,
                    "document_url": url,
                    "html_link": url,
                    "situation": DEFAULT_VALID_SITUATION,
                }
                continue
            for ref in await self._get_docs_links(soup, norm_type_id):
                yield {
                    **ref,
                    "year": int(ref["year"]),
                    "type": norm_type,
                    "situation": DEFAULT_VALID_SITUATION,
                    "document_url": urljoin(self.base_url, ref["html_link"]),
                }
