import re
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup

from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

# Ids of the site's "Tipo de Norma" filter. tipo=12 ("Emendas Constitucionais
# Federais") is left out on purpose: federal legislation has its own scraper.
TYPES = {
    "Decreto Estadual": 2,
    "Decreto Legislativo": 1,
    "Emenda Constitucional": 11,
    "Lei Complementar": 4,
    "Lei Ordinária": 3,
    "Resolução": 10,
}
_EMPTY_MESSAGE = "Nenhuma lei encontrada"
_PAGE_RE = re.compile(r"[?&]p=(\d+)")


class ParaAlepaScraper(BaseScraper):
    """Webscraper for Pará state legislation (https://bancodeleis.alepa.pa.gov.br)

    Year start (earliest on source): 1990

    Listing is the server-rendered search, one query per year and type:
    ``index.php?ano=2012&tipo=2&p=1``. Each page has 10 rows with number, type,
    year, ementa and a PDF link; ``#result-count`` holds the total and
    ``#paginacao`` links the last page. The JSON API (``api/leis.php``) is not
    used because it returns at most 50 rows and has no paging.
    """

    types = TYPES
    situations = {}
    name = "PARA"
    base_url = "https://bancodeleis.alepa.pa.gov.br"

    def _search_url(self, year: int, norm_type_id: int, page: int) -> str:
        query = urlencode({"ano": year, "tipo": norm_type_id, "p": page})
        return f"{self.base_url}/index.php?{query}"

    @staticmethod
    def _total_pages(soup: BeautifulSoup) -> int:
        pages = [
            int(match.group(1))
            for link in soup.select("#paginacao a[href]")
            if (match := _PAGE_RE.search(str(link["href"])))
        ]
        if pages:
            return max(pages)
        count = soup.find(id="result-count")
        rows = len(soup.select("#tbody-leis > tr"))
        if count and int(count.get_text(strip=True).replace(".", "")) > rows:
            raise ValueError("Listing has more than one page of results but no pager")
        return 1

    def _parse_rows(self, soup: BeautifulSoup) -> list[dict]:
        """Parse a listing page; raise unless it has rows or says it has none."""
        count = soup.find(id="result-count")
        tbody = soup.find(id="tbody-leis")
        if count is None or tbody is None:
            raise ValueError("Listing page has no result count or results table")
        if int(count.get_text(strip=True).replace(".", "")) == 0:
            if _EMPTY_MESSAGE not in tbody.get_text():
                raise ValueError("Listing reports 0 results without the empty message")
            return []
        docs = []
        for row in tbody.find_all("tr", recursive=False):
            number = row.select_one("td.td-num")
            year = row.select_one("td.td-num small")
            ementa = row.select_one("td.td-ementa")
            pdf = row.select_one("a.btn-pdf-t[href]")
            if not (number and year and ementa and pdf):
                raise ValueError(
                    f"Malformed listing row: {row.get_text(' ', strip=True)}"
                )
            docs.append(
                {
                    "norm_number": number.find(string=True, recursive=False)
                    .replace("Nº", "")
                    .strip(),
                    "year": int(year.get_text(strip=True)),
                    "summary": ementa.get_text().strip(),
                    "document_url": urljoin(self.base_url, str(pdf["href"])),
                }
            )
        if not docs:
            raise ValueError("Listing reports results but has no rows")
        return docs

    async def list_type(self, year: int, name: str, value: Any) -> AsyncIterator[dict]:
        first = (await self.http.request(self._search_url(year, value, 1))).soup()
        rows = self._parse_rows(first)

        async def fetch_page(page: int) -> list[dict]:
            if page == 1:
                return rows
            url = self._search_url(year, value, page)
            return self._parse_rows((await self.http.request(url)).soup())

        async for ref in self.paginate(fetch_page, self._total_pages(first)):
            yield {
                **ref,
                "title": f"{name} nº {ref['norm_number']}/{ref['year']}",
                "type": name,
                "situation": DEFAULT_VALID_SITUATION,
            }

    async def fetch(self, ref: dict) -> Document:
        return await self.fetch_pdf(ref)
