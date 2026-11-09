import re
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup, Tag

from src.core.convert import calc_pages
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

TYPES = {
    "Lei Ordinária": 1,
    "Lei Complementar": 2,
    "Emenda Constitucional": 4,
    "Resolução": 9,
    "Decreto Legislativo": 10,
}
_PAGE_SIZE = 20
_TOTAL_RE = re.compile(r"(\d+)\s+itens encontrados")
_DATE_RE = re.compile(r"\d{2}[/.]\d{2}[/.](\d{4}|\d{2})$")


def _year(text: str) -> int | None:
    """Year of a trailing dd/mm/yy(yy) date; malformed source dates yield None."""
    if match := _DATE_RE.search(text):
        year = int(match[1])
        return year if year > 100 else year + (1900 if year > 50 else 2000)
    return None


class AmapaAlapScraper(BaseScraper):
    """Webscraper for Amapa Legislative Assembly norms on eLegis (https://elegis.al.ap.leg.br/portal/legislacao).

    Year start (earliest on source): 1991

    eLegis filters ``ano`` by the year of the originating bill, so listing year Y yields
    the norms of Y's bills, each filed under its own publication year (often Y+1..Y+4).

    Example search request: https://elegis.al.ap.leg.br/portal/legislacao?ano=2012&tipo_proposicao_id=1&page=2
    """

    name = "AMAPA"
    types = TYPES
    base_url = "https://elegis.al.ap.leg.br"

    def _listing_url(self, year: int, type_id: int, page: int) -> str:
        query = urlencode({"ano": year, "tipo_proposicao_id": type_id, "page": page})
        return f"{self.base_url}/portal/legislacao?{query}"

    @staticmethod
    def _parse_row(row: Tag, year: int, name: str) -> dict:
        tds = row.find_all("td", recursive=False)
        if len(tds) != 6 or tds[4].a is None:
            raise ValueError(
                f"Unexpected eLegis listing row: {row.get_text(' ', strip=True)}"
            )
        title, summary, doe, published, proposition = (
            " ".join(td.get_text(" ", strip=True).split()) for td in tds[:5]
        )
        texts = {
            a.get_text(strip=True): a["href"] for a in tds[5].find_all("a", href=True)
        }
        # The consolidated text is the enacted law; the full text is often the bill.
        text = texts.get("Texto Consolidado") or texts.get("Texto Integral")
        return {
            "year": _year(published) or _year(title) or year,
            "type": name,
            "situation": DEFAULT_VALID_SITUATION,  # eLegis publishes no situation
            "title": title,
            "summary": summary,
            "doe_number": doe,
            "publication_date": published,
            "proposition": proposition,
            "document_url": tds[4].a["href"],
            "_html_link": f"{text}/imprimir" if text else None,
        }

    def _parse_listing(
        self, soup: BeautifulSoup, year: int, name: str
    ) -> tuple[int, list[dict]]:
        counter = _TOTAL_RE.search(soup.get_text(" ", strip=True))
        if counter is None:
            raise ValueError("eLegis listing has no result counter")
        rows = soup.select("table.ls-table > tbody > tr")
        if int(counter[1]) and not rows:
            raise ValueError("eLegis listing reports results but has no rows")
        return int(counter[1]), [self._parse_row(row, year, name) for row in rows]

    async def list_type(
        self, year: int | None, name: str, value: Any
    ) -> AsyncIterator[dict]:
        async def fetch_page(page: int) -> list[dict]:
            response = await self.http.request(self._listing_url(year, value, page))
            return self._parse_listing(response.soup(), year, name)[1]

        response = await self.http.request(self._listing_url(year, value, 1))
        total, refs = self._parse_listing(response.soup(), year, name)
        for ref in refs:
            yield ref
        async for ref in self.paginate(
            fetch_page, calc_pages(total, _PAGE_SIZE), start=2
        ):
            yield ref

    async def fetch(self, ref: dict) -> Document:
        if not ref.get("_html_link"):
            raise ValueError(
                "eLegis has no text for this norm, only the scanned bill PDF"
            )
        url = urljoin(self.base_url, ref["_html_link"])
        response = await self.http.request(url)
        soup = response.soup()
        for tag in soup(
            ["script", "style"]
        ):  # the printable page auto-runs window.print()
            tag.decompose()
        return await self.fetch_html(
            ref, str(soup), url.removesuffix("/imprimir"), response.content, ".html"
        )
