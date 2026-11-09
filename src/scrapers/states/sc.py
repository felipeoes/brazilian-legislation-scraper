"""Santa Catarina state legislation scraper.

Scrapes the ALESC legislation system at ``leis.alesc.sc.gov.br``.
The site is a NextJS app with server-rendered HTML.

Search: GET ``/legislativo?ano={year}&page={page}`` (legislative acts)
        GET ``/executivo?ano={year}&page={page}`` (executive decrees)
Document: GET ``/ato-normativo/{path}/{id}`` returns full HTML content.
"""

import re
from collections.abc import AsyncIterator
from urllib.parse import urljoin

from src.core.convert import strip_html_chrome
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

_RE_TYPE_FROM_TITLE = re.compile(
    "^(LEI COMPLEMENTAR|LEI|DECRETO LEGISLATIVO|DECRETO-LEI|DECRETO|EMENDA CONSTITUCIONAL|RESOLUÇÃO|PORTARIA|INSTRUÇÃO NORMATIVA)",
    re.IGNORECASE,
)
# Executive titles are "<CODE>-<number> <date>" (INT-000001); names confirmed from the
# pages' TipoNorma meta ("Instrução Normativa - INT"). Never store the code as the type.
_RE_CODE_TITLE = re.compile("^([A-Z]{2,4})-\\d+")
_EXEC_CODES = {
    "DEC": "Decreto",
    "DEL": "Decreto-Lei",
    "INT": "Instrução Normativa",
    "MPV": "Medida Provisória",
    "ONI": "Orientação Normativa",
    "PRT": "Portaria",
    "PRV": "Provimento",
    "RES": "Resolução",
}
TYPES = {"Legislativo": "legislativo", "Executivo": "executivo"}
SITUATIONS: list[str] = []


class SantaCatarinaScraper(BaseScraper):
    """Scraper for Santa Catarina legislation (leis.alesc.sc.gov.br).

    Year start (earliest on source): 1946
    """

    types = TYPES
    situations = SITUATIONS
    name = "SANTA_CATARINA"
    base_url = "https://leis.alesc.sc.gov.br"

    def _format_search_url(self, norm_type_id: str, year: int) -> str:
        return f"{self.base_url}/{norm_type_id}?ano={year}"

    @staticmethod
    def _infer_type(title: str) -> str:
        if code := _RE_CODE_TITLE.match(title):
            if code[1] not in _EXEC_CODES:
                raise ValueError(f"Unknown SC act code {code[1]!r} in {title!r}")
            return _EXEC_CODES[code[1]]
        m = _RE_TYPE_FROM_TITLE.match(title)
        if m:
            return m.group(1).title()
        prefix_match = re.match(
            "(.+?)(?:\\s+(?:n[º°o]|n\\.|número)\\s*\\d|\\s+\\d)", title, re.IGNORECASE
        )
        return prefix_match.group(1).strip() if prefix_match else title

    async def fetch(self, doc_info: dict) -> Document:
        """Fetch the server-rendered document HTML and convert to markdown."""
        if not doc_info.get("constitution"):
            doc_info["type"] = self._infer_type(" ".join(doc_info["title"].split()))
        document_url = doc_info["document_url"]
        response = await self.http.request(document_url)
        soup = response.soup()
        strip_html_chrome(soup)
        doc_info["situation"] = "Não consta"
        return await self.fetch_html(
            doc_info, str(soup), document_url, response.content, ".html"
        )

    async def list_documents(self, year: int | None) -> AsyncIterator[dict]:
        # The constitution is only linked from every page's footer; list it once.
        if year == self.years.start:
            yield {
                "year": 1989,
                "type": "Constituição Estadual",
                "title": "Constituição do Estado de Santa Catarina",
                "document_url": f"{self.base_url}/ato-normativo/legislativo/52785",
                "situation": DEFAULT_VALID_SITUATION,
                "constitution": True,
            }
        async for ref in super().list_documents(year):
            yield ref

    async def list_type(
        self, year: int, norm_type: str, path: str
    ) -> AsyncIterator[dict]:
        seen = set()
        page = 1
        while True:
            soup = (
                await self.http.request(
                    f"{self.base_url}/{path}?ano={year}&page={page}"
                )
            ).soup()
            if "Resultados da Pesquisa" not in soup.get_text():
                raise ValueError(f"Malformed SC listing page {page}")
            links = soup.select('.card-title a[href*="/ato-normativo/"]')
            new_links = [link for link in links if link.get("href") not in seen]
            if not new_links:
                break
            for link in new_links:
                seen.add(link["href"])
                title = link.get_text(strip=True)
                if title:
                    yield {
                        "year": year,
                        "type": norm_type,
                        "title": title,
                        "document_url": urljoin(self.base_url, link["href"]),
                        "situation": DEFAULT_VALID_SITUATION,
                    }
            page += 1
