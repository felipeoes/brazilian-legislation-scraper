import re
from collections.abc import AsyncIterator
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup
from loguru import logger

from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

TYPES = {
    "Lei Ordinária": "lei ord",
    "Lei Complementar": "lei comp",
    "Emenda Constitucional": "emenda",
    "Constituição Estadual": "constituição",
    "Resolução": "resolucao",
}
VALID_SITUATIONS = ["Não consta"]
INVALID_SITUATIONS = []
SITUATIONS = {s: s for s in VALID_SITUATIONS + INVALID_SITUATIONS}
_TYPE_PATTERNS = [
    ("^lei\\s*ord(?:in[aá]ria)?\\b", "Lei Ordinária"),
    ("^lei\\s*comp(?:lementar)?\\b", "Lei Complementar"),
    ("^emenda\\b", "Emenda Constitucional"),
    ("^constitui(?:c|ç)(?:a|ã)o\\b", "Constituição Estadual"),
    ("^resolu(?:c|ç)(?:a|ã)o\\b", "Resolução"),
]


class RNAlrnScraper(BaseScraper):
    """Webscraper for Rio Grande do Norte state legislation website (https://www.al.rn.leg.br/legislacao/pesquisa)

    Year start (earliest on source): 1971

    Example search request: https://www.al.rn.leg.br/legislacao/pesquisa?tipo=ano&nome=2023&page=1

    payload = {
        "tipo": "ano",
        "nome": 2023,
        "page": 1,
    }
    """

    types = TYPES
    situations = SITUATIONS
    name = "RIO_GRANDE_DO_NORTE"
    base_url = "https://www.al.rn.leg.br"

    def _format_search_url(self, year: int, page: int) -> str:
        """Build year-based search URL."""
        params = {"tipo": "ano", "nome": year, "page": page}
        return f"{self.base_url}/legislacao/pesquisa?{urlencode(params)}"

    @staticmethod
    def _infer_norm_type(title: str) -> str:
        """Infer norm type from document title prefix."""
        normalized = re.sub("(?<=[A-Za-zÀ-ÿ])(?=\\d)", " ", title)
        normalized = re.sub("[^\\wÀ-ÿ]+", " ", normalized, flags=re.UNICODE)
        normalized = re.sub("\\s+", " ", normalized).strip().casefold()
        for pattern, canonical in _TYPE_PATTERNS:
            if re.match(pattern, normalized, re.IGNORECASE):
                return canonical
        match = re.match("([A-Za-zÀ-ÿ\\s]+?)\\s+\\d", normalized, re.IGNORECASE)
        if match:
            return match.group(1).strip().title()
        return "Outro"

    async def _get_docs_links(
        self, url: str, soup: BeautifulSoup | None = None
    ) -> list:
        """Get document links from a listing page.

        Returns a list of dicts with keys 'title', 'year', 'summary', 'pdf_link'.
        """
        if soup is None:
            response = await self.http.request(url)
            soup = BeautifulSoup(response.content, "lxml")
        docs = []
        table = soup.find("table", class_="table table-sm table-striped")
        if not table:
            logger.warning(f"No table found for url: {url}")
            return []
        items = table.find_all("tr")
        if not items:
            logger.warning(f"Empty table for url: {url}")
        for item in items:
            tds = item.find_all("td")
            if len(tds) == 0:
                continue
            th = item.find("th")
            if not th:
                continue
            title = th.text.strip()
            year = int(tds[0].text.strip())
            pdf_link_tag = tds[1].find("a")
            if not pdf_link_tag:
                continue
            pdf_link = pdf_link_tag["href"]
            docs.append(
                {"year": year, "title": title, "summary": "", "pdf_link": pdf_link}
            )
        return docs

    async def fetch(self, doc_info: dict) -> Document | None:
        """Get document data from given document dict."""
        return await self.fetch_pdf(doc_info)

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        url = self._format_search_url(year, 1)
        soup = (await self.http.request(url)).soup()
        pagination = soup.find("ul", class_="pagination")
        pages = (
            int(pagination.find_all("li")[-2].find("a").text.strip())
            if pagination
            else 1
        )

        async def fetch_page(page: int) -> list[dict]:
            return (
                await self._get_docs_links(url, soup=soup)
                if page == 1
                else await self._get_docs_links(self._format_search_url(year, page))
            )

        async for ref in self.paginate(fetch_page, pages):
            yield {
                "year": year,
                "situation": DEFAULT_VALID_SITUATION,
                **ref,
                "type": self._infer_norm_type(ref["title"]),
                "document_url": urljoin(self.base_url, ref["pdf_link"]),
            }
