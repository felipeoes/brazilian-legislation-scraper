import re
from collections.abc import AsyncIterator
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit

from bs4 import BeautifulSoup

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
    ("^constitui(?:c|ç)(?:a|ã)o estadual\\b", "Constituição Estadual"),
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

    @classmethod
    def _is_state_norm(cls, title: str) -> bool:
        """State acts are numbered; federal acts and reference books are not."""
        if cls._infer_norm_type(title) == "Constituição Estadual":
            return True
        return "federal" not in title.casefold() and bool(
            re.search("\\bn[º°o]\\.?\\s*\\d", title, re.IGNORECASE)
        )

    @staticmethod
    def _total_pages(soup: BeautifulSoup) -> int:
        """Highest ``page=`` in the Laravel pagination nav; no nav means one page."""
        nav = soup.find("nav", attrs={"aria-label": "Pagination Navigation"})
        if nav is None:
            return 1
        return max(
            int(page)
            for link in nav.find_all("a", href=True)
            for page in parse_qs(urlsplit(str(link["href"])).query).get("page", [])
        )

    async def _get_docs_links(
        self, url: str, soup: BeautifulSoup | None = None
    ) -> list:
        """Get document links from a listing page.

        Returns a list of dicts with keys 'title', 'year', 'summary', '_pdf_link'.
        """
        if soup is None:
            response = await self.http.request(url)
            soup = BeautifulSoup(response.content, "lxml")
        docs = []
        table = soup.find("table", class_="table table-sm table-striped")
        if not table:
            raise ValueError(f"No results table at {url}")
        for item in table.find_all("tr"):
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
                {"year": year, "title": title, "summary": "", "_pdf_link": pdf_link}
            )
        return docs

    async def fetch(self, doc_info: dict) -> Document | None:
        """Get document data from given document dict."""
        # Norms without a file link to the bare folder, which answers 403 on every retry.
        if doc_info["_pdf_link"].rstrip("/").endswith("/storage/legislacao"):
            raise ValueError("RN lists this norm without a file")
        return await self.fetch_pdf(doc_info)

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        url = self._format_search_url(year, 1)
        soup = (await self.http.request(url)).soup()
        pages = self._total_pages(soup)

        async def fetch_page(page: int) -> list[dict]:
            return (
                await self._get_docs_links(url, soup=soup)
                if page == 1
                else await self._get_docs_links(self._format_search_url(year, page))
            )

        async for ref in self.paginate(fetch_page, pages):
            if not self._is_state_norm(ref["title"]):
                continue
            yield {
                "year": year,
                "situation": DEFAULT_VALID_SITUATION,
                **ref,
                "type": self._infer_norm_type(ref["title"]),
                "document_url": urljoin(self.base_url, ref["_pdf_link"]),
            }
