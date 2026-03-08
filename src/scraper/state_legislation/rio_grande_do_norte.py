from urllib.parse import urlencode

from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.scraper import StateScraper

TYPES = {
    "Lei Ordinária": "lei ord",
    "Lei Complementar": "lei comp",
    "Emenda Constitucional": "emenda",
    "Constituição Estadual": "constituição",
    "Resolução": "resolucao",
}

VALID_SITUATIONS = [
    "Não consta"
]  # ALRN does not have a situation field, so we can not distinguish between valid and invalid norms

INVALID_SITUATIONS = []
SITUATIONS = {s: s for s in VALID_SITUATIONS + INVALID_SITUATIONS}


class RNAlrnScraper(StateScraper):
    """Webscraper for Rio Grande do Norte state legislation website (https://www.al.rn.leg.br/legislacao/pesquisa)

    Year start (earliest on source): 1971

    Example search request: https://www.al.rn.leg.br/legislacao/pesquisa?tipo=ano&nome=2023&page=1

    payload = {
        "tipo": "ano",
        "nome": 2023,
        "page": 1,
    }
    """

    def __init__(
        self,
        base_url: str = "https://www.al.rn.leg.br",
        **kwargs,
    ):
        super().__init__(
            base_url,
            types=TYPES,
            situations=SITUATIONS,
            name="RIO_GRANDE_DO_NORTE",
            **kwargs,
        )

    def _format_search_url(self, year: int, page: int) -> str:
        """Build year-based search URL."""
        params = {
            "tipo": "ano",
            "nome": year,
            "page": page,
        }
        return f"{self.base_url}/legislacao/pesquisa?{urlencode(params)}"

    @staticmethod
    def _infer_norm_type(title: str) -> str:
        """Infer norm type from document title prefix."""
        lower = title.lower()
        if "lei ord" in lower:
            return "Lei Ordinária"
        if "lei comp" in lower:
            return "Lei Complementar"
        if "emenda" in lower:
            return "Emenda Constitucional"
        if "constitu" in lower:
            return "Constituição Estadual"
        if "resolu" in lower:
            return "Resolução"
        return "Outro"

    async def _get_docs_links(
        self, url: str, soup: BeautifulSoup | None = None
    ) -> list:
        """Get document links from a listing page.

        Returns a list of dicts with keys 'title', 'year', 'summary', 'pdf_link'.
        """
        if soup is None:
            response = await self.request_service.make_request(url)
            if not response:
                logger.warning(f"No response for url: {url}")
                return []
            soup = BeautifulSoup(await response.read(), "html.parser")

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
            if len(tds) == 0:  # skip invalid rows, valid documents have at least 1 td
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
                {
                    "year": year,
                    "title": title,
                    "summary": "",  # do not have a field for summary
                    "pdf_link": pdf_link,
                }
            )

        return docs

    async def _get_doc_data(self, doc_info: dict) -> dict | None:
        """Get document data from given document dict."""
        return await self._process_pdf_doc(doc_info)

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape all norms for a year using the year-based search endpoint.

        Fetches page 1 to discover total pages, then fetches all remaining
        pages concurrently. Results are grouped by inferred norm type and
        processed via ``_process_documents``.
        """
        first_url = self._format_search_url(year, 1)
        first_soup = await self.request_service.get_soup(first_url)
        if not first_soup:
            return []

        docs = await self._get_docs_links(first_url, soup=first_soup)

        pagination = first_soup.find("ul", class_="pagination")
        if pagination:
            total_pages = int(pagination.find_all("li")[-2].find("a").text.strip())
        else:
            total_pages = 1

        docs.extend(
            await self._fetch_all_pages(
                lambda p: self._get_docs_links(self._format_search_url(year, p)),
                total_pages,
                context={"year": year, "type": "NA", "situation": "Não consta"},
                desc=f"RIO GRANDE DO NORTE | Year {year} | get_docs_links",
            )
        )

        if not docs:
            return []

        # Group by inferred norm type for proper context in _process_documents
        docs_by_type: dict[str, list[dict]] = {}
        for doc in docs:
            norm_type = self._infer_norm_type(doc["title"])
            docs_by_type.setdefault(norm_type, []).append(doc)

        situation = next(iter(self.situations), "Não consta")
        tasks = [
            self._process_documents(
                type_docs,
                year=year,
                norm_type=norm_type,
                situation=situation,
                desc=f"RIO GRANDE DO NORTE | {norm_type} | Year {year}",
            )
            for norm_type, type_docs in docs_by_type.items()
        ]
        results = await self._gather_results(
            tasks,
            context={"year": year, "type": "NA", "situation": situation},
            desc=f"RIO GRANDE DO NORTE | Year {year}",
        )
        return self._flatten_results(results)
