from collections import defaultdict
from loguru import logger
from urllib.parse import urlencode, urljoin
from src.scraper.base.scraper import StateScraper

TYPES = {
    "Constituição Estadual": "/Web%5CConstituição%20Estadual",
    "Decreto": "/Decreto",
    "Decreto E": "/DecretoE",
    "Decreto-Lei": "/Decreto-Lei",
    "Deliberação Conselho de Governança": "/Web%5CDeliberacaoConselhoGov",
    "Emenda Constitucional": "/Emenda",
    "Lei Complementar": "/Lei%20Complementar",
    "Lei Estadual": "/Lei%20Estadual",
    "Mensagem Vetada": "/Mensagem%20Veto",
    "Resolução": "/Resolucoes",
    "Resolução Conjunta": "/Web%5CResolução%20Conjunta",
}

VALID_SITUATIONS = [
    "Não consta"
]  # Alems does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no lon

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class MSAlemsScraper(StateScraper):
    """Webscraper for Mato Grosso do Sul state legislation website (https://www.al.ms.gov.br/)

    Example search request: http://aacpdappls.net.ms.gov.br/appls/legislacao/secoge/govato.nsf/Emenda?OpenView&Start=1&Count=30&Expand=1#1

    OBS: Start=1&Count=30&Expand=1#1, for Expand 1 is the index related to the year
    """

    def __init__(
        self,
        base_url: str = "http://aacpdappls.net.ms.gov.br",
        **kwargs,
    ):
        super().__init__(
            base_url,
            types=TYPES,
            situations=SITUATIONS,
            name="MATO_GROSSO_DO_SUL",
            **kwargs,
        )
        self.params = {
            "OpenView": "",
            "Start": 1,
            "Count": 10000,
            "Expand": "",
        }
        # year→expand_index mapping per norm type, built during prefetch
        self._year_index_map: dict[str, dict[int, int]] = defaultdict(dict)

    def _format_search_url(self, norm_type_id: str, year_index: int) -> str:
        """Format url for search request"""
        return f"{self.base_url}/appls/legislacao/secoge/govato.nsf/{norm_type_id}?{urlencode(self.params)}{year_index}"

    async def _get_docs_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'summary', 'html_link'
        """

        soup = await self.request_service.get_soup(url)
        docs = []

        table = soup.find("table", border="0", cellpadding="2", cellspacing="0")

        items = table.find_all("tr", valign="top")
        for index, item in enumerate(items):
            # don't get tr's with colspan="4" since they are links to other years
            if item.find("td", colspan="4"):
                continue

            tds = item.find_all("td")
            if len(tds) < 5:  # skip invalid rows, valid documents have 5 or 6 tds
                continue

            title = tds[2].text.strip()
            summary = tds[3].text.strip()

            html_link = tds[2].find("a", href=True)
            html_link = html_link["href"]

            docs.append({"title": title, "summary": summary, "html_link": html_link})

        return docs

    async def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given doc info"""
        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")
        url = urljoin(self.base_url, html_link)

        if self._is_already_scraped(url, doc_info.get("title", "")):
            return None

        soup = await self.request_service.get_soup(url)

        # norm text will be the first p tag in the document
        norm_text_tag = soup.find("p")
        html_string = norm_text_tag.prettify().strip()

        # since we're getting the p tag, need to add the html and body tags to make it a valid html for markitdown
        html_string = f"<html><body>{html_string}</body></html>"

        # get text markdown
        text_markdown = await self._get_markdown(html_content=html_string)

        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url
        doc_info["_raw_content"] = html_string.encode("utf-8")
        doc_info["_content_extension"] = ".html"

        return doc_info

    async def _get_available_years(self, norm_type_id: str) -> list[int]:
        """Get available years for given norm type and build the year→expand_index map."""
        url = f"{self.base_url}/appls/legislacao/secoge/govato.nsf/{norm_type_id}?OpenView?Start=1&Count=10000"
        soup = await self.request_service.get_soup(url)

        years_desc: list[int] = []
        table = soup.find("table", border="0", cellpadding="2", cellspacing="0")
        items = table.find_all("tr", valign="top")
        for item in items:
            td = item.find("td")
            year = td.text.strip()
            if not year:
                continue
            years_desc.append(int(year))

        # years come in descending order; Expand index is 1-based position
        for idx_0, year in enumerate(years_desc):
            self._year_index_map[norm_type_id][year] = idx_0 + 1

        return years_desc

    async def _prefetch_year_indexes(self) -> None:
        """Fetch year→index mappings for every norm type and build self.years."""
        all_years: set[int] = set()
        for norm_type_id in self.types.values():
            years = await self._get_available_years(norm_type_id)
            all_years.update(years)
            logger.info(
                f"Prefetched {len(years)} years for {norm_type_id} "
                f"(range {min(years)}–{max(years)})"
            )

        self.years = sorted(
            y for y in all_years if self.year_start <= y <= self.year_end
        )

    async def _scrape_type(
        self, norm_type: str, norm_type_id: str, year: int
    ) -> list[dict]:
        """Scrape documents of a single type for a given year."""
        expand_index = self._year_index_map.get(norm_type_id, {}).get(year)
        if expand_index is None:
            return []

        url = self._format_search_url(norm_type_id, expand_index)
        docs = await self._get_docs_links(url)
        if not docs:
            return []

        tasks = [self._get_doc_data(doc) for doc in docs]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type},
            desc=f"MATO GROSSO DO SUL | {norm_type} {year}",
        )

        situation = self.situations[0] if self.situations else "Não consta"
        results = []
        for result in valid_results:
            queue_item = {
                "year": year,
                "situation": situation,
                "type": norm_type,
                **result,
            }
            await self._save_doc_result(queue_item)
            results.append(queue_item)

        if self.verbose:
            logger.info(f"Year: {year} | Type: {norm_type} | Results: {len(results)}")

        return results

    async def scrape(self) -> list:
        """Prefetch year indexes then delegate to BaseScraper's year-sequential flow."""
        await self._prefetch_year_indexes()
        return await super().scrape()
