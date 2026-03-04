import asyncio
import time
from loguru import logger
from tqdm import tqdm
from urllib.parse import urlencode, urljoin
from src.scraper.base.scraper import BaseScraper, YEAR_START
from src.scraper.base.concurrency import bounded_gather

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


class MSAlemsScraper(BaseScraper):
    """Webscraper for Mato Grosso do Sul state legislation website (https://www.al.ms.gov.br/)

    Example search request: http://aacpdappls.net.ms.gov.br/appls/legislacao/secoge/govato.nsf/Emenda?OpenView&Start=1&Count=30&Expand=1#1

    OBS: Start=1&Count=30&Expand=1#1, for Expand 1 is the index related to the year
    """

    def __init__(
        self,
        base_url: str = "http://aacpdappls.net.ms.gov.br",
        **kwargs,
    ):
        from src.scraper.base.scraper import STATE_LEGISLATION_SAVE_DIR

        if STATE_LEGISLATION_SAVE_DIR:
            kwargs.setdefault("docs_save_dir", STATE_LEGISLATION_SAVE_DIR)
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
            "Count": 10000,  # there is no limit for count, so setting to a large number to get all norms in one request
            "Expand": "",
        }

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

        return doc_info

    async def _get_available_years(self, norm_type_id: str) -> list:
        """Get available years for given norm type"""
        # need to construct the url instead of using the _format_search_url method to avoid expanding the years
        url = f"{self.base_url}/appls/legislacao/secoge/govato.nsf/{norm_type_id}?OpenView?Start=1&Count=10000"
        soup = await self.request_service.get_soup(url)

        years = []
        table = soup.find("table", border="0", cellpadding="2", cellspacing="0")
        items = table.find_all("tr", valign="top")
        for _, item in enumerate(items):
            td = item.find("td")
            year = td.text.strip()

            if not year:
                continue

            years.append(int(year))

        # sort in descending order to guarantee we start from the latest year for the rest of the scraping logic to work
        return sorted(years, reverse=True)

    async def _scrape_year(
        self,
        year: int,
        year_index: int,
        norm_type: str,
        norm_type_id: str,
        situation: str,
    ) -> list[dict]:
        url = self._format_search_url(norm_type_id, year_index)
        docs = await self._get_docs_links(url)

        # Get document data
        results = []
        tasks = [self._get_doc_data(doc) for doc in docs]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": situation},
            desc=f"MATO GROSSO DO SUL | {norm_type}",
        )

        for result in tqdm(
            valid_results,
            total=len(valid_results),
            desc="MATO GROSSO DO SUL | Get document data",
            disable=not self.verbose,
        ):
            # prepare item for saving
            queue_item = {
                "year": year,
                # hardcode since it seems we only get valid documents in search request
                "situation": situation,
                "type": norm_type,
                **result,
            }

            results.append(queue_item)

        if self.verbose:
            logger.info(
                f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)}"
            )

        return results

    async def _scrape_situation_type(
        self, situation: str, norm_type: str, norm_type_id: str, resume_from: int
    ) -> list:
        """Scrape norms for a specific situation and type across all years"""
        # get available years
        years = await self._get_available_years(norm_type_id)
        years_to_scrape = [
            (year_index, year)
            for year_index, year in enumerate(years)
            if year >= resume_from
        ]

        async def _scrape_and_save(year_index, year):
            results = await self._scrape_year(
                year, year_index + 1, norm_type, norm_type_id, situation
            )
            if results:
                await self.saver.save(results)
            return results or []

        all_results = await bounded_gather(
            [_scrape_and_save(yi, y) for yi, y in years_to_scrape],
            max_concurrency=self.max_workers,
            desc=f"MATO GROSSO DO SUL | {norm_type}",
            verbose=self.verbose,
        )

        flat_results = []
        for results in all_results:
            if results:
                flat_results.extend(results)

        return flat_results

    async def scrape(self) -> list:
        """Scrape data from all years"""
        if not self.saver:
            raise ValueError(
                "Saver is not initialized. Call _initialize_saver() first."
            )

        await self._ensure_session()
        self._scrape_start = time.time()

        # check if can resume from last scrapped year
        resume_from = self.year_start  # 1808
        forced_resume = self.year_start != YEAR_START
        if self.saver.last_year is not None and not forced_resume:
            logger.info(f"Resuming from {self.saver.last_year}")
            resume_from = int(self.saver.last_year)
        else:
            logger.info(f"Starting from {resume_from}")

        # scrape data with flattened situation+type loops
        tasks = [
            self._scrape_situation_type(sit, nt, ntid, resume_from)
            for sit in self.situations
            for nt, ntid in self.types.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error in _scrape_situation_type: {result}")
            elif result:
                self.results.extend(result)
                self.count += len(result)

        await self._save_summary()
        return self.results
