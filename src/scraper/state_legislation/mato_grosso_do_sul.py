from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from urllib.parse import urlencode, urljoin
from typing import List, Dict
from src.scraper.base.scraper import BaseScaper, YEAR_START

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


class MSAlemsScraper(BaseScaper):
    """Webscraper for Mato Grosso do Sul state legislation website (https://www.al.ms.gov.br/)

    Example search request: http://aacpdappls.net.ms.gov.br/appls/legislacao/secoge/govato.nsf/Emenda?OpenView&Start=1&Count=30&Expand=1#1

    OBS: Start=1&Count=30&Expand=1#1, for Expand 1 is the index related to the year
    """

    def __init__(
        self,
        base_url: str = "http://aacpdappls.net.ms.gov.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "MATO_GROSSO_DO_SUL"
        self.params = {
            "OpenView": "",
            "Start": 1,
            "Count": 10000,  # there is no limit for count, so setting to a large number to get all norms in one request
            "Expand": "",
        }
        self._initialize_saver()

    def _format_search_url(self, norm_type_id: str, year_index: int) -> str:
        """Format url for search request"""
        return f"{self.base_url}/appls/legislacao/secoge/govato.nsf/{norm_type_id}?{urlencode(self.params)}{year_index}"

    def _get_docs_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'summary', 'html_link'
        """

        soup = self._get_soup(url)
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

            # # first row will be year tr and will have more than 5 tds, skip it
            # if index == 0:
            #     continue

            title = tds[2].text.strip()
            summary = tds[3].text.strip()

            html_link = tds[2].find("a", href=True)
            html_link = html_link["href"]

            docs.append({"title": title, "summary": summary, "html_link": html_link})

        return docs

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given doc info"""
        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")
        url = urljoin(self.base_url, html_link)
        soup = self._get_soup(url)

        # norm text will be the first p tag in the document
        norm_text_tag = soup.find("p")
        html_string = norm_text_tag.prettify().strip()

        # since we're getting the p tag, need to add the html and body tags to make it a valid html for markitdown
        html_string = f"<html><body>{html_string}</body></html>"

        # get text markdown
        buffer = BytesIO()
        buffer.write(html_string.encode())
        buffer.seek(0)

        text_markdown = self._get_markdown(stream=buffer)

        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url

        return doc_info

    def _get_available_years(self, norm_type_id: str) -> list:
        """Get available years for given norm type"""
        # need to construct the url instead of using the _format_search_url method to avoid expanding the years
        url = f"{self.base_url}/appls/legislacao/secoge/govato.nsf/{norm_type_id}?OpenView?Start=1&Count=10000"
        soup = self._get_soup(url)

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

    def _scrape_year(
        self,
        year: int,
        year_index: int,
        norm_type: str,
        norm_type_id: str,
        situation: str,
    ) -> List[Dict]:
        url = self._format_search_url(norm_type_id, year_index)
        docs = self._get_docs_links(url)

        # Get document data
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._get_doc_data, doc) for doc in docs]

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="MATO GROSSO DO SUL | Get document data",
                disable=not self.verbose,
            ):
                try:
                    result = future.result()
                    if result is None:
                        continue

                    # prepare item for saving
                    queue_item = {
                        "year": year,
                        # hardcode since it seems we only get valid documents in search request
                        "situation": situation,
                        "type": norm_type,
                        **result,
                    }

                    results.append(queue_item)
                except Exception as e:
                    print(f"Error getting document data: {e}")
                    continue

            self.results.extend(results)
            self.count += len(results)

            if self.verbose:
                print(
                    f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                )

        return results

    def scrape(self) -> list:
        """Scrape data from all years"""
        if not self.saver:
            raise ValueError(
                "Saver is not initialized. Call _initialize_saver() first."
            )

        # check if can resume from last scrapped year
        resume_from = self.year_start  # 1808
        forced_resume = self.year_start != YEAR_START
        if self.saver.last_year is not None and not forced_resume:
            print(f"Resuming from {self.saver.last_year}")
            resume_from = int(self.saver.last_year)
        else:
            print(f"Starting from {resume_from}")

        # scrape data
        for situation in tqdm(
            self.situations,
            desc="MATO GROSSO DO SUL | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc="MATO GROSSO DO SUL | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):

                # get available years
                years = self._get_available_years(norm_type_id)

                for year_index, year in enumerate(
                    tqdm(
                        years,
                        desc="MATO GROSSO DO SUL | Year",
                        total=len(years),
                        disable=not self.verbose,
                    )
                ):
                    if year < resume_from:
                        continue

                    # scrape year
                    results = self._scrape_year(
                        year, year_index + 1, norm_type, norm_type_id, situation
                    )
                    if results:
                        self.saver.save(results)

        return self.results
