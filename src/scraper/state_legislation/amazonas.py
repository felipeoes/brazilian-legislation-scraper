from urllib.parse import urljoin

from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.scraper import BaseScraper

TYPES = {
    "Decreto Legislativo": 41535,
    "Decreto": 41536,
    "Emendas Constitucionais": 41533,
    "Lei Complementar": 10,
    "Lei Delegada": 11,
    "Lei Ordinária": 12,
    "Lei Promulgada": 41534,
    "Regimento Interno": 41538,
    "Constituição Estadual": "12/1989/10/746",  # texto completo, modificar a lógica no scraper
}

VALID_SITUATIONS = [
    "Não consta"
]  # Conama does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = (
    []
)  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class LegislaAMScraper(BaseScraper):
    """Webscraper for Amazonas state legislation website (https://legisla.imprensaoficial.am.gov.br/)

    Example search request: https://legisla.imprensaoficial.am.gov.br/diario_am/41535/2022?page=1
    """

    # TODO: Change scraper to be based on https://sapl.al.am.leg.br/norma/pesquisar

    def __init__(
        self,
        base_url: str = "https://legisla.imprensaoficial.am.gov.br",
        **kwargs,
    ):
        from src.scraper.base.scraper import STATE_LEGISLATION_SAVE_DIR

        if STATE_LEGISLATION_SAVE_DIR:
            kwargs.setdefault("docs_save_dir", STATE_LEGISLATION_SAVE_DIR)
        super().__init__(
            base_url, name="AMAZONAS", types=TYPES, situations=SITUATIONS, **kwargs
        )
        self.fetched_constitution = False

    def _format_search_url(self, norm_type_id: str, year: int, page: int) -> str:
        """Format url for search request"""
        return f"{self.base_url}/diario_am/{norm_type_id}/{year}?page={page}"

    async def _get_docs_links(self, url: str) -> tuple[list, bool]:
        """Get documents html links from given page.
        Returns a tuple of (docs, reached_end) where docs is a list of dicts
        with keys 'title', 'summary', 'html_link' and reached_end indicates
        there are no more pages."""
        soup = await self.request_service.get_soup(url)

        # check if the page is empty (error)
        container = soup.find("div", id="container")
        if container:
            error = container.find("h1")
            if error and error.text == "Error":
                return [], True

        docs = []
        items = soup.find_all("li", class_="item-li")

        for item in items:
            title = item.find("h5").text
            html_link = item.find("a")
            if not html_link:  # some norms do not have a link to text, skip them
                continue
            html_link = html_link.get("href")

            docs.append(
                {
                    "title": title,
                    "summary": "",  # legislaAM does not provide a summary
                    "html_link": html_link,
                }
            )

        return docs, False

    def _get_norm_text(self, soup: BeautifulSoup) -> BeautifulSoup:
        """Get norm text from given document soup"""
        norm_element = soup.find("div", class_="materia rounded")
        norm_text = norm_element.text

        # check if norm_text length is less than 50 characters, if so, it is an invalid norm (doesn't have any text, just a title)
        if len(norm_text) < 70:
            return None

        # Remove the "Este texto não substitui o publicado no DOL/DOE..." disclaimer
        # It always appears as the last non-empty paragraph inside the materia div.
        for tag in norm_element.find_all(["p", "span", "div"]):
            txt = tag.get_text(strip=True)
            if txt.lower().startswith("este texto não substitui"):
                tag.decompose()
                break

        # add html tags to the text
        empty_soup = BeautifulSoup(
            "<html><head></head><body></body></html>", "html.parser"
        )
        empty_soup.body.append(norm_element)
        return empty_soup

    async def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given document dict"""
        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")

        url = urljoin(self.base_url, html_link)
        soup = await self.request_service.get_soup(url)

        html_content = self._get_norm_text(soup)
        if html_content is None:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=url,
                error_message="No norm text found in page",
            )
            return None
        html_string = html_content.prettify()

        text_markdown = await self._get_markdown(html_content=html_string)

        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url

        return doc_info

    async def _scrape_situation_type(
        self, situation: str, norm_type: str, norm_type_id: str, year: str
    ) -> list:
        """Scrape norms for a specific situation and type"""
        if not self.fetched_constitution and norm_type == "Constituição Estadual":
            url = f"{self.base_url}/diario_am/{norm_type_id}"
            doc_info = {
                "year": year,
                "situation": situation,
                "type": norm_type,
                "title": "Constituição Estadual",
                "date": year,
                "summary": "",
                "html_link": url,
            }

            doc_info = await self._get_doc_data(doc_info)

            self.fetched_constitution = True
            logger.info("Scraped state constitution")
            return [doc_info]

        total_pages = 30
        reached_end_page = False

        # Get documents html links
        documents = []
        start_page = 1
        while not reached_end_page:
            tasks = [
                self._get_docs_links(
                    self._format_search_url(norm_type_id, year, page),
                )
                for page in range(start_page, total_pages + 1)
            ]
            valid_results = await self._gather_results(
                tasks,
                context={
                    "year": year,
                    "type": norm_type,
                    "situation": situation,
                },
                desc=f"AMAZONAS | {norm_type} | get_docs_links",
            )
            for result in valid_results:
                docs, ended = result
                if ended:
                    reached_end_page = True
                if docs:
                    documents.extend(docs)

            start_page += total_pages
            total_pages += 10

            if (
                start_page > total_pages
            ):  # adding this condition to avoid infinite loop for some buggy pages
                reached_end_page = True

        # Get document data
        results = []
        tasks = [self._get_doc_data(doc_info) for doc_info in documents]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": situation},
            desc=f"AMAZONAS | {norm_type}",
        )
        for result in valid_results:
            queue_item = {
                "year": year,
                "situation": (
                    result["situation"] if result.get("situation") else situation
                ),
                "type": norm_type,
                **result,
            }
            results.append(queue_item)

        if self.verbose:
            logger.info(
                f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)}"
            )

        return results

    async def _scrape_year(self, year: str) -> list:
        """Scrape norms for a specific year"""
        tasks = [
            self._scrape_situation_type(sit, nt, ntid, year)
            for sit in self.situations
            for nt, ntid in self.types.items()
        ]
        valid = await self._gather_results(
            tasks,
            context={"year": year, "type": "NA", "situation": "NA"},
            desc=f"{self.name} | Year {year}",
        )
        return [
            item
            for result in valid
            for item in (result if isinstance(result, list) else [result])
        ]
