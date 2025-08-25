from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests.compat
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper

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


class LegislaAMScraper(BaseScaper):
    """Webscraper for Amazonas state legislation website (https://legisla.imprensaoficial.am.gov.br/)

    Example search request: https://legisla.imprensaoficial.am.gov.br/diario_am/41535/2022?page=1
    """

    # TODO: Change scraper to be based on https://sapl.al.am.leg.br/norma/pesquisar

    def __init__(
        self,
        base_url: str = "https://legisla.imprensaoficial.am.gov.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "AMAZONAS"
        self.reached_end_page = False
        self.fetched_constitution = False
        self._initialize_saver()

    def _format_search_url(self, norm_type_id: str, year: int, page: int) -> str:
        """Format url for search request"""
        return f"{self.base_url}/diario_am/{norm_type_id}/{year}?page={page}"

    def _get_docs_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'summary', 'html_link'"""
        soup = self._get_soup(url)

        # check if the page is empty (error)
        container = soup.find("div", id="container")
        if container:
            error = container.find("h1")
            if error and error.text == "Error":
                self.reached_end_page = True
                return []

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

        return docs

    def _get_norm_text(self, soup: BeautifulSoup) -> BeautifulSoup:
        """Get norm text from given document soup"""
        norm_element = soup.find("div", class_="materia rounded")
        norm_text = norm_element.text

        # check if norm_text length is less than 50 characters, if so, it is an invalid norm (doesn't have any text, just a title)
        if len(norm_text) < 70:
            return None

        # add html tags to the text
        empty_soup = BeautifulSoup(
            "<html><head></head><body></body></html>", "html.parser"
        )
        empty_soup.body.append(norm_element)
        return empty_soup

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given document dict"""
        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")

        url = urljoin(self.base_url, html_link)
        soup = self._get_soup(url)

        html_content = self._get_norm_text(soup)
        if html_content is None:
            return None
        html_string = html_content.prettify()

        buffer = BytesIO()
        buffer.write(html_content.html.encode())
        buffer.seek(0)

        text_markdown = self._get_markdown(stream=buffer)

        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url

        return doc_info

    def _scrape_year(self, year: str):
        """Scrape norms for a specific year"""

        for situation in tqdm(
            self.situations,
            desc="AMAZONAS | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc=f"AMAZONAS | Year: {year} | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):
                # total pages info is not available, so we need to check if the page is empty. In order to make parallel calls, we will assume an initial number of pages and increase if needed. We will know that all the pages were scraped when we request a page and it shows a error message

                if (
                    not self.fetched_constitution
                    and norm_type == "Constituição Estadual"
                ):
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

                    doc_info = self._get_doc_data(doc_info)

                    self.queue.put(doc_info)
                    self.results.append(doc_info)
                    self.count += 1
                    self.fetched_constitution = True
                    print("Scraped state constitution")
                    continue

                total_pages = 30
                self.reached_end_page = False

                # Get documents html links
                documents = []
                start_page = 1
                while not self.reached_end_page:
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = [
                            executor.submit(
                                self._get_docs_links,
                                self._format_search_url(norm_type_id, year, page),
                            )
                            for page in range(start_page, total_pages + 1)
                        ]
                        for future in tqdm(
                            as_completed(futures),
                            total=total_pages - start_page + 1,
                            desc="AMAZONAS | Get document link",
                            disable=not self.verbose,
                        ):
                            docs = future.result()
                            if docs:
                                documents.extend(docs)

                    start_page += total_pages
                    total_pages += 10

                    if (
                        start_page > total_pages
                    ):  # adding this condition to avoid infinite loop for some buggy pages
                        self.reached_end_page = True

                # Get document data
                results = []
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self._get_doc_data, doc_info)
                        for doc_info in documents
                    ]
                    for future in tqdm(
                        as_completed(futures),
                        total=len(documents),
                        desc="AMAZONAS | Get document data",
                        disable=not self.verbose,
                    ):
                        result = future.result()
                        if result is None:
                            continue

                        # save to one drive
                        queue_item = {
                            "year": year,
                            # hardcode since we only get valid documents in search request
                            "situation": (
                                result["situation"]
                                if result.get("situation")
                                else situation
                            ),
                            "type": norm_type,
                            **result,
                        }

                        self.queue.put(queue_item)
                        results.append(queue_item)

                self.results.extend(results)
                self.count += len(results)

                if self.verbose:
                    print(
                        f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                    )
