from urllib.parse import urljoin, urlencode
from typing import Optional
import requests
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests.compat
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper
from src.database.saver import FileSaver

TYPES = {
    "Lei Complementar": 11,
    "Constituição Estadual 1967": 33,
    "Constituição Estadual 1947": 32,
    "Constituição Estadual 1935": 31,
    "Constituição Estadual 1891": 30,
    "Decreto": 2,
    "Decreto Financeiro": 1,
    "Decreto Simples": 3,
    "Emenda Constitucional": 4,
    "Lei Complementar": 5,
    "Lei Delegada": 6,
    "Lei Ordinária": 7,
    "Portaria Casa Civil": 19,
    "Portaria Conjunta Casa Civil": 20,
    "Instrução Normativa Casa Civil": 92,
}

VALID_SITUATIONS = [
    "Não consta"
]  # BahiaLegisla does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no lon

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class BahiaLegislaScraper(BaseScaper):
    """Webscraper for Bahia state legislation website (https://www.legislabahia.ba.gov.br/)

    Example search request: https://www.legislabahia.ba.gov.br/documentos?categoria%5B%5D=7&num=&ementa=&exp=&data%5Bmin%5D=2025-01-01&data%5Bmax%5D=2025-12-31&page=0
    """

    def __init__(
        self,
        base_url: str = "https://www.legislabahia.ba.gov.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "BAHIA"
        self.params = {
            "categoria[]": "",
            "num": "",
            "ementa": "",
            "exp": "",
            "data[min]": "",
            "data[max]": "",
            "page": 0,
        }
        # Initialize the FileSaver
        self.saver = FileSaver(self.docs_save_dir)

    def _format_search_url(self, norm_type_id: str, year: int, page: int) -> str:
        """Format url for search request"""
        self.params["categoria[]"] = norm_type_id
        self.params["data[min]"] = f"{year}-01-01"
        self.params["data[max]"] = f"{year}-12-31"
        self.params["page"] = page
        return f"{self.base_url}/documentos?{urlencode(self.params)}"

    def _get_docs_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'html_link'
        """
        soup = self._get_soup(url)

        if not soup:
            raise ValueError(f"Failed to get soup for URL: {url}")

        docs = []

        # check if the page is empty ("Nenhum resultado encontrado")
        if soup.find("td", class_="views-empty"):
            return []

        items = soup.find("tbody").find_all("tr")

        for item in items:
            tds = item.find_all("td")
            if len(tds) != 2:
                continue

            title = tds[0].find("b").text
            html_link = tds[0].find("a")["href"]

            docs.append(
                {
                    "title": title.strip(),
                    "html_link": html_link,
                }
            )

        return docs

    def _get_doc_data(self, doc_info: dict) -> Optional[dict]:
        """Get document data from given document dict"""
        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")
        url = urljoin(self.base_url, html_link)

        response = self._make_request(url)
        if not response:
            print(f"Failed to get document data from URL: {url}")
            return None

        soup = BeautifulSoup(response.content, "html.parser")

        # get norm_number, date, publication_date and summary
        norm_number = soup.find("div", class_="field--name-field-numero-doc")
        if norm_number:
            norm_number = norm_number.find("div", class_="field--item")  # type: ignore

        date = soup.find("div", class_="field--name-field-data-doc")
        if date:
            date = date.find("div", class_="field--item")  # type: ignore

        publication_date = soup.find(
            "div", class_="field--name-field-data-de-publicacao-no-doe"
        )
        if publication_date:
            publication_date = publication_date.find("div", class_="field--item")  # type: ignore

        summary = soup.find("div", class_="field--name-field-ementa")
        if summary:
            summary = summary.find("div", class_="field--item")  # type: ignore

        # get html string and text markdown
        # class="visivel-separador field field--name-body field--type-text-with-summary field--label-hidden field--item"
        norm_text_tag = soup.find("div", class_="field--name-body")
        if not norm_text_tag:
            return None  # invalid norm

        html_string = f"<html>{norm_text_tag.prettify()}</html>"

        buffer = BytesIO()
        buffer.write(html_string.encode())
        buffer.seek(0)

        text_markdown = self._get_markdown(stream=buffer)

        doc_info["norm_number"] = norm_number.text.strip() if norm_number else ""
        doc_info["date"] = date.text.strip() if date else ""
        doc_info["publication_date"] = (
            publication_date.text.strip() if publication_date else ""
        )
        doc_info["summary"] = summary.text.strip() if summary else ""
        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url

        return doc_info

    def _scrape_year(self, year: int) -> list:
        """Scrape norms for a specific year"""
        all_results = []
        
        for situation in tqdm(
            self.situations,
            desc="BAHIA | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc=f"BAHIA | Year: {year} | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):
                url = self._format_search_url(norm_type_id, year, 0)
                soup = self._get_soup(url)

                # get total pages
                pagination = soup.find("ul", class_="pagination js-pager__items")
                if pagination:
                    pages = pagination.find_all("li")
                    last_page = pages[-1].find("a")["href"]
                    total_pages = int(last_page.split("page=")[-1])
                else:
                    total_pages = 1

                # Get documents html links
                documents = []
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(
                            self._get_docs_links,
                            self._format_search_url(norm_type_id, year, page),
                        )
                        for page in range(total_pages)
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        total=total_pages,
                        desc="BAHIA | Get document link",
                        disable=not self.verbose,
                    ):
                        docs = future.result()
                        if docs:
                            documents.extend(docs)

                # Get document data
                results = []
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self._get_doc_data, doc) for doc in documents
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        total=len(documents),
                        desc="BAHIA | Get document data",
                        disable=not self.verbose,
                    ):
                        result = future.result()
                        if result is None:
                            continue

                        # prepare item for saving
                        queue_item = {
                            "year": year,
                            # hardcode since we only get valid documents in search request
                            "situation": situation,
                            "type": norm_type,
                            **result,
                        }

                        results.append(queue_item)

                    all_results.extend(results)
                    self.count += len(results)

                    if self.verbose:
                        print(
                            f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                        )

        return all_results
