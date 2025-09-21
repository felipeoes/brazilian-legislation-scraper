from typing import Optional
from urllib.parse import urljoin

import requests
import re
from io import BytesIO
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper
from src.database.saver import FileSaver

TYPES = {
    "Acórdão do Colegiado da Procuradoria": 18,
    "Ato Administrativo": 10,
    "Consituição Estadual": 1,
    "Decreto Executivo": 12,
    "Decreto Legislativo": 5,
    "Decreto Normativo": 6,
    "Decreto Regulamentar": 9,
    "Decreto Suplementar": 11,
    "Emenda à Constituição Estadual": 2,
    "Lei Complementar": 4,
    "Lei Delegada": 8,
    "Lei Ordinária": 3,
    "Resolução": 7,
}

VALID_SITUATIONS = {
    "Em Vigor": 2,
}

INVALID_SITUATIONS = {
    "Declarada Inconstitucional": 4,
    "Declarada Insubisistente": 5,
    "Eficácia Suspensa": 7,
    "Não Emitida. Falha de Sequência.": 9,
    "Revogada": 3,
    "Tornado Sem Efeito": 10,
}  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = {**VALID_SITUATIONS, **INVALID_SITUATIONS}


class ESAlesScraper(BaseScaper):
    """Webscraper for Espirito Santo state legislation website (https://www3.al.es.gov.br/legislacao)

    Example search request: https://www3.al.es.gov.br/legislacao/consulta-legislacao.aspx?tipo=7&situacao=2&ano=2000&interno=1
    """

    def __init__(
        self,
        base_url: str = "https://www3.al.es.gov.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "ESPIRITO_SANTO"
        self.params = {"tipo": "", "situacao": "", "ano": "", "interno": 1}
        self.reached_end_page = False
        # Initialize the FileSaver
        self.saver = FileSaver(self.docs_save_dir)

    def _format_search_url(
        self, norm_type_id: str, situation_id: str, year: int
    ) -> str:
        """Format url for search request"""
        self.params["tipo"] = norm_type_id
        self.params["situacao"] = situation_id
        self.params["ano"] = year

        return f"{self.base_url}/legislacao/consulta-legislacao.aspx?tipo={norm_type_id}&situacao={situation_id}&ano={year}&interno=1"

    def _get_page_html(self, url: str, page_number: int):
        """
        Navigates to a specific page number using __doPostBack.

        Args:
            url: The initial URL of the page.
            page_number: The page number to navigate to (e.g., 1, 2, 3...).

        Returns:
            The HTML content of the requested page as bytes, or None if an error occurs.
        """
        session = (
            requests.Session()
        )  # need to create a new session for each request in order to make the logic work

        retries = 3
        for attempt in range(retries):
            try:
                response = session.get(url, verify=False)
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

                if response:
                    break  # If the request is successful, exit the loop
            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt == retries - 1:
                    print("Max retries reached. Exiting.")
                    return None

        soup = BeautifulSoup(response.content, "html.parser")

        if page_number == 1:  # don't need to post back for page 1
            return soup.prettify()

        viewstate = soup.find(id="__VIEWSTATE")
        eventvalidation = soup.find(id="__EVENTVALIDATION")

        if not viewstate or not eventvalidation:
            print("Error: __VIEWSTATE or __EVENTVALIDATION not found on the page.")
            return None

        viewstate_value = viewstate["value"]
        eventvalidation_value = eventvalidation["value"]

        if page_number > 1:
            # Construct the event target for specific page number
            page_index = page_number - 1
            event_target = f"ctl00$ContentPlaceHolder1$rptPaging$ctl{page_index:02d}$lbPaging"  # Using :02d to
        else:
            print("Error: Page number must be greater than or equal to 1.")
            return None

        post_data = {
            "__EVENTTARGET": event_target,
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": viewstate_value,
            "__EVENTVALIDATION": eventvalidation_value,
            # Include other necessary form data if needed
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0",  # Optional: Mimic a browser
        }

        try:
            page_response = session.post(
                url, data=post_data, headers=headers, verify=False
            )
            page_response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            return page_response.content
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page_number}: {e}")
            return None

    def _get_docs_links(self, url: str, page: int) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'year', 'norm_number', 'summary', 'document_url'
        """

        page_html = self._get_page_html(url, page)
        soup = BeautifulSoup(page_html, "html.parser")
        docs = []

        # find all items
        container = soup.find("div", class_="kt-portlet__body")
        items = container.find_all("div", class_="kt-widget5__item")

        # if no items found, we reached the end of the page
        if not items:
            self.reached_end_page = True
            return []

        # check if page number is not in pagination and is greater than last available page
        if page > 0:
            pagination = soup.find("div", class_="pagination pagination-custom")
            if not pagination:
                self.reached_end_page = True
                return []

            last_available_page = int(pagination.find_all("a")[-2].text)
            if page > last_available_page:
                self.reached_end_page = True
                return []

        for item in items:
            # get title
            title = item.find("a", class_="kt-widget5__title").text
            summary = item.find("a", class_="kt-widget5__desc").text
            date = item.find("span", class_="kt-font-info").text
            authors = (
                item.find_all("div", class_="kt-widget5__info")[1]
                .find("span", class_="kt-font-info")
                .text
            )
            # btn btn-sm btn-label-info btn-pill d-block
            doc_link = item.find_all("a", class_="btn-label-info")
            if (
                len(doc_link) == 0
            ):  # if there is no link to the document text, the norm won't be useful
                continue

            doc_link = doc_link[0]["href"]

            if "processo.aspx?" in doc_link:
                # this is a link to a process, not a document
                print(f"Skipping '{title}' since it is a process link")
                continue

            # skip docx links
            if doc_link.endswith(".docx"):
                print(f"Skipping '{title}' since it is a docx document")
                continue

            docs.append(
                {
                    "title": re.sub(r"\r\n +", " ", title.strip()),
                    "summary": summary.strip(),
                    "date": date.strip(),
                    "authors": authors.strip(),
                    "doc_link": doc_link,
                }
            )

        return docs

    def _get_doc_data(self, doc_info: dict) -> Optional[dict]:
        """Get document data from document link"""
        doc_link = doc_info.pop("doc_link")
        url = urljoin(self.base_url, doc_link)

        # some urls are malformed, e.g., they ends with .pd instead of .pdf
        if url.endswith(".pd"):
            url = url + "f"

        # if url ends with .pdf, get only text_markdown
        if url.endswith(".pdf"):
            text_markdown = self._get_markdown(url)

            if not text_markdown:
                # pdf may be an image
                response = self._make_request(url)
                if not response:
                    print(f"Failed to download PDF from URL: {url}")
                    return None

                text_markdown = self._get_pdf_image_markdown(response.content)

            doc_info["html_string"] = ""
            doc_info["text_markdown"] = text_markdown
            doc_info["document_url"] = url
            return doc_info

        soup = self._get_soup(url)
        if not soup:
            print(f"Failed to get soup for URL: {url}")
            return None

        html_string = soup.prettify()

        buffer = BytesIO()
        buffer.write(html_string.encode())
        buffer.seek(0)

        text_markdown = self._get_markdown(stream=buffer)

        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url

        return doc_info

    def _scrape_year(self, year: int) -> list:
        """Scrape norms for a specific year"""
        all_results = []

        for situation, situation_id in tqdm(
            self.situations.items(),
            desc="ESPIRITO SANTO | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc=f"ESPIRITO SANTO | Year: {year} | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):

                # total pages info is not available, so we need to check if the page is empty. In order to make parallel calls, we will assume an initial number of pages and increase if needed. We will know that all the pages were scraped when we request a page and it shows a error message

                total_pages = 10
                self.reached_end_page = False

                # Get documents html links
                documents = []
                start_page = 1
                while not self.reached_end_page:
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = [
                            executor.submit(
                                self._get_docs_links,
                                self._format_search_url(
                                    norm_type_id, situation_id, year
                                ),
                                page,
                            )
                            for page in range(start_page, total_pages + 1)
                        ]
                    for future in tqdm(
                        as_completed(futures),
                        total=total_pages - start_page + 1,
                        desc="ESPIRITO SANTO | Get document link",
                        disable=not self.verbose,
                    ):
                        docs = future.result()
                        if docs:
                            documents.extend(docs)

                    start_page += total_pages
                    total_pages += 10

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
                        desc="ESPIRITO SANTO | Get document data",
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
