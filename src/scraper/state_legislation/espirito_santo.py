from io import BytesIO
from urllib.parse import urljoin

import re
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from src.scraper.base.scraper import StateScraper

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


class ESAlesScraper(StateScraper):
    """Webscraper for Espirito Santo state legislation website (https://www3.al.es.gov.br/legislacao)

    Example search request: https://www3.al.es.gov.br/legislacao/consulta-legislacao.aspx?tipo=7&situacao=2&ano=2000&interno=1
    """

    def __init__(
        self,
        base_url: str = "https://www3.al.es.gov.br",
        **kwargs,
    ):
        super().__init__(
            base_url,
            name="ESPIRITO_SANTO",
            types=TYPES,
            situations=SITUATIONS,
            **kwargs,
        )

    def _format_search_url(
        self, norm_type_id: str, situation_id: str, year: int
    ) -> str:
        """Format url for search request"""
        return f"{self.base_url}/legislacao/consulta-legislacao.aspx?tipo={norm_type_id}&situacao={situation_id}&ano={year}&interno=1"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=15),
        reraise=True,
    )
    async def _get_page_html(self, url: str, page_number: int):
        """Navigate to a specific page number using __doPostBack via RequestService."""
        resp = await self.request_service.make_request(url)
        if not resp:
            return None

        content = await resp.read()
        soup = BeautifulSoup(content, "html.parser")

        if page_number == 1:
            return soup.prettify()

        viewstate = soup.find(id="__VIEWSTATE")
        eventvalidation = soup.find(id="__EVENTVALIDATION")

        if not viewstate or not eventvalidation:
            logger.error(
                "Error: __VIEWSTATE or __EVENTVALIDATION not found on the page."
            )
            return None

        page_index = page_number - 1
        event_target = (
            f"ctl00$ContentPlaceHolder1$rptPaging$ctl{page_index:02d}$lbPaging"
        )

        post_data = {
            "__EVENTTARGET": event_target,
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": viewstate["value"],
            "__EVENTVALIDATION": eventvalidation["value"],
        }

        post_resp = await self.request_service.make_request(
            url, method="POST", payload=post_data
        )
        if not post_resp:
            logger.error(f"Error fetching page {page_number}")
            return None
        return await post_resp.read()

    async def _get_docs_links(self, url: str, page: int) -> tuple[list, bool]:
        """Get documents html links from given page.
        Returns (docs, reached_end) where docs is a list of dicts and
        reached_end indicates there are no more pages.
        """

        page_html = await self._get_page_html(url, page)
        soup = BeautifulSoup(page_html, "html.parser")
        docs = []

        # find all items
        container = soup.find("div", class_="kt-portlet__body")
        items = container.find_all("div", class_="kt-widget5__item")

        # if no items found, we reached the end of the page
        if not items:
            return [], True

        # check if page number is not in pagination and is greater than last available page
        if page > 0:
            pagination = soup.find("div", class_="pagination pagination-custom")
            if not pagination:
                return [], True

            last_available_page = int(pagination.find_all("a")[-2].text)
            if page > last_available_page:
                return [], True

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
                logger.info(f"Skipping '{title}' since it is a process link")
                continue

            # skip docx links
            if doc_link.endswith(".docx"):
                logger.info(f"Skipping '{title}' since it is a docx document")
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

        return docs, False

    async def _get_doc_data(self, doc_info: dict) -> dict | None:
        """Get document data from document link"""
        doc_link = doc_info.pop("doc_link")
        url = urljoin(self.base_url, doc_link)

        # some urls are malformed, e.g., they ends with .pd instead of .pdf
        if url.endswith(".pd"):
            url = url + "f"

        if self._is_already_scraped(url, doc_info.get("title", "")):
            return None

        # if url ends with .pdf, get only text_markdown
        if url.endswith(".pdf"):
            text_markdown, raw_content, content_ext = await self._download_and_convert(
                url
            )

            if not text_markdown:
                # pdf may be an image
                response = await self.request_service.make_request(url)
                if not response:
                    logger.error(f"Failed to download PDF from URL: {url}")
                    await self._save_doc_error(
                        title=doc_info.get("title", ""),
                        year=doc_info.get("year", ""),
                        html_link=url,
                        error_message="Failed to download PDF",
                    )
                    return None

                pdf_content = await response.read()
                text_markdown = await self._get_markdown(stream=BytesIO(pdf_content))
                raw_content = pdf_content
                content_ext = ".pdf"

            doc_info["text_markdown"] = text_markdown
            doc_info["document_url"] = url
            doc_info["_raw_content"] = raw_content
            doc_info["_content_extension"] = content_ext
            return doc_info

        soup = await self.request_service.get_soup(url)
        if not soup:
            logger.error(f"Failed to get soup for URL: {url}")
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=url,
                error_message="Failed to get document page",
            )
            return None

        html_string = soup.prettify()

        text_markdown = await self._get_markdown(html_content=html_string)

        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url
        doc_info["_raw_content"] = html_string.encode("utf-8")
        doc_info["_content_extension"] = ".html"

        return doc_info

    async def _scrape_situation_type(
        self,
        year: int,
        situation: str,
        situation_id: int,
        norm_type: str,
        norm_type_id: int,
    ) -> list:
        """Scrape norms for a specific situation and type"""
        # total pages info is not available, so we need to check if the page is empty. In order to make parallel calls, we will assume an initial number of pages and increase if needed. We will know that all the pages were scraped when we request a page and it shows a error message

        total_pages = 10
        reached_end_page = False

        # Get documents html links
        documents = []
        start_page = 1
        while not reached_end_page:
            tasks = [
                self._get_docs_links(
                    self._format_search_url(norm_type_id, situation_id, year),
                    page,
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
                desc=f"ESPIRITO SANTO | {norm_type} | get_docs_links",
            )
            for result in valid_results:
                docs, ended = result
                if ended:
                    reached_end_page = True
                if docs:
                    documents.extend(docs)

            start_page += total_pages
            total_pages += 10

        # Get document data
        ctx = {"year": year, "situation": situation, "type": norm_type}
        tasks = [
            self._with_save(self._get_doc_data(doc_info), ctx) for doc_info in documents
        ]
        results = await self._gather_results(
            tasks,
            context=ctx,
            desc=f"ESPIRITO SANTO | {norm_type}",
        )

        if self.verbose:
            logger.info(
                f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)}"
            )

        return results

    async def _scrape_year(self, year: int) -> list:
        """Scrape norms for a specific year"""
        tasks = [
            self._scrape_situation_type(
                year, situation, situation_id, norm_type, norm_type_id
            )
            for situation, situation_id in self.situations.items()
            for norm_type, norm_type_id in self.types.items()
        ]
        valid = await self._gather_results(
            tasks,
            context={"year": year, "type": "N/A", "situation": "N/A"},
            desc=f"{self.name} | Year {year}",
        )
        return self._flatten_results(valid)
