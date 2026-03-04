import re
from urllib.parse import urljoin, urlencode
from typing import Optional
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_random_exponential
from src.scraper.base.scraper import BaseScraper, STATE_LEGISLATION_SAVE_DIR

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
    "Lei Delegada": 6,
    "Lei Ordinária": 7,
    "Portaria Casa Civil": 19,
    "Portaria Conjunta Casa Civil": 20,
    "Instrução Normativa Casa Civil": 92,
}

VALID_SITUATIONS = [
    "Não consta"
]  # BahiaLegisla does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class BahiaLegislaScraper(BaseScraper):
    """Webscraper for Bahia state legislation website (https://www.legislabahia.ba.gov.br/)

    Example search request: https://www.legislabahia.ba.gov.br/documentos?categoria%5B%5D=7&num=&ementa=&exp=&data%5Bmin%5D=2025-01-01&data%5Bmax%5D=2025-12-31&page=0
    """

    _REVOGADO_RE = re.compile(r"\brevogad[ao]\b", re.IGNORECASE)

    def __init__(
        self,
        base_url: str = "https://www.legislabahia.ba.gov.br",
        **kwargs,
    ):
        if STATE_LEGISLATION_SAVE_DIR:
            kwargs.setdefault("docs_save_dir", STATE_LEGISLATION_SAVE_DIR)
        super().__init__(
            base_url, name="BAHIA", types=TYPES, situations=SITUATIONS, **kwargs
        )

    def _build_search_url(self, norm_type_id: str, year: int, page: int) -> str:
        """Build search URL from arguments (no shared state mutation)."""
        params = {
            "categoria[]": norm_type_id,
            "num": "",
            "ementa": "",
            "exp": "",
            "data[min]": f"{year}-01-01",
            "data[max]": f"{year}-12-31",
            "page": page,
        }
        return f"{self.base_url}/documentos?{urlencode(params)}"

    @retry(
        stop=stop_after_attempt(7),
        wait=wait_random_exponential(multiplier=2, max=30),
        reraise=True,
    )
    async def _fetch_soup_with_retry(
        self, url: str, timeout: int = 120
    ) -> BeautifulSoup:
        soup = await self.request_service.get_soup(url, timeout=timeout)
        if not soup:
            raise ValueError(f"Failed to get soup for URL: {url}")
        return soup

    @retry(
        stop=stop_after_attempt(7),
        wait=wait_random_exponential(multiplier=2, max=30),
        reraise=True,
    )
    async def _fetch_request_with_retry(self, url: str, timeout: int = 120):
        response = await self.request_service.make_request(url, timeout=timeout)
        if not response:
            raise ValueError(f"Failed to fetch document page: {url}")
        return response

    async def _get_docs_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'html_link'
        """
        soup = await self._fetch_soup_with_retry(url)

        docs = []

        # check if the page is empty ("Nenhum resultado encontrado")
        if soup.find("td", class_="views-empty"):
            return []

        tbody = soup.find("tbody")
        if not tbody:
            return []

        items = tbody.find_all("tr")

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

    async def _get_doc_data(self, doc_info: dict) -> Optional[dict]:
        """Get document data from given document dict"""
        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")
        url = urljoin(self.base_url, html_link)

        try:
            response = await self._fetch_request_with_retry(url)
        except Exception as e:
            logger.error(f"Failed to get document data from URL: {url} | Error: {e}")
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year="",
                situation="",
                norm_type="",
                html_link=url,
                error_message="Failed to fetch document page after multiple attempts",
            )
            return None

        content = await response.read()
        soup = BeautifulSoup(content, "html.parser")

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
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year="",
                situation="",
                norm_type="",
                html_link=url,
                error_message="Could not find div.field--name-body in document page",
            )
            return None  # invalid norm

        # Remove empty heading tags — Word HTML exports often end with <h2></h2>.
        # markitdown treats any <h2>/<h3>/… as a section boundary and discards
        # all <p> elements that precede it, so these artifacts must be stripped.
        for heading in norm_text_tag.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
            if not heading.get_text(strip=True):
                heading.decompose()

        html_string = f"<html><body>{norm_text_tag.prettify()}</body></html>"

        # Detect revogado/revogada via regex: <span class="revogado"> or
        # <div class="alteracao"> whose text starts with "revogado/a".
        is_revogado = bool(norm_text_tag.find("span", class_=self._REVOGADO_RE)) or any(
            self._REVOGADO_RE.match(div.get_text(strip=True))
            for div in norm_text_tag.find_all("div", class_="alteracao")
        )
        if is_revogado:
            doc_info["situation"] = "Revogado"

        # Use direct HTML content conversion instead of BytesIO stream
        text_markdown = await self._get_markdown(html_content=html_string)

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

    async def _scrape_situation_type(
        self, year: int, situation: str, norm_type: str, norm_type_id: int
    ) -> list:
        """Scrape norms for a specific situation and type"""
        url = self._build_search_url(norm_type_id, year, 0)

        soup = await self._fetch_soup_with_retry(url)

        # get total pages
        total_pages = 1
        pagination = soup.find("ul", class_="pagination js-pager__items")
        if pagination:
            pages = pagination.find_all("li")
            # pages[-1] is the "last page" nav button whose href=page=N (0-indexed).
            # range(N+1) gives pages 0..N inclusive.
            last_li_a = pages[-1].find("a")
            if last_li_a:
                total_pages = int(last_li_a["href"].split("page=")[-1]) + 1

        # Get documents html links
        documents = []
        tasks = [
            self._get_docs_links(
                self._build_search_url(norm_type_id, year, page),
            )
            for page in range(total_pages)
        ]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": situation},
            desc=f"BAHIA | {norm_type} | get_docs_links",
        )
        for result in valid_results:
            if result:
                documents.extend(result)

        # Get document data
        results = []
        tasks = [self._get_doc_data(doc) for doc in documents]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": situation},
            desc=f"BAHIA | {norm_type}",
        )
        for result in valid_results:
            queue_item = {
                "year": year,
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

    async def _scrape_year(self, year: int) -> list:
        """Scrape norms for a specific year"""
        tasks = [
            self._scrape_situation_type(year, situation, norm_type, norm_type_id)
            for situation in self.situations
            for norm_type, norm_type_id in self.types.items()
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
