import re

from collections import defaultdict
from datetime import datetime
from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.scraper import BaseScraper, STATE_LEGISLATION_SAVE_DIR

TYPES = {
    "Lei Ordinária": "lei_ordinarias",
    "Lei Complementar": "lei_complementares",
    "Constituição Estadual": "detalhar_constituicao",  # texto completo, modificar a lógica no scraper
    "Decreto": "lei_decretos",
}

VALID_SITUATIONS = [
    "Não consta revogação expressa",
]  # Legis - Acre only publishes norms that are currently valid (no explicit revocation)

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class AcreLegisScraper(BaseScraper):
    """Webscraper for Legis - Acre website (https://legis.ac.gov.br)

    Example search request: https://legis.ac.gov.br/principal/1
    """

    def __init__(
        self,
        base_url: str = "https://legis.ac.gov.br/principal",
        **kwargs,
    ):
        if STATE_LEGISLATION_SAVE_DIR:
            kwargs.setdefault("docs_save_dir", STATE_LEGISLATION_SAVE_DIR)
        super().__init__(
            base_url, name="ACRE", types=TYPES, situations=SITUATIONS, **kwargs
        )
        self.year_regex = re.compile(r"\d{4}")
        # {year: {norm_type: [doc_info, ...]}} — populated before year iteration
        self._prefetched_docs: dict[int, dict[str, list]] = defaultdict(
            lambda: defaultdict(list)
        )

    def _format_search_url(self, norm_type_id: str) -> str:
        """Format url for search request"""
        return f"{self.base_url}/{norm_type_id}"

    async def _get_docs_links(self, soup: BeautifulSoup, norm_type_id: str) -> list:
        """Get documents html links from soup object.
        Returns a list of dicts with keys 'title', 'year', 'summary' and 'html_link'
        """

        # get all tr's from table that is within the div with id == norm_type_id
        trs = (
            soup.find("div", id=norm_type_id)
            .find("table")
            .find_all("tr", {"class": "visaoQuadrosTr"})
        )

        # get all html links
        html_links = []
        for tr in trs:
            a = tr.find("a")
            title = a.text.strip()
            html_link = a["href"]
            summary = tr.find("td").find_next("td").text.strip()
            year = self.year_regex.search(title.split(",")[-1]).group()
            html_links.append(
                {
                    "title": title,
                    "year": year,
                    "summary": summary,
                    "html_link": html_link,
                }
            )

        return html_links

    async def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given html link"""
        doc_html_link = doc_info["html_link"]
        doc_title = doc_info["title"]
        doc_year = doc_info["year"]
        doc_summary = doc_info["summary"]

        response = await self.request_service.make_request(doc_html_link)
        if response is None:
            await self._save_doc_error(
                title=doc_title,
                year=doc_year,
                html_link=doc_html_link,
                error_message="Failed to fetch document page",
            )
            return None

        soup = BeautifulSoup(await response.text(), "html.parser")
        html_string = soup.find("div", id="body-law")
        if not html_string:
            html_string = soup.find("div", id="exportacao")

        if html_string:
            # Remove attachments table
            for row in html_string.find_all("div", class_="row"):
                row.decompose()
            # Remove the "texto_publicado_doe" span
            doe_span = html_string.find("span", id="texto_publicado_doe")
            if doe_span:
                doe_span.decompose()

        if html_string:
            # Remove hyperlinks (unwrap <a> tags, keeping inner text)
            for a_tag in html_string.find_all("a"):
                a_tag.unwrap()

        html_string = html_string.prettify() if html_string else ""

        # get text markdown from extracted HTML
        text_markdown = await self._get_markdown(
            html_content=html_string,
        )

        if text_markdown is None:
            await self._save_doc_error(
                title=doc_title,
                year=doc_year,
                html_link=doc_html_link,
                error_message="Failed to convert document to markdown",
            )
            return None

        return {
            "title": doc_title,
            "year": doc_year,
            "summary": doc_summary,
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": doc_html_link,
        }

    async def _get_state_constitution(self, norm_type_id: str) -> dict:
        """Get state constitution data"""
        document_url = f"{self.base_url.replace('/principal', '')}/{norm_type_id}"
        response = await self.request_service.make_request(document_url)
        if response is None:
            return {
                "title": "Constituição Estadual",
                "year": datetime.now().year,
                "summary": "Constituição Estadual do Estado do Acre",
                "html_string": "",
                "text_markdown": None,
                "document_url": document_url,
            }

        soup = BeautifulSoup(await response.text(), "html.parser")
        html_string = soup.find("div", id="exportacao")

        if html_string:
            for row in html_string.find_all("div", class_="row"):
                row.decompose()
            doe_span = html_string.find("span", id="texto_publicado_doe")
            if doe_span:
                doe_span.decompose()

        if html_string:
            for a_tag in html_string.find_all("a"):
                a_tag.unwrap()

        html_string = html_string.prettify() if html_string else ""

        # get text markdown
        text_markdown = await self._get_markdown(
            html_content=html_string,
        )

        return {
            "title": "Constituição Estadual",
            "year": datetime.now().year,
            "summary": "Constituição Estadual do Estado do Acre",
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": document_url,
        }

    async def _prefetch_all_links(self) -> None:
        """Fetch all document links from the single page and group by year.

        The Legis AC website loads all documents on a single page.
        This method extracts links for all norm types and buckets them
        into ``_prefetched_docs`` by year for year-sequential processing.
        """
        url = self._format_search_url(1)
        soup = await self.request_service.get_soup(url)
        if soup is None:
            logger.error("Failed to fetch Acre main page")
            return

        for norm_type, norm_type_id in self.types.items():
            if norm_type == "Constituição Estadual":
                continue

            html_links = await self._get_docs_links(soup, norm_type_id)
            count = 0
            for doc in html_links:
                year = int(doc["year"])
                self._prefetched_docs[year][norm_type].append(doc)
                count += 1

            if self.verbose:
                logger.info(f"Prefetched {count} links for {norm_type}")

    async def _scrape_type(
        self, norm_type: str, norm_type_id: str, year: int
    ) -> list[dict]:
        """Scrape all documents of a single type for a year."""
        situation = self.situations[0]

        if norm_type == "Constituição Estadual":
            # Scrape once on the first year that reaches here
            if getattr(self, "_scraped_constitution", False):
                return []
            self._scraped_constitution = True
            doc_info = await self._get_state_constitution(norm_type_id)
            doc_info["situation"] = situation
            doc_info["type"] = norm_type
            return [doc_info]

        docs = self._prefetched_docs.get(year, {}).get(norm_type, [])
        if not docs:
            return []

        tasks = [self._get_doc_data(doc) for doc in docs]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": situation},
            desc=f"ACRE | {norm_type} | Year {year}",
        )

        results = []
        for result in valid_results:
            queue_item = {
                "situation": situation,
                "type": norm_type,
                **result,
            }
            results.append(queue_item)

        return results

    async def scrape(self) -> list:
        """Scrape data from all years.

        Prefetches all document links from the single page, groups them
        by year, then delegates to BaseScraper.scrape() for year-sequential
        processing with resumability.
        """
        await self._prefetch_all_links()

        all_years = set(self._prefetched_docs.keys())
        if all_years:
            self.years = sorted(
                y for y in all_years if self.year_start <= y <= self.year_end
            )

        return await super().scrape()
