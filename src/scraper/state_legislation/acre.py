import re
import time

from datetime import datetime
from bs4 import BeautifulSoup
from loguru import logger
from tqdm import tqdm
from src.scraper.base.scraper import BaseScraper

TYPES = {
    "Lei Ordinária": "lei_ordinarias",
    "Lei Complementar": "lei_complementares",
    "Constituição Estadual": "detalhar_constituicao",  # texto completo, modificar a lógica no scraper
    "Decreto": "lei_decretos",
}

VALID_SITUATIONS = [
    "Não consta revogação expressa",
]  # Legis - Acre only publishes norms that are currently valid (no explicit revocation)

INVALID_SITUATIONS = (
    []
)  # norms with these situations are invalid norms (no longer have legal effect)

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
        from src.scraper.base.scraper import STATE_LEGISLATION_SAVE_DIR

        if STATE_LEGISLATION_SAVE_DIR:
            kwargs.setdefault("docs_save_dir", STATE_LEGISLATION_SAVE_DIR)
        super().__init__(
            base_url, name="ACRE", types=TYPES, situations=SITUATIONS, **kwargs
        )
        self.year_regex = re.compile(r"\d{4}")

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

        html_string = html_string.prettify() if html_string else ""

        # get text markdown from extracted HTML
        text_markdown = await self._get_markdown(
            html_content=html_string, remove_hyperlinks=True
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

        html_string = html_string.prettify() if html_string else ""

        # get text markdown
        text_markdown = await self._get_markdown(
            html_content=html_string, remove_hyperlinks=True
        )

        return {
            "title": "Constituição Estadual",
            "year": datetime.now().year,
            "summary": "Constituição Estadual do Estado do Acre",
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": document_url,
        }

    async def _scrape_situation_type(
        self, situation: str, norm_type: str, norm_type_id: str
    ) -> list:
        """Scrape norms for a specific situation and type"""
        # if it's state constitution, we need to change logic. All the text is within div class="exportacao"
        if norm_type == "Constituição Estadual":
            doc_info = await self._get_state_constitution(norm_type_id)
            doc_info["situation"] = situation
            doc_info["type"] = norm_type
            return [doc_info]

        url = self._format_search_url(1)
        soup = await self.request_service.get_soup(url)
        if soup is None:
            return []

        html_links = await self._get_docs_links(soup, norm_type_id)
        results = []

        # Get data from all documents text links using asyncio.gather with progress tracking
        tasks = [self._get_doc_data(doc) for doc in html_links]

        valid_results = await self._gather_results(
            tasks,
            context={"year": "NA", "type": norm_type, "situation": situation},
            desc=f"ACRE | {norm_type}",
        )

        for result in valid_results:
            # prepare item for saving
            queue_item = {
                # "year": year, # getting year from document title because Legis does not have a search by year
                # website only shows documents without any revocation
                "situation": situation,
                "type": norm_type,
                **result,
            }
            results.append(queue_item)

        if self.verbose:
            logger.info(
                f"Type: {norm_type} | Situation: {situation} | Total: {len(results)}"
            )

        return results

    async def scrape(self):
        """Scrape norms"""

        self._scrape_start = time.time()

        # Collect all tasks for concurrent execution with progress tracking
        task_configs = [
            (situation, norm_type, norm_type_id)
            for situation in self.situations
            for norm_type, norm_type_id in self.types.items()
        ]

        all_results = []
        for situation, norm_type, norm_type_id in tqdm(task_configs, desc="ACRE"):
            results = await self._scrape_situation_type(
                situation, norm_type, norm_type_id
            )
            if results:
                await self.saver.save(results)
                all_results.extend(results)

        self.results = all_results
        self.count = len(all_results)
        await self._save_summary()
        return self.results
