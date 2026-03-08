import re

from collections import defaultdict
from datetime import datetime
from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.scraper import StateScraper

TYPES = {
    "Lei Ordinária": "lei_ordinarias",
    "Lei Complementar": "lei_complementares",
    "Constituição Estadual": "detalhar_constituicao",  # texto completo, handled separately
    "Decreto": "lei_decretos",
    "Emenda Constitucional": "emendas",
}

SITUATIONS = {"Não consta revogação expressa": "Não consta revogação expressa"}


class AcreLegisScraper(StateScraper):
    """Webscraper for Legis - Acre website (https://legis.ac.gov.br)

    Year start (earliest on source): 1963

    Example search request: https://legis.ac.gov.br/principal/1
    """

    def __init__(
        self,
        base_url: str = "https://legis.ac.gov.br/principal",
        **kwargs,
    ):
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
            m = self.year_regex.search(title.split(",")[-1])
            year = m.group() if m else "0000"
            html_links.append(
                {
                    "title": title,
                    "year": year,
                    "summary": summary,
                    "html_link": html_link,
                }
            )

        return html_links

    def _clean_acre_html(self, soup: "BeautifulSoup") -> str:
        """Extract and clean HTML content from an Acre legislation page."""
        html_tag = soup.find("div", id="body-law") or soup.find("div", id="exportacao")
        if not html_tag:
            return ""

        for row in html_tag.find_all("div", class_="row"):
            row.decompose()
        doe_span = html_tag.find("span", id="texto_publicado_doe")
        if doe_span:
            doe_span.decompose()

        # Reuse base helpers: unwrap links and remove disclaimer notices
        self._clean_norm_soup(html_tag, unwrap_links=True, remove_disclaimers=True)

        return html_tag.prettify()

    async def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given html link"""
        doc_html_link = doc_info["html_link"]
        doc_title = doc_info["title"]
        doc_year = doc_info["year"]

        if self._is_already_scraped(doc_html_link, doc_title):
            return None

        response = await self.request_service.make_request(doc_html_link)
        if not response:
            await self._save_doc_error(
                title=doc_title,
                year=doc_year,
                html_link=doc_html_link,
                error_message="Failed to fetch document page",
            )
            return None

        soup = BeautifulSoup(await response.text(), "html.parser")
        html_string = self._clean_acre_html(soup)

        doc_data = {k: v for k, v in doc_info.items() if k != "html_link"}
        return await self._process_html_doc(doc_data, html_string, doc_html_link)

    async def _prefetch_all_links(self) -> None:
        """Fetch all document links from the single page and group by year.

        The Legis AC website loads all documents on a single page.
        This method extracts links for all norm types and buckets them
        into ``_prefetched_docs`` by year for year-sequential processing.
        """
        url = self._format_search_url(1)
        soup = await self.request_service.get_soup(url)
        if not soup:
            logger.error("Failed to fetch Acre main page")
            return

        for norm_type, norm_type_id in self.types.items():
            if norm_type == "Constituição Estadual":
                continue

            html_links = await self._get_docs_links(soup, norm_type_id)
            for doc in html_links:
                year = int(doc["year"])
                self._prefetched_docs[year][norm_type].append(doc)

            if self.verbose:
                logger.info(f"Prefetched {len(html_links)} links for {norm_type}")

    async def _scrape_type(
        self, norm_type: str, norm_type_id: str, year: int
    ) -> list[dict]:
        """Scrape all documents of a single type for a year."""
        situation = next(iter(self.situations), "Não consta")

        if norm_type == "Constituição Estadual":
            # Scrape once on the first year that reaches here
            if getattr(self, "_scraped_constitution", False):
                return []
            self._scraped_constitution = True
            document_url = f"{self.base_url.replace('/principal', '')}/{norm_type_id}"
            doc = await self._fetch_and_save_constitution(
                url=document_url,
                title="Constituição Estadual",
                year=datetime.now().year,
                summary="Constituição Estadual do Estado do Acre",
            )
            return [doc] if doc else []

        docs = self._prefetched_docs.get(year, {}).get(norm_type, [])
        if not docs:
            return []

        return await self._process_documents(
            docs,
            year=year,
            norm_type=norm_type,
            situation=situation,
            desc=f"ACRE | {norm_type} | Year {year}",
        )

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
