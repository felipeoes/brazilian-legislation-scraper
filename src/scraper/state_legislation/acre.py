from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument
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

    # legis.ac.gov.br has slow-loading resources; use domcontentloaded
    # to avoid waiting for scripts/fonts that may never finish.
    _mhtml_wait_until = "domcontentloaded"

    def __init__(
        self,
        base_url: str = "https://legis.ac.gov.br/principal",
        **kwargs,
    ):
        # legis.ac.gov.br can't handle many concurrent browser connections;
        # keeping max_workers low prevents MHTML capture timeouts (30s).
        # kwargs.setdefault("max_workers", 5)
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
        # Remove header with brasão (constitution page)
        topo = html_tag.find("div", class_="topo-lei")
        if topo:
            topo.decompose()
        # Remove the ementa metadata table (first table with an empty leading cell)
        first_table = html_tag.find("table")
        if first_table:
            first_td = first_table.find("td")
            if first_td and not first_td.get_text(strip=True):
                first_table.decompose()

        # Reuse base helpers: unwrap links and remove disclaimer notices
        self._clean_norm_soup(html_tag, unwrap_links=True, remove_disclaimers=True)

        # Use str() instead of prettify() to preserve inline flow
        # (prettify adds newlines between siblings, breaking inline text)
        return str(html_tag)

    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        """Get document data from given html link"""
        doc_html_link = doc_info["html_link"]
        doc_title = doc_info["title"]
        doc_year = doc_info["year"]

        if self._is_already_scraped(doc_html_link, doc_title):
            return None

        try:
            soup, mhtml = await self._fetch_soup_and_mhtml(doc_html_link)
        except Exception as exc:
            await self._save_doc_error(
                title=doc_title,
                year=doc_year,
                html_link=doc_html_link,
                error_message=f"Failed to fetch document page: {exc}",
            )
            return None

        html_string = self._clean_acre_html(soup)

        doc_data = {k: v for k, v in doc_info.items() if k != "html_link"}
        return await self._process_html_doc(doc_data, html_string, doc_html_link, mhtml)

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

            logger.debug(f"Prefetched {len(html_links)} links for {norm_type}")

    async def _scrape_type(
        self, norm_type: str, norm_type_id: str, year: int
    ) -> list[dict]:
        """Scrape all documents of a single type for a year."""
        situation = self.default_situation

        if norm_type == "Constituição Estadual":
            # Scrape once on the first year that reaches here
            if getattr(self, "_scraped_constitution", False):
                return []
            self._scraped_constitution = True
            document_url = f"{self.base_url.replace('/principal', '')}/{norm_type_id}"

            try:
                soup, mhtml = await self._fetch_soup_and_mhtml(document_url)
            except Exception as exc:
                await self._save_doc_error(
                    title="Constituição Estadual",
                    year=year,
                    html_link=document_url,
                    error_message=f"Failed to fetch constitution: {exc}",
                )
                return []

            html_string = self._clean_acre_html(soup)
            if not html_string:
                return []

            doc_data = {
                "year": datetime.now().year,
                "type": "Constituição Estadual",
                "situation": self.default_situation,
                "summary": "Constituição Estadual do Estado do Acre",
            }
            doc = await self._process_html_doc(
                doc_data, html_string, document_url, mhtml
            )
            if doc:
                doc.title = "Constituição Estadual"
                saved = await self._save_doc_result(doc)
                if saved is not None:
                    # Update doc from saved dict if needed, but ScrapedDocument is preferred
                    pass
                self._track_results([doc.model_dump()])
                self.count += 1
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

    async def scrape(self) -> int:
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
