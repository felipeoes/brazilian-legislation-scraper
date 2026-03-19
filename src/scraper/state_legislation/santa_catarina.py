"""Santa Catarina state legislation scraper.

Scrapes the ALESC legislation system at ``leis.alesc.sc.gov.br``.
The site is a NextJS app with server-rendered HTML.

Search: GET ``/legislativo?ano={year}&page={page}`` (legislative acts)
        GET ``/executivo?ano={year}&page={page}`` (executive decrees)
Document: GET ``/ato-normativo/{path}/{id}`` returns full HTML content.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument

import re
from typing import Any
from urllib.parse import urljoin

from src.scraper.base.converter import strip_html_chrome
from src.scraper.base.scraper import StateScraper

_RE_TYPE_FROM_TITLE = re.compile(
    r"^(LEI COMPLEMENTAR|LEI|DECRETO LEGISLATIVO|DECRETO-LEI|DECRETO|DEC-|"
    r"EMENDA CONSTITUCIONAL|RESOLUÇÃO|RES-|PORTARIA|PRT-|INSTRUÇÃO NORMATIVA)",
    re.IGNORECASE,
)

_ABBREV_TO_TYPE = {
    "DEC-": "Decreto",
    "RES-": "Resolução",
    "PRT-": "Portaria",
}

TYPES = {
    "Legislativo": "legislativo",
    "Executivo": "executivo",
}

SITUATIONS: list[str] = []


class SantaCatarinaScraper(StateScraper):
    """Scraper for Santa Catarina legislation (leis.alesc.sc.gov.br).

    Year start (earliest on source): 1946
    """

    def __init__(
        self,
        base_url: str = "https://leis.alesc.sc.gov.br",
        **kwargs: Any,
    ):
        super().__init__(
            base_url,
            types=TYPES,
            situations=SITUATIONS,
            name="SANTA_CATARINA",
            **kwargs,
        )

    def _format_search_url(self, norm_type_id: str, year: int) -> str:
        return f"{self.base_url}/{norm_type_id}?ano={year}"

    async def _get_docs_links(self, norm_type_id: str, year: int) -> list:
        """Paginate through all search pages and collect document links."""
        all_docs: list[dict] = []
        seen_hrefs: set[str] = set()
        page = 1
        max_empty = 1
        empty_streak = 0

        while True:
            url = f"{self.base_url}/{norm_type_id}?ano={year}&page={page}"
            soup = await self.request_service.get_soup(url)

            if not soup:
                break

            links = soup.find_all("a", href=re.compile(r"/ato-normativo/"))
            new_docs = 0
            for link in links:
                href = link.get("href", "")
                if href in seen_hrefs:
                    continue
                seen_hrefs.add(href)

                title = link.get_text(strip=True)
                if not title:
                    continue

                ementa = ""
                card = link.find_parent("div", class_=re.compile("card"))
                if card:
                    ementa_div = card.find("div", class_=re.compile("ementa|title"))
                    if ementa_div and ementa_div.get_text(strip=True) != title:
                        ementa = ementa_div.get_text(strip=True)

                doc_url = urljoin(self.base_url, href)
                all_docs.append(
                    {
                        "title": title,
                        "summary": ementa,
                        "document_url": doc_url,
                    }
                )
                new_docs += 1

            if new_docs == 0:
                empty_streak += 1
                if empty_streak >= max_empty:
                    break
            else:
                empty_streak = 0

            page += 1

        return all_docs

    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        """Fetch document HTML and convert to markdown."""
        document_url = doc_info.get("document_url", "")
        title = doc_info.get("title", "")

        if not document_url:
            return None

        if self._is_already_scraped(document_url, title):
            return None

        try:
            soup, mhtml = await self._fetch_soup_and_mhtml(document_url)
        except Exception as exc:
            await self._save_doc_error(
                title=title,
                year=doc_info.get("year", ""),
                html_link=document_url,
                error_message=f"Failed to fetch document page: {exc}",
            )
            return None

        strip_html_chrome(soup)
        html_content = str(soup)

        normalized_title = " ".join(title.split())
        norm_type = normalized_title
        m = _RE_TYPE_FROM_TITLE.match(title)
        if m:
            matched = m.group(1)
            norm_type = _ABBREV_TO_TYPE.get(matched.upper(), matched.title())
        else:
            prefix_match = re.match(
                r"(.+?)(?:\s+(?:n[º°o]|n\.|número)\s*\d|\s+\d)",
                normalized_title,
                re.IGNORECASE,
            )
            if prefix_match:
                norm_type = prefix_match.group(1).strip()

        doc_info["type"] = norm_type
        doc_info["situation"] = "Não consta"
        return await self._process_html_doc(doc_info, html_content, document_url, mhtml)

    async def _scrape_type(
        self, norm_type: str, norm_type_id: str, year: int
    ) -> list[dict]:
        """Scrape documents for a given search path and year."""
        documents = await self._get_docs_links(norm_type_id, year)

        if not documents:
            return []

        for doc in documents:
            doc["year"] = year
        return await self._process_documents(
            documents,
            year=year,
            norm_type=norm_type,
        )
