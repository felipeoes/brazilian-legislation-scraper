"""Santa Catarina state legislation scraper.

Scrapes the ALESC legislation system at ``leis.alesc.sc.gov.br``.
The site is a NextJS app with server-rendered HTML.

Search: GET ``/legislativo?ano={year}&page={page}`` (legislative acts)
        GET ``/executivo?ano={year}&page={page}`` (executive decrees)
Document: GET ``/ato-normativo/{path}/{id}`` returns full HTML content.
"""

import re
from collections.abc import AsyncIterator
from urllib.parse import urljoin

from src.core.convert import strip_html_chrome
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

_RE_TYPE_FROM_TITLE = re.compile(
    "^(LEI COMPLEMENTAR|LEI|DECRETO LEGISLATIVO|DECRETO-LEI|DECRETO|DEC-|EMENDA CONSTITUCIONAL|RESOLUÇÃO|RES-|PORTARIA|PRT-|INSTRUÇÃO NORMATIVA)",
    re.IGNORECASE,
)
_ABBREV_TO_TYPE = {"DEC-": "Decreto", "RES-": "Resolução", "PRT-": "Portaria"}
TYPES = {"Legislativo": "legislativo", "Executivo": "executivo"}
SITUATIONS: list[str] = []


class SantaCatarinaScraper(BaseScraper):
    """Scraper for Santa Catarina legislation (leis.alesc.sc.gov.br).

    Year start (earliest on source): 1946
    """

    types = TYPES
    situations = SITUATIONS
    name = "SANTA_CATARINA"
    base_url = "https://leis.alesc.sc.gov.br"

    def _format_search_url(self, norm_type_id: str, year: int) -> str:
        return f"{self.base_url}/{norm_type_id}?ano={year}"

    async def fetch(self, doc_info: dict) -> Document | None:
        """Fetch document HTML and convert to markdown."""
        document_url = doc_info.get("document_url", "")
        title = doc_info.get("title", "")
        if not document_url:
            return None
        try:
            soup, mhtml = await self.browser.snapshot(document_url)
        except Exception as exc:
            raise ValueError(f"Failed to fetch document page: {exc}")
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
                "(.+?)(?:\\s+(?:n[º°o]|n\\.|número)\\s*\\d|\\s+\\d)",
                normalized_title,
                re.IGNORECASE,
            )
            if prefix_match:
                norm_type = prefix_match.group(1).strip()
        doc_info["type"] = norm_type
        doc_info["situation"] = "Não consta"
        return await self.fetch_html(doc_info, html_content, document_url, mhtml)

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        for norm_type, path in self.types.items():
            seen = set()
            page = 1
            while True:
                soup = (
                    await self.http.request(
                        f"{self.base_url}/{path}?ano={year}&page={page}"
                    )
                ).soup()
                links = soup.find_all("a", href=re.compile("/ato-normativo/"))
                new_links = [link for link in links if link.get("href") not in seen]
                if not new_links:
                    break
                for link in new_links:
                    seen.add(link["href"])
                    title = link.get_text(strip=True)
                    if title:
                        yield {
                            "year": year,
                            "type": norm_type,
                            "title": title,
                            "document_url": urljoin(self.base_url, link["href"]),
                            "situation": DEFAULT_VALID_SITUATION,
                        }
                page += 1
