from collections import defaultdict
from typing import Optional
from urllib.parse import urljoin, urlencode

import re
from datetime import datetime
from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.scraper import BaseScraper, STATE_LEGISLATION_SAVE_DIR, YEAR_START

TYPES = {
    "Ato Deliberativo": "ato-deliberativo",
    "Ato Normativo": "ato-normativo",
    "Decreto Legislativo": "decleg/decleg.htm",  # HTML index, same scraping logic as Emenda/Lei Complementar
    "Emenda Constitucional": "legislacao5/const_e/ement.htm",  # lei complementar, lei ordinaria and emenda constitucional share the same scraping logic
    "Lei Complementar": "ementario/lc.htm",
    "Lei Ordinária": "lei_ordinaria.htm",
    "Resolução": "resolucao",  # ato normativo, ato deliberativo and resolução share the same scraping logic
}


VALID_SITUATIONS = [
    "Não consta"
]  # Alece does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no lon

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class CearaAleceScraper(BaseScraper):
    """Webscraper for Ceara state legislation website (https://www.al.ce.gov.br/)

    Example search request: https://www.al.ce.gov.br/legislativo/leis-e-normativos-internos?categoria=ato-normativo&page=1
    """

    PAGINATED_TYPES = {"Ato Deliberativo", "Ato Normativo", "Resolução"}
    STATIC_INDEX_TYPES = {
        "Emenda Constitucional",
        "Lei Complementar",
        "Decreto Legislativo",
    }

    def __init__(
        self,
        base_url: str = "https://www.al.ce.gov.br/legislativo",
        **kwargs,
    ):
        if STATE_LEGISLATION_SAVE_DIR:
            kwargs.setdefault("docs_save_dir", STATE_LEGISLATION_SAVE_DIR)
        super().__init__(
            base_url, name="CEARA", types=TYPES, situations=SITUATIONS, **kwargs
        )
        self.params = {
            "categoria": "",
            "page": 1,
        }
        # {year: {norm_type: [doc_info, ...]}} — populated before year iteration
        self._prefetched_docs: dict[int, dict[str, list]] = defaultdict(
            lambda: defaultdict(list)
        )

    def _format_search_url(self, norm_type_id: str, page: int) -> str:
        """Format url for search request"""
        self.params["categoria"] = norm_type_id
        self.params["page"] = page
        return f"{self.base_url}/leis-e-normativos-internos?{urlencode(self.params)}"

    async def _get_docs_links(self, norm_type: str, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'year', 'norm_number', 'summary', 'document_url'
        """

        soup = await self.request_service.get_soup(url, timeout=60)
        docs = []

        # check if the page is empty
        empty_tag = soup.find("p", class_="mt-5")
        if empty_tag and "nenhum dado localizado" in empty_tag.text.lower():
            return []

        # there may be 2 tables in the page, we want the second one
        tables = soup.find_all("table")
        if len(tables) < 2:
            table = tables[0]
        else:
            table = tables[1]
        items = table.find_all("tr")
        for item in items:
            tds = item.find_all("td")
            if len(tds) != 6:
                continue

            norm_number = tds[0].text.strip()
            year = norm_number.split("/")[1]
            title = norm_number
            summary = tds[2].text.strip()
            document_url = tds[5].find("a")["href"]
            docs.append(
                {
                    "title": f"{norm_type} {title}",
                    "year": year,
                    "norm_number": norm_number,
                    "summary": summary,
                    "document_url": document_url,
                }
            )

        return docs

    async def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given document dict"""
        # Use the /visualizar HTML page to avoid needing OCR on the PDF download.
        visualizar_url = doc_info["document_url"].rstrip("/") + "/visualizar"
        soup = await self.request_service.get_soup(visualizar_url)

        content = soup.find("div", class_="card-body") or soup.find("main") or soup
        for img in content.find_all("img"):
            img.decompose()

        text_markdown = await self._get_markdown(html_content=str(content))
        doc_info["text_markdown"] = text_markdown

        return doc_info

    async def _prefetch_paginated_links(self, resume_from: int) -> None:
        """Fetch all document links for paginated categories and group by year.

        The ALECE website does not support server-side year filtering for
        Ato Deliberativo, Ato Normativo, and Resolução. This method fetches
        all pages up-front and buckets documents into ``_prefetched_docs``
        so ``_scrape_type`` can process them year-by-year.
        """
        for norm_type in self.PAGINATED_TYPES:
            norm_type_id = self.types[norm_type]
            url = self._format_search_url(norm_type_id, 1)
            soup = await self.request_service.get_soup(url, timeout=60)

            pagination = soup.find("ul", class_="pagination")
            if pagination:
                pages = pagination.find_all("li")
                last_page = pages[-2].find("a")["href"]
                total_pages = int(last_page.split("page=")[-1])
            else:
                total_pages = 1

            tasks = [
                self._get_docs_links(
                    norm_type,
                    self._format_search_url(norm_type_id, page),
                )
                for page in range(1, total_pages + 1)
            ]
            valid_results = await self._gather_results(
                tasks,
                context={"year": "NA", "type": norm_type, "situation": "NA"},
                desc=f"CEARA | {norm_type} | prefetch_links",
            )

            count = 0
            for result in valid_results:
                for doc in result:
                    year = int(doc["year"])
                    if year < resume_from:
                        continue
                    self._prefetched_docs[year][norm_type].append(doc)
                    count += 1

            if self.verbose:
                logger.info(
                    f"Prefetched {count} links for {norm_type} (pages: {total_pages})"
                )

    async def _fetch_lei_ordinaria_years(self) -> list[int]:
        """Return sorted list of available years for Lei Ordinária."""
        norm_type_id = self.types["Lei Ordinária"]
        url = f"https://www2.al.ce.gov.br/legislativo/{norm_type_id}"
        soup = await self.request_service.get_soup(url)
        table = soup.find("table", {"class": "MsoNormalTable"})
        rows = table.find_all("tr")
        years = []
        for row in rows[1:]:
            for td in row.find_all("td"):
                a = td.find("a")
                if a:
                    years.append(int(a.text))
        years.sort()
        return years

    async def _scrape_paginated_type(self, norm_type: str, year: int) -> list[dict]:
        """Scrape documents for a paginated category in a given year using prefetched links."""
        docs = self._prefetched_docs.get(year, {}).get(norm_type, [])
        if not docs:
            return []

        situation = self.situations[0]

        tasks = [self._get_doc_data(doc) for doc in docs]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": situation},
            desc=f"CEARA | {norm_type} | Year {year}",
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

    async def _scrape_type(
        self, norm_type: str, norm_type_id: str, year: int
    ) -> list[dict]:
        """Scrape all documents of a single type for a year."""
        if norm_type in self.PAGINATED_TYPES:
            return await self._scrape_paginated_type(norm_type, year)

        situation = self.situations[0]

        if norm_type == "Lei Ordinária":
            if year not in self._lei_ordinaria_years:
                return []
            return await self._scrape_laws_constitution_amendments(
                situation, norm_type, norm_type_id, year
            )

        if norm_type in self.STATIC_INDEX_TYPES:
            # These are single-page indexes without year filtering.
            # Only scrape once (on the first year that reaches here).
            cache_key = f"_scraped_{norm_type}"
            if getattr(self, cache_key, False):
                return []
            setattr(self, cache_key, True)
            results = await self._scrape_laws_constitution_amendments(
                situation, norm_type, norm_type_id
            )
            # Filter to year range since the index page returns all years
            year_set = set(self.years)
            return [r for r in results if r.get("year") in year_set]

        return []

    async def _get_laws_constitution_amendments_docs_links(
        self, url: str, norm_type: str
    ) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'year', 'summary', 'html_link'
        """
        soup = await self.request_service.get_soup(url)
        docs = []

        table = soup.find_all("table")
        if len(table) > 1:
            table = table[1]
        else:
            table = table[0]
        items = table.find_all("tr")
        for index in range(len(items)):
            # skip first row since it's the header
            if index == 0:
                continue

            item = items[index]

            tds = item.find_all("td")

            # for leis ordinarias, the table has at most 3 columns
            if norm_type == "Lei Ordinária" and len(tds) > 3:
                continue

            title = tds[0].text.strip()

            try:  # some documents are not available, so we skip them
                summary = tds[1].text.strip()
                html_link = tds[0].find("a")["href"]
                # remove "../" from html_link
                html_link = html_link.replace("../", "")
            except Exception:
                logger.debug(f"No link found for document '{title}' — skipping")
                continue

            # don't need for lei ordinaria
            year = None
            if norm_type != "Lei Ordinária":
                # regex to get year part directly (FORMATs: "DE 20.10.09", "DE 20.10.2009",
                # "DE 06/03/25", "DE 06/03/2009", "N° 468, 2.06.2010", "DE 1°.07.2021")
                year_match = re.search(
                    r"[\s,]\d{1,2}°?[./]\d{1,2}[./](\d{2}|\d{4})\b", title
                )
                if not year_match:
                    # fallback: any 4-digit year in valid century range
                    year_match = re.search(r"\b((?:19|20)\d{2})\b", title)
                if not year_match:
                    logger.warning(
                        f"Could not extract year from title '{title[:80]}' — skipping"
                    )
                    continue
                year_text = year_match.group(1)

                if len(year_text) == 2:
                    year_now = datetime.now().year
                    year_text = f"20{year_text}"
                    if int(year_text) > year_now:
                        year_text = f"19{year_text[2:]}"

                year = int(year_text)

            docs.append(
                {
                    "title": title,
                    "year": year,
                    "summary": summary,
                    "html_link": html_link,
                }
            )

        return docs

    def construct_url(self, norm_type: str, html_link: str, year: Optional[int]) -> str:
        """Construct the full url for the document page"""
        if "https://" in html_link:
            # don't need to do anything, it's already a full url
            return html_link

        # file:///\\10.85.100.8\10.85.100.8\legislativo\legislacao5\leis2014\15517.htm.
        if "file://" in html_link:
            # get content after \legislacao5\
            html_link = html_link.split("legislacao5")[-1].replace("\\", "/")
            if html_link.startswith("/"):
                html_link = html_link[1:]

            base_url = "https://www2.al.ce.gov.br/legislativo/legislacao5/"
            return urljoin(base_url, html_link)

        if norm_type == "Lei Complementar":
            base_url = "https://www2.al.ce.gov.br/legislativo/"
            if "ementario" not in html_link:
                html_link = f"ementario/{html_link}"
        elif norm_type == "Decreto Legislativo":
            base_url = "https://www2.al.ce.gov.br/legislativo/decleg/"
        else:
            base_url = "https://www2.al.ce.gov.br/legislativo/legislacao5/"
            if norm_type == "Lei Ordinária":
                if "leis" not in html_link:
                    year = year if year >= 2000 else str(year)[2:]

                    html_link = f"leis{year}/{html_link}"

                elif (
                    "legislacao5" in html_link
                    or "legislativo/legislacao5/" in html_link
                    or "/legislativo/legislacao5/" in html_link
                ):
                    html_link = (
                        html_link.replace("/legislativo/legislacao5/", "")
                        .replace("legislativo/legislacao5/", "")
                        .replace("legislacao5/", "")
                    )

        return urljoin(base_url, html_link)

    async def _get_laws_constitution_amendments_doc_data(
        self, doc_info: dict, norm_type: str, year: Optional[int] = None
    ) -> Optional[dict]:
        """Get document data from given document dict"""
        # html_link will be a link to the document page

        html_link = doc_info.pop("html_link")
        url = self.construct_url(norm_type, html_link, year)

        response = await self.request_service.make_request(url)

        if not response or response.status == 404:
            logger.error(
                f"Error fetching document page: {url}. Year: {year}. HTML link: {html_link}"
            )
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year=year or "",
                situation="",
                norm_type=norm_type,
                html_link=url,
                error_message="Failed to fetch document page",
            )
            return None

        soup = BeautifulSoup(await response.read(), "html.parser")

        # Remove VOLTAR nav link (structure varies: <a><b>VOLTAR</b></a>, <a><span>VOLTAR</span></a>, etc.)
        for a in soup.find_all("a"):
            if a.get_text(strip=True).upper() == "VOLTAR":
                a.decompose()

        # Remove editorial disclaimer "O texto desta Lei não substitui o publicado no Diário Oficial"
        # (text may contain \r\n linebreaks, so match on the opening phrase only)
        for node in soup.find_all(
            string=re.compile(r"O texto desta Lei", re.IGNORECASE)
        ):
            node.parent.decompose()

        # Word-generated HTML docs wrap all content in <div class="Section1">.
        # Extracting just that div avoids markitdown getting confused by MSO namespace bloat.
        section1 = soup.find("div", class_="Section1")
        content = section1 if section1 else soup
        html_string = str(content).strip()

        # check if invalid document
        if "NÄO EXISTE LEI COM ESTE NÚMERO".lower() in html_string.lower():
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year=year or "",
                situation="",
                norm_type=norm_type,
                html_link=url,
                error_message="Invalid document (NÄO EXISTE LEI COM ESTE NÚMERO)",
            )
            return None

        # Use direct HTML content conversion
        text_markdown = await self._get_markdown(html_content=html_string)

        if not text_markdown:
            logger.error(f"Error converting document to markdown: {url}. Year: {year}")
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year=year or "",
                situation="",
                norm_type=norm_type,
                html_link=url,
                error_message="Markdown conversion returned empty result",
            )
            return None

        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url

        return doc_info

    async def _scrape_laws_constitution_amendments(
        self, situation: str, norm_type: str, norm_type_id: str, year: int = None
    ):
        """Scrape constitution amendments"""
        # for laws and constitution amendments we need to scrape a different page

        if year is not None:  # norm_type is ordinary laws
            # if year >= 2021 link will be in the format: legislacao5/leis{year}/LEIS{year}.htm
            if year >= 2021:
                url = f"https://www2.al.ce.gov.br/legislativo/legislacao5/leis{year}/LEIS{year}.htm"

            # if year < 2000, url will be like leis91/e91.htm
            elif year >= 2000:
                url = f"https://www2.al.ce.gov.br/legislativo/legislacao5/leis{year}/e{year}.htm"
            else:
                year2_digits = str(year)[2:]
                url = f"https://www2.al.ce.gov.br/legislativo/legislacao5/leis{year2_digits}/e{year2_digits}.htm"

        else:
            url = f"https://www2.al.ce.gov.br/legislativo/{norm_type_id}"

        docs = await self._get_laws_constitution_amendments_docs_links(url, norm_type)

        tasks = [
            self._get_laws_constitution_amendments_doc_data(
                doc,
                norm_type,
                year,
            )
            for doc in docs
        ]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": situation},
            desc=f"CEARA | {norm_type}",
        )
        results = []
        for result in valid_results:
            # prepare item for saving
            queue_item = {
                # hardcode since we only get valid documents in search request
                "situation": situation,
                "type": norm_type,
                **result,
            }

            if queue_item["year"] is None:
                queue_item["year"] = year

            results.append(queue_item)

        if self.verbose:
            logger.info(
                f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
            )

        return results

    async def scrape(self) -> list:
        """Scrape data from all years.

        Prefetches all document links for paginated categories, groups them
        by year, then delegates to BaseScraper.scrape() for year-sequential
        processing with resumability.
        """
        # Determine resume point early so prefetch can skip old years
        resume_from = self.year_start
        forced_resume = self.year_start != YEAR_START
        if self.saver and self.saver.last_year is not None and not forced_resume:
            resume_from = int(self.saver.last_year)

        # Prefetch links for paginated categories (grouped by year)
        await self._prefetch_paginated_links(resume_from)

        # Fetch available Lei Ordinária years
        self._lei_ordinaria_years: set[int] = set(
            await self._fetch_lei_ordinaria_years()
        )

        # Build the set of years that actually have data, bounded by year range
        all_years = set(self._prefetched_docs.keys()) | self._lei_ordinaria_years
        if all_years:
            self.years = sorted(
                y for y in all_years if self.year_start <= y <= self.year_end
            )

        return await super().scrape()
