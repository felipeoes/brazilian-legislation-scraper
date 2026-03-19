from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument
import asyncio
import re
from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.scraper.base.converter import valid_markdown
from src.scraper.base.scraper import StateScraper
from src.services.browser.playwright import BrowserService

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument


TYPES = {
    "Ato Deliberativo": "ato-deliberativo",
    "Ato Normativo": "ato-normativo",
    "Decreto Legislativo": "decleg/decleg.htm",  # HTML index, same scraping logic as Emenda/Lei Complementar
    "Emenda Constitucional": "legislacao5/const_e/ement.htm",  # lei complementar, lei ordinaria and emenda constitucional share the same scraping logic
    "Lei Complementar": "ementario/lc.htm",
    "Lei Ordinária": "lei_ordinaria.htm",
    "Resolução": "resolucao",  # ato normativo, ato deliberativo and resolução share the same scraping logic
}


SITUATIONS = {"Não consta": "Não consta"}


class CearaAleceScraper(StateScraper):
    """Webscraper for Ceara state legislation website (https://www.al.ce.gov.br/)

    Year start (earliest on source): 1968

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
        params = {**self.params, "categoria": norm_type_id, "page": page}
        return f"{self.base_url}/leis-e-normativos-internos?{urlencode(params)}"

    async def _get_docs_links(
        self, norm_type: str, url: str, soup: BeautifulSoup | None = None
    ) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'year', 'norm_number', 'summary', 'document_url'
        """

        if soup is None:
            soup = await self.request_service.get_soup(url, timeout=60)
        if not soup:
            return []
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

    def _remove_summary_element(
        self, container: BeautifulSoup | Tag, summary: str
    ) -> None:
        """Remove the smallest HTML element that contains the summary text."""
        if not summary:
            return
        normalized_summary = re.sub(r"\s+", " ", summary).strip().casefold()
        if not normalized_summary:
            return

        candidates: list[tuple[int, Tag]] = []
        for tag in container.find_all(["p", "td", "div", "span", "font", "table"]):
            tag_text = re.sub(r"\s+", " ", tag.get_text(" ", strip=True))
            normalized_tag = tag_text.casefold()
            if normalized_summary not in normalized_tag:
                continue
            if "art." in tag_text.lower():
                continue
            candidates.append((len(normalized_tag), tag))

        if candidates:
            _, best = min(candidates, key=lambda x: x[0])
            best.decompose()

    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        """Get document data from given document dict"""
        # Use the /visualizar HTML page as the canonical document_url so that
        # (1) resume keys match and (2) MHTML capture navigates to an HTML page
        # instead of triggering a PDF download.
        visualizar_url = doc_info["document_url"].rstrip("/") + "/visualizar"
        doc_info["document_url"] = visualizar_url

        if self._is_already_scraped(
            doc_info["document_url"], doc_info.get("title", "")
        ):
            return None
        try:
            soup, mhtml = await self._fetch_soup_and_mhtml(visualizar_url)
        except Exception as exc:
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year=doc_info.get("year", ""),
                norm_type=doc_info.get("norm_type", ""),
                html_link=visualizar_url,
                error_message=f"Failed to fetch document page: {exc}",
            )
            return None

        content = soup.find("div", class_="card-body") or soup.find("main") or soup
        self._clean_norm_soup(content, unwrap_links=False)
        self._remove_summary_element(content, doc_info.get("summary", ""))

        html_string = str(content)
        text_markdown = await self._get_markdown(html_content=html_string)

        valid, reason = valid_markdown(text_markdown)
        if not valid:
            logger.error(f"Invalid markdown for {visualizar_url}: {reason}")
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year=doc_info.get("year", ""),
                norm_type=doc_info.get("norm_type", ""),
                html_link=visualizar_url,
                error_message=reason,
            )
            return None

        doc_info["text_markdown"] = text_markdown
        doc_info["raw_content"] = mhtml
        doc_info["content_extension"] = ".mhtml"

        return doc_info

    async def _prefetch_paginated_links(self, resume_from: int) -> None:
        """Fetch all document links for paginated categories and group by year.

        The ALECE website does not support server-side year filtering for
        Ato Deliberativo, Ato Normativo, and Resolução. This method fetches
        page 1 of all types concurrently, then fetches remaining pages in a
        single batch, and buckets documents into ``_prefetched_docs``.
        """
        # Phase 1: fetch page 1 of all types concurrently
        type_list = list(self.PAGINATED_TYPES)
        first_page_urls = {
            nt: self._format_search_url(self.types[nt], 1) for nt in type_list
        }
        first_soups = await asyncio.gather(
            *(
                self.request_service.get_soup(first_page_urls[nt], timeout=60)
                for nt in type_list
            )
        )

        # Phase 2: discover total pages and build remaining-page tasks
        remaining_tasks: list[tuple[str, object]] = []  # (norm_type, coro)
        for norm_type, soup in zip(type_list, first_soups):
            if not soup:
                continue
            norm_type_id = self.types[norm_type]

            pagination = soup.find("ul", class_="pagination")
            if pagination:
                pages = pagination.find_all("li")
                last_page = pages[-2].find("a")["href"]
                total_pages = int(last_page.split("page=")[-1])
            else:
                total_pages = 1

            # Bucket page-1 results immediately
            docs_p1 = await self._get_docs_links(
                norm_type, first_page_urls[norm_type], soup=soup
            )
            self._bucket_prefetched(docs_p1, norm_type, resume_from)

            for page in range(2, total_pages + 1):
                remaining_tasks.append(
                    (
                        norm_type,
                        self._get_docs_links(
                            norm_type,
                            self._format_search_url(norm_type_id, page),
                        ),
                    )
                )

        if remaining_tasks:
            results = await self._gather_results(
                [t[1] for t in remaining_tasks],
                context={"year": "NA", "type": "paginated", "situation": "NA"},
                desc="CEARA | paginated | prefetch_links",
            )
            for (norm_type, _), result in zip(remaining_tasks, results):
                self._bucket_prefetched(result, norm_type, resume_from)

    def _bucket_prefetched(self, docs: list, norm_type: str, resume_from: int) -> None:
        """Sort fetched document links into ``_prefetched_docs`` by year."""
        for doc in docs:
            year = int(doc["year"])
            if year < resume_from or year > self.year_end:
                continue
            self._prefetched_docs[year][norm_type].append(doc)

    async def _fetch_lei_ordinaria_years(self) -> list[int]:
        """Return sorted list of available years for Lei Ordinária."""
        norm_type_id = self.types["Lei Ordinária"]
        url = f"https://www2.al.ce.gov.br/legislativo/{norm_type_id}"
        soup = await self.request_service.get_soup(url)
        if not soup:
            return []
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

        situation = self.default_situation
        return await self._process_documents(
            docs,
            year=year,
            norm_type=norm_type,
            situation=situation,
            desc=f"CEARA | {norm_type} | Year {year}",
        )

    async def _scrape_type(
        self, norm_type: str, norm_type_id: str, year: int
    ) -> list[dict]:
        """Scrape all documents of a single type for a year."""
        if norm_type in self.PAGINATED_TYPES:
            return await self._scrape_paginated_type(norm_type, year)

        situation = self.default_situation

        if norm_type == "Lei Ordinária":
            if year not in self._lei_ordinaria_years:
                return []
            return await self._scrape_laws_constitution_amendments(
                situation, norm_type, norm_type_id, year
            )

        if norm_type in self.STATIC_INDEX_TYPES:
            docs = self._prefetched_docs.get(year, {}).get(norm_type, [])
            if not docs:
                return []
            return await self._process_documents(
                docs,
                year=year,
                norm_type=norm_type,
                situation=situation,
                doc_data_fn=self._get_laws_constitution_amendments_doc_data,
                doc_data_kwargs={"norm_type": norm_type, "year": year},
            )

        return []

    async def _get_laws_constitution_amendments_docs_links(
        self, url: str, norm_type: str
    ) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'year', 'summary', 'html_link'
        """
        soup = await self.request_service.get_soup(url)
        if not soup:
            return []
        docs = []

        table = soup.find_all("table")
        if len(table) > 1:
            table = table[1]
        else:
            table = table[0]
        items = table.find_all("tr")
        for item in items[1:]:
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

    def construct_url(self, norm_type: str, html_link: str, year: int | None) -> str:
        """Construct the full url for the document page"""
        if "http://" in html_link or "https://" in html_link:
            return html_link.replace("http://", "https://", 1)

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

    async def _browser_pdf_to_markdown(self, url: str) -> str:
        """Render a URL to PDF via the browser and convert to markdown."""
        if self._mhtml_browser is None:
            self._mhtml_browser = BrowserService(
                headless=True,
                multiple_pages=True,
                max_workers=self.max_workers,
                owner_class_name=f"{self.__class__.__name__}_mhtml",
            )
            await self._mhtml_browser.initialize()

        page = await self._mhtml_browser.get_available_page()
        try:
            await page.goto(url, wait_until="load", timeout=60_000)
            pdf_bytes = await page.pdf()
            return await self._converter.bytes_to_markdown(pdf_bytes)
        except Exception as e:
            logger.warning(f"Browser PDF fallback failed for {url}: {e}")
            return ""
        finally:
            self._mhtml_browser.release_page(page)

    async def _get_laws_constitution_amendments_doc_data(
        self, doc_info: dict, norm_type: str, year: int | None = None
    ) -> dict | None:
        """Get document data from given document dict"""
        doc_info = dict(doc_info)
        html_link = doc_info.pop("html_link")
        url = self.construct_url(norm_type, html_link, year)

        if self._is_already_scraped(url, doc_info.get("title", "")):
            return None

        try:
            soup, mhtml = await self._fetch_soup_and_mhtml(url)
        except Exception as exc:
            logger.error(
                f"Error fetching document page: {url}. Year: {year}. HTML link: {html_link}"
            )
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year=year or "",
                norm_type=norm_type,
                html_link=url,
                error_message=f"Failed to fetch document page: {exc}",
            )
            return None

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

        # Word-generated HTML docs wrap content in <div class="Section1/2/3/...">.
        # Merge all Section divs to capture full text (old docs split across multiple).
        sections = soup.find_all("div", class_=re.compile(r"^Section\d+$"))
        if sections:
            container = BeautifulSoup("<div></div>", "html.parser").div
            for sec in sections:
                for child in list(sec.children):
                    container.append(child.extract())
            content = container
        else:
            content = soup
        self._remove_summary_element(content, doc_info.get("summary", ""))
        html_string = str(content).strip()

        # check if invalid document
        if "NÄO EXISTE LEI COM ESTE NÚMERO".lower() in html_string.lower():
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year=year or "",
                norm_type=norm_type,
                html_link=url,
                error_message="Invalid document (NÄO EXISTE LEI COM ESTE NÚMERO)",
            )
            return None

        # Use direct HTML content conversion
        text_markdown = await self._get_markdown(html_content=html_string)

        valid, reason = valid_markdown(text_markdown)
        if not valid:
            # Fallback: render page to PDF via browser, then use PDF→markdown pipeline
            pdf_md = await self._browser_pdf_to_markdown(url)
            if pdf_md:
                text_markdown = pdf_md
                valid, reason = valid_markdown(text_markdown)

        if not valid:
            logger.error(f"Invalid markdown for {url} (year={year}): {reason}")
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year=year or "",
                norm_type=norm_type,
                html_link=url,
                error_message=reason,
            )
            return None

        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url
        doc_info["raw_content"] = mhtml
        doc_info["content_extension"] = ".mhtml"

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

        return await self._process_documents(
            docs,
            year=year,
            norm_type=norm_type,
            situation=situation,
            doc_data_fn=self._get_laws_constitution_amendments_doc_data,
            doc_data_kwargs={"norm_type": norm_type, "year": year},
        )

    async def _prefetch_static_index_links(self) -> None:
        """Prefetch links for static index types and bucket by year."""
        tasks = []
        type_list = []
        for norm_type in self.STATIC_INDEX_TYPES:
            norm_type_id = self.types[norm_type]
            url = f"https://www2.al.ce.gov.br/legislativo/{norm_type_id}"
            tasks.append(
                self._get_laws_constitution_amendments_docs_links(url, norm_type)
            )
            type_list.append(norm_type)

        results = await asyncio.gather(*tasks)
        for norm_type, docs in zip(type_list, results):
            for doc in docs:
                year = doc.get("year")
                if year is None or year < self.year_start or year > self.year_end:
                    continue
                self._prefetched_docs[year][norm_type].append(doc)

    async def scrape(self) -> int:
        """Scrape data from all years.

        Prefetches all document links for paginated and static index
        categories, groups them by year, then delegates to
        BaseScraper.scrape() for year-sequential processing with
        document-level resumability.
        """
        # Prefetch paginated links and Lei Ordinária years concurrently
        _, lei_ord_years = await asyncio.gather(
            self._prefetch_paginated_links(self.year_start),
            self._fetch_lei_ordinaria_years(),
        )
        self._lei_ordinaria_years: set[int] = set(lei_ord_years)

        # Prefetch static index links (cheap — only fetches link lists)
        await self._prefetch_static_index_links()

        # Build the set of years that actually have data, bounded by year range
        all_years = set(self._prefetched_docs.keys()) | self._lei_ordinaria_years
        if all_years:
            self.years = sorted(
                y for y in all_years if self.year_start <= y <= self.year_end
            )

        return await super().scrape()
