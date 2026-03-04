from typing import Optional
from urllib.parse import urljoin, urlencode

import asyncio
import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.scraper import BaseScraper, YEAR_START

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

    def __init__(
        self,
        base_url: str = "https://www.al.ce.gov.br/legislativo",
        **kwargs,
    ):
        from src.scraper.base.scraper import STATE_LEGISLATION_SAVE_DIR

        if STATE_LEGISLATION_SAVE_DIR:
            kwargs.setdefault("docs_save_dir", STATE_LEGISLATION_SAVE_DIR)
        super().__init__(
            base_url, name="CEARA", types=TYPES, situations=SITUATIONS, **kwargs
        )
        self.params = {
            "categoria": "",
            "page": 1,
        }

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
        # document url will be a link to pdf document
        text_markdown = await self._get_markdown(url=doc_info["document_url"])
        doc_info["text_markdown"] = text_markdown

        return doc_info

    async def _scrape_situation_type_norms(
        self, situation: str, norm_type: str, norm_type_id: str
    ) -> list:
        """Scrape laws and norms from given situation and norm type"""
        url = self._format_search_url(norm_type_id, 1)
        soup = await self.request_service.get_soup(url, timeout=60)

        # get total pages
        pagination = soup.find("ul", class_="pagination")
        if pagination:
            pages = pagination.find_all("li")
            last_page = pages[-2].find("a")["href"]
            total_pages = int(last_page.split("page=")[-1])
        else:
            total_pages = 1

        # Get documents html links
        tasks = [
            self._get_docs_links(
                norm_type,
                self._format_search_url(norm_type_id, page),
            )
            for page in range(1, total_pages + 1)
        ]
        valid_results = await self._gather_results(
            tasks,
            context={"year": "NA", "type": norm_type, "situation": situation},
            desc=f"CEARA | {norm_type} | get_docs_links",
        )
        documents = []
        for result in valid_results:
            documents.extend(result)

        # Get document data
        tasks = [self._get_doc_data(doc) for doc in documents]
        valid_results = await self._gather_results(
            tasks,
            context={"year": "NA", "type": norm_type, "situation": situation},
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

            results.append(queue_item)

        if self.verbose:
            logger.info(
                f"Finished scraping for | Situation: {situation} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
            )

        return results

    async def _scrape_norms(
        self, situation: str, norm_type: str, norm_type_id: str
    ) -> list:
        """Scrape laws and norms from given situation and norm type"""
        return await self._scrape_situation_type_norms(
            situation, norm_type, norm_type_id
        )

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
                    logger.warning(f"Could not extract year from title '{title[:80]}' — skipping")
                    continue
                year_text = year_match.group(1)

                if len(year_text) == 2:
                    year_now = datetime.now().year
                    year_text = f"20{year_text}"
                    if int(year_text) > year_now:
                        year_text = f"19{year_text[2:]}"

                year = int(year_text)

            try:  # some documents are not available, so we skip them
                summary = tds[1].text.strip()
                html_link = tds[0].find("a")["href"]
                # remove "../" from html_link
                html_link = html_link.replace("../", "")
                docs.append(
                    {
                        "title": title,
                        "year": year,
                        "summary": summary,
                        "html_link": html_link,
                    }
                )

            except Exception:
                logger.warning(f"No link found for document '{title}' — skipping")
                continue

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

        if not response:
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
        for node in soup.find_all(string=re.compile(r"O texto desta Lei", re.IGNORECASE)):
            node.parent.decompose()

        html_string = soup.prettify().strip()

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

    async def _save_results(self, results: list):
        if results:
            await self.saver.save(results)

    async def scrape(self) -> list:
        """Scrape data from all years"""

        self._scrape_start = time.time()

        # check if can resume from last scrapped year
        resume_from = self.year_start  # 1808
        forced_resume = self.year_start != YEAR_START
        if self.saver and self.saver.last_year is not None and not forced_resume:
            logger.info(f"Resuming from {self.saver.last_year}")
            resume_from = int(self.saver.last_year)
        else:
            logger.info(f"Starting from {resume_from}")

        # Collect all tasks for concurrent execution
        all_tasks = []
        task_metadata = []

        for situation in self.situations:
            for norm_type, norm_type_id in self.types.items():
                if norm_type in ["Ato Deliberativo", "Ato Normativo", "Resolução"]:
                    all_tasks.append(
                        self._scrape_situation_type_norms(
                            situation, norm_type, norm_type_id
                        )
                    )
                    task_metadata.append(
                        {"situation": situation, "norm_type": norm_type, "year": None}
                    )
                elif norm_type in ["Emenda Constitucional", "Lei Complementar", "Decreto Legislativo"]:
                    all_tasks.append(
                        self._scrape_laws_constitution_amendments(
                            situation, norm_type, norm_type_id
                        )
                    )
                    task_metadata.append(
                        {"situation": situation, "norm_type": norm_type, "year": None}
                    )
                else:
                    # For Lei Ordinária, we need to get available years first
                    # This part remains sequential as it depends on fetching available years
                    url = f"https://www2.al.ce.gov.br/legislativo/{norm_type_id}"
                    soup = await self.request_service.get_soup(url)
                    table = soup.find("table", {"class": "MsoNormalTable"})
                    rows = table.find_all("tr")
                    available_years = []
                    for index in range(1, len(rows)):
                        item = rows[index]
                        tds = item.find_all("td")
                        for td in tds:
                            a = td.find("a")
                            if a:
                                available_years.append(int(a.text))
                    available_years.sort()

                    for year in available_years:
                        if year < resume_from:
                            continue
                        all_tasks.append(
                            self._scrape_laws_constitution_amendments(
                                situation, norm_type, norm_type_id, year
                            )
                        )
                        task_metadata.append(
                            {
                                "situation": situation,
                                "norm_type": norm_type,
                                "year": year,
                            }
                        )

        # Execute all tasks concurrently
        results_list = await asyncio.gather(*all_tasks, return_exceptions=True)

        # Process results
        for i, result in enumerate(results_list):
            if isinstance(result, Exception):
                meta = task_metadata[i]
                logger.error(
                    f"Error in scraping {meta['norm_type']} (year: {meta['year']}): {result}"
                )
            elif result:
                self.results.extend(result)
                self.count += len(result)
                await self._save_results(result)

        await self._save_summary()
        return self.results
