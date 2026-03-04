import asyncio
import time
from typing import Optional
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.scraper import BaseScraper

TYPES = {
    "Lei Ordinária": "lei ord",
    "Lei Complementar": "lei comp",
    "Emenda Constitucional": "emenda",
    "Constituição Estadual": "constituição",
}

VALID_SITUATIONS = [
    "Não consta"
]  # ALRN does not have a situation field, so we can not distinguish between valid and invalid norms

INVALID_SITUATIONS = []
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class RNAlrnScraper(BaseScraper):
    """Webscraper for Rio Grande do Norte state legislation website (https://www.al.rn.leg.br/legislacao/pesquisa)

    Example search request: https://www.al.rn.leg.br/legislacao/pesquisa?tipo=nome&nome=lei%20ord&page=4

    payload = {
        "tipo": "nome",
        "nome": "lei ord",
        "page": 4,
    }
    """

    def __init__(
        self,
        base_url: str = "https://www.al.rn.leg.br",
        **kwargs,
    ):
        from src.scraper.base.scraper import STATE_LEGISLATION_SAVE_DIR

        if STATE_LEGISLATION_SAVE_DIR:
            kwargs.setdefault("docs_save_dir", STATE_LEGISLATION_SAVE_DIR)
        super().__init__(
            base_url,
            types=TYPES,
            situations=SITUATIONS,
            name="RIO_GRANDE_DO_NORTE",
            **kwargs,
        )

    def _build_search_url(self, norm_type_id: str, page: int) -> str:
        """Build search URL from arguments (no shared state mutation)."""
        params = {
            "tipo": "nome",
            "nome": norm_type_id,
            "page": page,
        }
        return f"{self.base_url}/legislacao/pesquisa?{urlencode(params)}"

    async def _get_docs_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'summary', 'html_link'
        """
        response = await self.request_service.make_request(url)
        if response is None:
            logger.warning(f"No response for url: {url}")
            return []
        soup = BeautifulSoup(await response.read(), "html.parser")

        docs = []

        table = soup.find("table", class_="table table-sm table-striped")
        items = table.find_all("tr")

        if not items:
            logger.warning(f"Empty table for url: {url}")

        for item in items:
            tds = item.find_all("td")
            if len(tds) == 0:  # skip invalid rows, valid documents have at least 1 td
                continue

            th = item.find("th")

            title = th.text.strip()
            year = int(tds[0].text.strip())
            pdf_link = tds[1].find("a")
            pdf_link = pdf_link["href"]

            docs.append(
                {
                    "year": year,
                    "title": title,
                    "summary": "",  # do not have a field for summary
                    "pdf_link": pdf_link,
                }
            )

        return docs

    async def _get_doc_data(
        self, doc_info: dict, pdf_len_threshold: int = 200
    ) -> Optional[dict]:
        """Get document data from given document dict"""
        # remove pdf_link from doc_info
        pdf_link = doc_info.pop("pdf_link")
        response = await self.request_service.make_request(pdf_link)

        if not response:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=pdf_link,
                error_message="Failed to download PDF",
            )
            return None

        text_markdown = await self._get_markdown(response=response)

        if (
            not text_markdown
            or not text_markdown.strip()
            or len(text_markdown.strip()) < pdf_len_threshold
        ):
            # probably image pdf
            text_markdown = await self._get_pdf_image_markdown(await response.read())

        if (
            not text_markdown or not text_markdown.strip()
        ):  # indeed an invalid or unavailable pdf
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=pdf_link,
                error_message="Empty markdown from PDF",
            )
            return None

        doc_info["text_markdown"] = text_markdown.strip()
        doc_info["document_url"] = pdf_link

        return doc_info

    async def _scrape_situation_type(
        self, situation: str, norm_type: str, norm_type_id: str
    ) -> list:
        """Scrape norms for a specific situation and type"""
        url = self._build_search_url(norm_type_id, 1)
        soup = await self.request_service.get_soup(url)

        total_pages = soup.find("ul", class_="pagination")
        if not total_pages:  # must have only one page
            total_pages = 1
        else:
            total_pages = total_pages.find_all("li")[-2]
            total_pages = int(total_pages.find("a").text.strip())

        # Get documents html links
        documents = []

        tasks = [
            self._get_docs_links(
                self._build_search_url(norm_type_id, page),
            )
            for page in range(1, total_pages + 1)
        ]
        valid_results = await self._gather_results(
            tasks,
            context={"year": "N/A", "type": norm_type, "situation": situation},
            desc=f"RIO GRANDE DO NORTE | {norm_type} | get_docs_links",
        )
        for result in valid_results:
            if result:
                documents.extend(result)

        # Get document data
        results = []
        tasks = [self._get_doc_data(doc_info) for doc_info in documents]
        valid_results = await self._gather_results(
            tasks,
            context={"year": "N/A", "type": norm_type, "situation": situation},
            desc=f"RIO GRANDE DO NORTE | {norm_type}",
        )
        for result in valid_results:
            if result:
                # prepare item for saving
                queue_item = {
                    # hardcode since we only get valid documents in search request
                    "situation": situation,
                    "type": norm_type,
                    **result,
                }

                results.append(queue_item)
            else:
                logger.warning("Invalid document returned from get_doc_data")

        if self.verbose:
            logger.info(
                f"Finished scraping for Situation: {situation} | Type: {norm_type} | Results: {len(results)}"
            )

        return results

    async def scrape(self) -> list:
        """Scrape data from all types and situations"""

        await self._ensure_session()
        self._scrape_start = time.time()

        # scrape data
        tasks = [
            self._scrape_situation_type(sit, nt, nt_id)
            for sit in self.situations
            for nt, nt_id in self.types.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error in _scrape_situation_type: {result}")
            elif result:
                await self.saver.save(result)
                self.results.extend(result)
                self.count += len(result)

        await self._save_summary()
        return self.results
