from collections import defaultdict
from io import BytesIO
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.scraper import StateScraper

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


class RNAlrnScraper(StateScraper):
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
        super().__init__(
            base_url,
            types=TYPES,
            situations=SITUATIONS,
            name="RIO_GRANDE_DO_NORTE",
            **kwargs,
        )
        # {year: {norm_type: [doc_info, ...]}} built during prefetch
        self._prefetched_docs: dict[int, dict[str, list]] = defaultdict(
            lambda: defaultdict(list)
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
        if not response:
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
    ) -> dict | None:
        """Get document data from given document dict"""
        # remove pdf_link from doc_info
        pdf_link = doc_info.pop("pdf_link")

        if self._is_already_scraped(pdf_link, doc_info.get("title", "")):
            return None

        response = await self.request_service.make_request(pdf_link)

        if not response:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=pdf_link,
                error_message="Failed to download PDF",
            )
            return None

        raw_content = await response.read()
        text_markdown = await self._get_markdown(stream=BytesIO(raw_content))

        if (
            not text_markdown
            or not text_markdown.strip()
            or len(text_markdown.strip()) < pdf_len_threshold
        ):
            # probably image pdf
            text_markdown = await self._get_markdown(stream=BytesIO(raw_content))

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
        doc_info["_raw_content"] = raw_content
        doc_info["_content_extension"] = ".pdf"

        return doc_info

    async def _prefetch_type_links(self, norm_type: str, norm_type_id: str) -> None:
        """Fetch all pages for a norm type and group doc links by year."""
        url = self._build_search_url(norm_type_id, 1)
        soup = await self.request_service.get_soup(url)

        total_pages = soup.find("ul", class_="pagination")
        if not total_pages:
            total_pages = 1
        else:
            total_pages = int(total_pages.find_all("li")[-2].find("a").text.strip())

        tasks = [
            self._get_docs_links(self._build_search_url(norm_type_id, page))
            for page in range(1, total_pages + 1)
        ]
        valid_results = await self._gather_results(
            tasks,
            context={"type": norm_type},
            desc=f"RIO GRANDE DO NORTE | {norm_type} | prefetch",
        )

        count = 0
        for result in valid_results:
            if result:
                for doc in result:
                    year = doc["year"]
                    self._prefetched_docs[year][norm_type].append(doc)
                    count += 1

        logger.info(f"Prefetched {count} links for {norm_type} ({total_pages} pages)")

    async def _prefetch_all_links(self) -> None:
        """Prefetch doc links for all norm types and build self.years."""
        for norm_type, norm_type_id in self.types.items():
            await self._prefetch_type_links(norm_type, norm_type_id)

        all_years = set(self._prefetched_docs.keys())
        if all_years:
            self.years = sorted(
                y for y in all_years if self.year_start <= y <= self.year_end
            )

    async def _scrape_type(
        self, norm_type: str, norm_type_id: str, year: int
    ) -> list[dict]:
        """Scrape documents of a single type for a given year using prefetched links."""
        docs = self._prefetched_docs.get(year, {}).get(norm_type, [])
        if not docs:
            return []

        situation = self.situations[0] if self.situations else "Não consta"
        ctx = {"year": year, "situation": situation, "type": norm_type}
        tasks = [self._with_save(self._get_doc_data(doc), ctx) for doc in docs]
        results = await self._gather_results(
            tasks,
            context=ctx,
            desc=f"RIO GRANDE DO NORTE | {norm_type} {year}",
        )

        if self.verbose:
            logger.info(f"Year: {year} | Type: {norm_type} | Results: {len(results)}")

        return results

    async def scrape(self) -> list:
        """Prefetch all doc links then delegate to BaseScraper's year-sequential flow."""
        await self._prefetch_all_links()
        return await super().scrape()
