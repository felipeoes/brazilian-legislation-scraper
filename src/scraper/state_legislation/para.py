import re
from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.scraper import StateScraper

TYPES = {
    "Decreto Estadual": 2,
    "Decreto Legislativo": 1,
    "Emenda Constitucional": 11,
    "Lei Complementar": 4,
    "Lei Ordinária": 3,
    "Resolução": 10,
}

VALID_SITUATIONS = [
    "Não consta"
]  # Alepa does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no longer in effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class ParaAlepaScraper(StateScraper):
    """Webscraper for Para state legislation website (http://bancodeleis.alepa.pa.gov.br)

    Example search request: http://bancodeleis.alepa.pa.gov.br/index.php

    payload = {
        numero:
        anoLei: 2000
        tipo: 2
        pChave:
        verifica: 1
        button: Buscar
    }

    """

    def __init__(
        self,
        base_url: str = "http://bancodeleis.alepa.pa.gov.br",
        **kwargs,
    ):
        super().__init__(
            base_url, types=TYPES, situations=SITUATIONS, name="PARA", **kwargs
        )
        self.fetched_constitution = False
        self.regex_total_count = re.compile(r"Total de Registros:\s+(\d+)")

    def _build_params(self, norm_type_id: int, year: int) -> dict:
        """Build a fresh params dict for a specific type/year query (no shared state)."""
        return {
            "numero": "",
            "anoLei": year,
            "tipo": norm_type_id,
            "pChave": "",
            "verifica": 1,
            "button": "Buscar",
        }

    async def _get_docs_links(self, url: str, params: dict, norm_type: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'summary', 'pdf_link'
        """
        response = await self.request_service.make_request(
            url, method="POST", payload=params
        )
        if not response:
            logger.error(f"Error fetching page: {url}")
            return []

        soup = BeautifulSoup(await response.read(), "html.parser")

        #   Total de Registros:                      0
        # check if empty page
        total_count = self.regex_total_count.search(soup.prettify())
        if total_count is None or int(total_count.group(1)) == 0:
            return []

        docs = []

        # items will be in the last table of the page
        table = soup.find_all("table")[-1]
        items = table.find_all("tr")

        for item in items:
            tds = item.find_all("td")
            if len(tds) == 2:
                title = tds[0].find("strong").next_sibling.strip()
                pdf_link = tds[1].find("a")
                summary = pdf_link.text.strip()
                pdf_link = pdf_link["href"]

                docs.append(
                    {
                        "title": f"{norm_type} {title}",
                        "summary": summary,
                        "pdf_link": pdf_link,
                    }
                )

        return docs

    async def _get_doc_data(self, doc_info: dict) -> dict | None:
        """Get document data from given document dict"""
        # remove pdf_link from doc_info
        pdf_link = doc_info.pop("pdf_link")

        if self._is_already_scraped(pdf_link, doc_info.get("title", "")):
            return None

        text_markdown, raw_content, content_ext = await self._download_and_convert(
            pdf_link
        )

        if not text_markdown or not text_markdown.strip():
            logger.error(f"Error getting markdown from pdf: {pdf_link}")
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=pdf_link,
                error_message="Empty markdown from PDF",
            )
            return None

        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = pdf_link
        doc_info["_raw_content"] = raw_content
        doc_info["_content_extension"] = content_ext
        return doc_info

    def _scrape_constitution(self):
        """Scrape the constitution"""

    async def _scrape_situation_type(
        self, situation: str, norm_type: str, norm_type_id: int, year: int
    ) -> list:
        """Scrape norms for a specific situation and type"""
        # all docs are fetched in one single page
        params = self._build_params(norm_type_id, year)
        url = f"{self.base_url}/index.php"
        docs = await self._get_docs_links(url, params, norm_type)

        ctx = {"year": year, "situation": situation, "type": norm_type}
        tasks = [
            self._with_save(self._get_doc_data(doc_info), ctx) for doc_info in docs
        ]
        results = await self._gather_results(
            tasks,
            context=ctx,
            desc=f"PARA | {norm_type}",
        )

        if self.verbose:
            logger.info(
                f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)}"
            )

        return results

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape norms for a specific year"""
        tasks = [
            self._scrape_situation_type(sit, nt, ntid, year)
            for sit in self.situations
            for nt, ntid in self.types.items()
        ]
        valid = await self._gather_results(
            tasks,
            context={"year": year, "type": "N/A", "situation": "N/A"},
            desc=f"{self.name} | Year {year}",
        )
        return self._flatten_results(valid)
