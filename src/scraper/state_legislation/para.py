import re
from typing import Optional, List, Dict
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper
from src.database.saver import FileSaver

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

INVALID_SITUATIONS = (
    []
)  # norms with these situations are invalid norms (no longer in effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class ParaAlepaScraper(BaseScaper):
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
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "PARA"
        self.params = {
            "numero": "",
            "anoLei": "",
            "tipo": "",
            "pChave": "",
            "verifica": 1,
            "button": "Buscar",
        }
        self.fetched_constitution = False
        self.regex_total_count = re.compile(r"Total de Registros:\s+(\d+)")
        # Initialize the FileSaver
        self.saver = FileSaver(self.docs_save_dir)

    def _format_search_url(self, norm_type_id: int, year: int) -> str:
        self.params["tipo"] = norm_type_id
        self.params["anoLei"] = year

        return f"{self.base_url}/index.php"

    def _get_docs_links(self, url: str, norm_type: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'summary', 'pdf_link'
        """
        response = self._make_request(url, method="POST", payload=self.params)
        if not response:
            print(f"Error fetching page: {url}")
            return []
        
        soup = BeautifulSoup(response.content, "html.parser")

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

    def _get_doc_data(self, doc_info: dict) -> Optional[dict]:
        """Get document data from given document dict"""
        # remove pdf_link from doc_info
        pdf_link = doc_info.pop("pdf_link")
        text_markdown = self._get_markdown(pdf_link)

        if not text_markdown or not text_markdown.strip():
            print(f"Error getting markdown from pdf: {pdf_link}")
            return None

        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = pdf_link
        return doc_info

    def _scrape_constitution(self):
        """Scrape the constitution"""

    def _scrape_year(self, year: int) -> List[Dict]:
        """Scrape norms for a specific year"""
        all_results = []
        
        for situation in tqdm(
            self.situations,
            desc="PARA | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc=f"PARA | Year: {year} | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):
                # all docs are fetched in one single page
                url = self._format_search_url(norm_type_id, year)
                docs = self._get_docs_links(url, norm_type)

                results = []
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self._get_doc_data, doc_info)
                        for doc_info in docs
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        total=len(futures),
                        desc=f"PARA | Year: {year} | {norm_type}",
                        disable=not self.verbose,
                    ):
                        result = future.result()
                        if result is None:
                            continue

                        # save to results
                        queue_item = {
                            "year": year,
                            "situation": situation,
                            "type": norm_type,
                            **result,
                        }

                        results.append(queue_item)

                all_results.extend(results)
                self.results.extend(results)
                self.count += len(results)

                if self.verbose:
                    print(
                        f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                    )
        
        return all_results
