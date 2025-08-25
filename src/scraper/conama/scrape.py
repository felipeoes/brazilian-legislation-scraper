import urllib.parse
import re
from typing import Dict, List, Optional

from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper

TYPES = {
    "Resolução": 1,
    "Moção": 2,
    "Recomendação": 3,
    "Proposição": 4,
    "Decisão": 5,
    "Portaria": 6,
}

VALID_SITUATIONS = [
    "Não consta"
]  # Conama does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = (
    []
)  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class ConamaScraper(BaseScaper):
    """Webscraper for Conama (Conselho Nacional do Meio Ambiente) website (https://conama.mma.gov.br/atos-normativos-sistema)

    Example search request: https://conama.mma.gov.br/?option=com_sisconama&order=asc&offset=0&limit=30&task=atosnormativos.getList&tipo=6&ano=1984

    Observation: Conama does not have a situation field, invalid norms will have an indication in the document text
    """

    def __init__(
        self,
        base_url: str = "https://conama.mma.gov.br/",
        **kwargs,
    ):
        super().__init__(
            base_url, types=list(TYPES.keys()), situations=SITUATIONS, **kwargs
        )
        self.params = {
            "option": "com_sisconama",
            "order": "asc",
            "offset": 0,
            "limit": 100,
            "task": "atosnormativos.getList",
        }
        self.docs_save_dir = self.docs_save_dir / "CONAMA"
        self._situation_regex = re.compile(r"Revogad|Revogação", re.IGNORECASE)
        self._initialize_saver()

    def _format_search_url(self, norm_type: str) -> str:
        """Format url for search request"""
        return f"{self.base_url}?option={self.params['option']}&order={self.params['order']}&offset={self.params['offset']}&limit={self.params['limit']}&task={self.params['task']}&tipo={TYPES[norm_type]}&ano={self.params['ano']}"

    def _get_doc_data(self, doc_info: dict) -> Optional[Dict]:
        """Get document data from norm dict. Download url for pdf will follow the pattern: https://conama.mma.gov.br/?option=com_sisconama&task=arquivo.download&id={id}"""
        doc_id = doc_info["aid"]
        doc_number = doc_info["numero"]
        doc_description = doc_info["descricao"]
        doc_type = doc_info["nomeato"]
        doc_status = doc_info["status"]
        doc_keyword = doc_info["palavra_chave"]
        doc_origin = doc_info["porigem"]
        doc_url = urllib.parse.urljoin(
            self.base_url,
            f"?option=com_sisconama&task=arquivo.download&id={doc_id}",
        )

        # get text markdown
        text_markdown = self._get_markdown(doc_url)

        if text_markdown is None or not text_markdown.strip():
            return None
        
        # get situation from doc_status. If "Revogad" or "Revogação" in doc_status, situation is "Revogada", otherwise "Não consta"
        situation = "Não consta revogação expressa"
        if doc_status and self._situation_regex.search(doc_status):
            situation = "Revogada"
        

        # title will be like Resolução CONAMA Nº 501/2021
        return {
            "title": f"{doc_type} CONAMA Nº {doc_number}/{doc_info['ano']}",
            "id": doc_id,
            "number": doc_number,
            "summary": doc_description,
            "situation": situation,
            "keyword": doc_keyword,
            "origin": doc_origin,
            "text_markdown": text_markdown,
            "document_url": doc_url,
        }

    def _scrape_year(self, year: int) -> List[Dict]:
        """Scrape norms for a specific year"""
        results = []
        year_str = str(year)

        for situation in tqdm(
            self.situations,
            desc="CONAMA | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type in tqdm(
                self.types,
                desc="CONAMA | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):
                self.params["ano"] = year_str
                self.params["offset"] = 0
                url = self._format_search_url(norm_type)

                response = self._make_request(url)
                if response is None:
                    continue
                data = response.json()["data"]
                total_norms = data["total"]

                norms = []
                norms.extend(data["rows"])

                # get all norms
                while self.params["offset"] < total_norms:
                    self.params["offset"] += self.params["limit"]
                    url = self._format_search_url(norm_type)
                    response = self._make_request(url)
                    if response is None:
                        break
                    data = response.json()["data"]
                    norms.extend(data["rows"])

                type_results = []
                # get all norm data
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self._get_doc_data, norm) for norm in norms
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        desc="CONAMA | Get document data",
                        total=len(norms),
                        disable=not self.verbose,
                    ):
                        result = future.result()
                        if result is None:
                            continue

                        # save to one drive
                        queue_item = {
                            "year": year_str,
                            "type": norm_type,
                            "situation": situation,
                            **result,
                        }

                        self.queue.put(queue_item)
                        type_results.append(queue_item)

                    results.extend(type_results)
                    self.results.extend(type_results)
                    self.count += len(type_results)

                    if self.verbose:
                        print(
                            f"Year: {year_str} | Type: {norm_type} | Situation: {situation} | Total: {len(type_results)}"
                        )

        return results
