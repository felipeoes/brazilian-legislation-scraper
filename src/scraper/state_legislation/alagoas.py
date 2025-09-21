from typing import List, Optional
import requests
import base64

from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper
from src.database.saver import FileSaver

TYPES = {
    "Consituição Estadual": "TIP080",
    "Decreto": "TIP002",
    "Decreto Autônomo": "TIP045",
    "Emenda Constitucional": "TIP081",
    "Ementário": "TIP108",
    "Lei Complementar": "TIP042",
    "Lei Delegada": "TIP044",
    "Lei Ordinária": "TIP043",
}

VALID_SITUATIONS = [
    "Não consta"
]  # Conama does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = (
    []
)  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class AlagoasSefazScraper(BaseScaper):
    """Webscraper for Alagoas Sefaz website (https://gcs2.sefaz.al.gov.br/#/administrativo/documentos/consultar-gabinete)

    Example search request: https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/administrativo/documento/consultar?pagina=1

    Payload: {
            "palavraChave": null,
            "periodoInicial": "2024-01-01T03:00:00.000+0000",
            "periodoFinal": "2024-12-31T03:00:00.000+0000",
            "numero": null,
            "especieLegislativa": "TIP002",
            "codigoCategoria": "CAT017",
            "codigoSetor": null
        }

    Observation: Alagoas Sefaz does not have a situation field
    """

    def __init__(
        self,
        base_url: str = "https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/administrativo/documento/consultar",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.params = {
            "periodoInicial": "2025-02-01T00:00:00.000-0300",
            "periodoFinal": "2025-12-31T00:00:00.000-0300",
            "numero": None,
            "especieLegislativa": TYPES["Consituição Estadual"],
            "codigoCategoria": "CAT017",
            "codigoSetor": None,
        }
        self.docs_save_dir = self.docs_save_dir / "ALAGOAS"
        self.view_doc_url = "https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/documentos/visualizarDocumento?"
        # Initialize the FileSaver
        self.saver = FileSaver(self.docs_save_dir)

    def _format_search_url(self, norm_type_id: str, year: int, page: int = 1) -> str:
        """Format url for search request"""
        self.params["especieLegislativa"] = norm_type_id
        self.params["periodoInicial"] = f"{year}-01-01T00:00:00.000-0300"
        self.params["periodoFinal"] = f"{year}-12-31T00:00:00.000-0300"

        if page is not None and page > 1:
            return self.base_url + f"?pagina={page}"

        return self.base_url

    def _get_docs_links(self, url: str, norms: list) -> Optional[List[dict]]:
        """Get document links from search request"""
        try:
            response = self._make_request(url, method="POST", json=self.params)

            if response is None:
                return

            data = response.json()
            norms.extend(data["documentos"])

        except Exception as e:
            print(f"Error getting document links from url: {url} | Error: {e}")

    def _get_doc_data(self, doc_info: dict) -> Optional[dict]:
        """Get document data from norm dict. Download url for pdf will follow the pattern: ttps://gcs2.sefaz.al.gov.br/#/documentos/visualizar-documento?acess={acess}&key={key}"""

        key = requests.utils.quote(
            requests.utils.quote(doc_info["link"]["key"])
        )  # need to double encode otherwise it will return 404
        doc_link = f"{self.view_doc_url}acess={doc_info['link']['acess']}&key={key}"
        filename = ""
        try:
            # get text markdown
            response = self._make_request(doc_link).json()
            base64_data = response["arquivo"]["base64"]
            filename = ".".join(response["arquivo"]["nomeArquivo"].split(".")[:-1])

            pdf_bytes = base64.b64decode(base64_data)
            text_markdown = self._get_pdf_image_markdown(pdf_bytes)

        except Exception as e:
            print(f"Error getting markdown from url: {doc_link} | Error: {e}")
            text_markdown = None

        if text_markdown is None:
            return None

        return {
            "id": doc_info["numeroDocumento"],
            "title": filename,
            "summary": doc_info["textoEmenta"],
            "category": doc_info["categoria"]["descricao"],
            "publication_date": doc_info["dataPublicacao"],
            "text_markdown": text_markdown,
            "document_url": doc_link,
        }

    def _scrape_year(self, year: str) -> List[dict]:
        """Scrape norms for a specific year"""
        all_results = []
        
        for situation in tqdm(
            self.situations,
            desc="SEFAZ - ALAGOAS | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc=f"SEFAZ - ALAGOAS Year: {year} | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):
                url = self._format_search_url(norm_type_id, year)

                response = self._make_request(url, method="POST", json=self.params)

                if response is None:
                    continue

                data = response.json()
                total_norms = data["registrosTotais"]

                if total_norms is None:
                    continue

                pages = total_norms // 10 + 1

                norms = []
                norms.extend(data["documentos"])

                # get all norms
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(
                            self._get_docs_links,
                            self._format_search_url(norm_type_id, year, page), 
                            norms
                        )
                        for page in range(2, pages)
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        desc="SEFAZ - ALAGOAS | Get document links",
                        total=len(futures),
                        disable=not self.verbose,
                    ):
                        try:
                            result = future.result()
                            if result is None:
                                continue

                            # norms.extend(result)
                        except Exception as e:
                            print(f"Error getting document links | Error: {e}")

                results = []

                # get all norm data
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self._get_doc_data, norm) for norm in norms
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        desc="SEFAZ - ALAGOAS | Get document data",
                        total=len(norms),
                        disable=not self.verbose,
                    ):

                        try:
                            result = future.result()
                            if result is None:
                                continue

                            # prepare item for saving
                            queue_item = {
                                "year": year,
                                "type": norm_type,
                                "situation": situation,
                                **result,
                            }

                            results.append(queue_item)

                        except Exception as e:
                            print(f"Error getting document data | Error: {e}")

                    all_results.extend(results)
                    self.count += len(results)

                    if self.verbose:
                        print(
                            f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                        )
        
        return all_results
