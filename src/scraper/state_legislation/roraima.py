from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Optional, Optional

from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper


# gotten from https://sapl.al.rr.leg.br/api/norma/tiponormajuridica/
TYPES = {
    "Ato da Mesa Diretora": 9,
    "Ação Direta de Inconstitucionalidade": 11,
    "Constituição Estadual": 10,
    "Código de Ética Parlamentar - Resolução 29/1995": 12,
    "Decreto Legislativo": 1,
    "Emenda à Constituição": 6,
    "Lei Complementar": 3,
    "Lei Delegada": 7,
    "Lei Ordinária": 2,
    "Questões de Ordem": 13,
}

SITUATIONS = []

class RoraimaAlpbScraper(BaseScaper):
    """Webscraper for Roraima state legislation website (https://sapl.al.rr.leg.br/)

    Example search request: https://sapl.al.rr.leg.br/api/norma/normajuridica/?tipo=2&page=3&ano=2025

    params = {
        tipo: 2
        page: 3
        ano: 2025
    }
    """

    def __init__(
        self,
        base_url: str = "https://sapl.al.rr.leg.br",
        **kwargs: Any,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "RORAIMA"
        self.subjects: Dict[int, str] = {}
        self._initialize_saver()
        self._fetch_subjects()

    def _format_search_url(
        self,
        norm_type_id: str,
        year: int,
        page: int = 1,
    ) -> str:
        """Format url for search request"""
        return f"{self.base_url}/api/norma/normajuridica/?tipo={norm_type_id}&page={page}&ano={year}"

    def _get_docs_links(self, url: str) -> list:
        """Get document links from search request."""
        response = self._make_request(url)
        if not response:
            return []
            
        items = response.json().get("results", [])
        docs = []

        for item in items:
            if not item.get("texto_integral"):
                continue

            situation = (
                "Revogada"
                if item.get("data_vigencia")
                else "Não consta revogação expressa"
            )

            doc = {
                "id": item["id"],
                "norm_number": item["numero"],
                "title": item["__str__"],
                "situation": situation,
                "summary": item["ementa"],
                "subject": [self.subjects.get(s, "") for s in item.get("assuntos", [])],
                "date": item["data"],
                "origin": item.get("esfera_federacao"),
                "publication": item.get("veiculo_publicacao"),
                "pdf_link": item["texto_integral"],
            }
            docs.append(doc)

        return docs

    def _process_pdf(self, pdf_link: str, year: int) -> Optional[dict]:
        """Process PDF and return text markdown."""
        response = self._make_request(pdf_link)
        if not response or not response.content:
            return None

        text_markdown = self._get_markdown(response=response)
        if text_markdown and text_markdown.strip():
            return {
                "text_markdown": text_markdown,
                "document_url": pdf_link,
            }
            
        if not text_markdown or not text_markdown.strip():
            try:
                text_markdown = self._get_pdf_image_markdown(response.content)
            except Exception as e:
                print(f"Error processing PDF: {e}")
                return None

        if not text_markdown or not text_markdown.strip():
            return None

        return {
            "text_markdown": text_markdown,
            "document_url": pdf_link,
        }

    def _get_doc_data(self, doc_info: dict, year: int) -> Optional[dict]:
        """Get document data"""
        pdf_link = doc_info.pop("pdf_link")
        processed_pdf = self._process_pdf(pdf_link, year)

        if processed_pdf is None:
            return None

        doc_info.update(processed_pdf)
        return doc_info

    def _fetch_subjects(self):
        """Fetch all subjects from the API."""
        if self.subjects:
            return

        subjects_url = f"{self.base_url}/api/norma/assuntonorma/"
        response = self._make_request(subjects_url)
        if not response:
            return
            
        data = response.json()
        total_pages = data["pagination"]["total_pages"]

        subjects = {item["id"]: item["assunto"] for item in data["results"]}

        for page in tqdm(
            range(2, total_pages + 1),
            desc="RORAIMA | Fetching subjects",
            total=total_pages,
            disable=not self.verbose,
        ):
            response = self._make_request(f"{subjects_url}?page={page}")
            if not response:
                continue
            data = response.json()
            subjects.update(
                {item["id"]: item["assunto"] for item in data["results"]}
            )

        self.subjects = subjects

    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
        for norm_type, norm_type_id in tqdm(
            self.types.items() if isinstance(self.types, dict) else [],
            desc=f"RORAIMA | Year: {year} | Types",
            total=len(self.types) if isinstance(self.types, dict) else 0,
            disable=not self.verbose,
        ):
            url = self._format_search_url(norm_type_id, year)
            response = self._make_request(url)

            if not response or response.status_code != 200:
                if self.verbose:
                    status_code = response.status_code if response else "No response"
                    print(
                        
                        f"Error fetching data for Year: {year} | Type: {norm_type} | Status Code: {status_code}"
                    )
                continue

            data = response.json()
            if not data.get("results"):
                continue

            total_pages = data["pagination"]["total_pages"]
            
            data_futures = []
            results = []
            with ThreadPoolExecutor() as executor:
                doc_links_futures = [
                    executor.submit(
                        self._get_docs_links,
                        self._format_search_url(norm_type_id, year, page=page),
                    )
                    for page in range(1, total_pages + 1)
                ]

                documents = []
                for future in tqdm(
                    as_completed(doc_links_futures),
                    desc="RORAIMA | Get document links",
                    total=len(doc_links_futures),
                    disable=not self.verbose,
                ):
                    doc_info = future.result()
                    if not doc_info:
                        continue
                    
                    documents.extend(doc_info)

                    for doc in doc_info:
                        data_futures.append(
                        executor.submit(self._get_doc_data, doc, year))

                for future in tqdm(
                    as_completed(data_futures),
                    desc="RORAIMA | Get document data",
                    total=len(data_futures),
                    disable=not self.verbose,
                ):
                    result = future.result()
                    if result:
                        queue_item = {"year": year, "type": norm_type, **result}
                        self.queue.put(queue_item)
                        results.append(queue_item)

            self.results.extend(results)
            self.count += len(results)

            if self.verbose:
                print(
                    f"Finished scraping for Year: {year}  | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                )
