from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List

from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper
from src.database.saver import FileSaver


# gotten from https://sapl3.al.pb.leg.br/api/norma/tiponormajuridica/

TYPES = {
    "Ação Direta de Inconstitucionalidade Estadual": 15,
    "Constituição Estadual": 5,
    "Decreto Executivo": 18,
    "Decreto Legislativo": 6,  # the record with id == 3 is invalid, thus using 6
    "Decreto-Lei": 17,
    "Emenda Constitucional": 9,
    "Lei Complementar": 1,
    "Lei Ordinária": 2,
    "Lei Ordinária Promulgada": 8,
    "Regimento Interno": 14,
    "Resolução": 4,
}

SITUATIONS = []

class ParaibaAlpbScraper(BaseScaper):
    """Webscraper for Paraíba state legislation website (https://sapl3.al.pb.leg.br/)

    Example search request: https://sapl3.al.pb.leg.br/api/norma/normajuridica/?tipo=2&page=3&ano=2025

    params = {
        tipo: 2
        page: 3
        ano: 2025
    }
    """

    def __init__(
        self,
        base_url: str = "https://sapl3.al.pb.leg.br",
        **kwargs: Any,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "PARAIBA"
        self.subjects: Dict[int, str] = {}
        # Initialize the FileSaver
        self.saver = FileSaver(self.docs_save_dir)

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

    def _process_pdf(self, pdf_link: str, year: int) -> dict:
        """Process PDF and return text markdown."""
        response = self._make_request(pdf_link)
        if not response.content:
            return None

        if year <= 1990:
            text_markdown = self._get_pdf_image_markdown(response.content)
        else:
            text_markdown = self._get_markdown(response=response)
            if not text_markdown:
                text_markdown = self._get_pdf_image_markdown(response.content)

        if not text_markdown or not text_markdown.strip():
            return None

        return {
            "text_markdown": text_markdown,
            "document_url": pdf_link,
        }

    def _get_doc_data(self, doc_info: dict, year: int) -> dict:
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
        data = response.json()
        total_pages = data["pagination"]["total_pages"]

        subjects = {item["id"]: item["assunto"] for item in data["results"]}

        for page in tqdm(
            range(2, total_pages + 1),
            desc="PARAIBA | Fetching subjects",
            total=total_pages,
            disable=not self.verbose,
        ):
            response = self._make_request(f"{subjects_url}?page={page}")
            data = response.json()
            subjects.update(
                {item["id"]: item["assunto"] for item in data["results"]}
            )

        self.subjects = subjects

    def _scrape_year(self, year: int) -> list:
        """Scrape norms for a specific year"""
        all_results = []
        self._fetch_subjects()

        for norm_type, norm_type_id in tqdm(
            self.types.items(),
            desc=f"PARAIBA | Year: {year} | Types",
            total=len(self.types),
            disable=not self.verbose,
        ):
            url = self._format_search_url(norm_type_id, year)
            response = self._make_request(url)

            if response.status_code == 400:
                continue

            data = response.json()
            if not data.get("results"):
                continue

            total_pages = data["pagination"]["total_pages"]
            
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
                    desc="PARAIBA | Get document links",
                    total=len(doc_links_futures),
                    disable=not self.verbose,
                ):
                    documents.extend(future.result())

                doc_data_futures = [
                    executor.submit(self._get_doc_data, doc_info, year)
                    for doc_info in documents
                ]

                results = []
                for future in tqdm(
                    as_completed(doc_data_futures),
                    desc="PARAIBA | Get document data",
                    total=len(doc_data_futures),
                    disable=not self.verbose,
                ):
                    result = future.result()
                    if result:
                        queue_item = {"year": year, "type": norm_type, **result}
                        results.append(queue_item)

            all_results.extend(results)
            self.count += len(results)

            if self.verbose:
                print(
                    f"Finished scraping for Year: {year}  | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                )
                
        return all_results
