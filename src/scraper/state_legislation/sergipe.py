import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper


# Type mappings for Sergipe - these would need to be determined from the API
TYPES = {
    "Lei Ordinária": 3,
    "Lei Complementar": 2,
    "Decreto": 4,
    "Emenda Constitucional": 8,
}

# TODO: logic to get constitution is different, implement it (https://legison.pge.se.gov.br/Public/GetConstituicao)

# For Sergipe, situations come from the API response's "situacao" field
# We'll define valid/invalid based on common patterns
VALID_SITUATIONS = {
    0: "Não consta",
    1: "Em Vigor",
}

INVALID_SITUATIONS = {
    2: "Revogado",
}

SITUATIONS = VALID_SITUATIONS | INVALID_SITUATIONS


class SergipeLegsonScraper(BaseScaper):
    """Webscraper for Sergipe state legislation website (https://legison.pge.se.gov.br/)

    Example search request: POST to https://legison.pge.se.gov.br/Public/Consulta
    """

    def __init__(
        self,
        base_url: str = "https://legison.pge.se.gov.br",
        **kwargs: Any,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "SERGIPE"
        self.search_url = f"{self.base_url}/Public/Consulta"
        self.doc_content_url = f"{self.base_url}/Public/GetConteudoAto"
        self._initialize_saver()
        self._fetch_constitution()

    def _format_search_url(self, norm_type_id: str, year: int, page: int = 1) -> str:
        """Format url for search request - returns the search URL"""
        return self.search_url

    def _format_search_payload(self, norm_type_id: str, year: int, page: int = 1) -> dict:
        """Format payload for search request"""
        return {
            "Ementa": "",
            "IdIniciativa": 0,
            "IdOrgao": 0,
            "IdTema": 0,
            "IdTipo": str(norm_type_id),
            "IdUsuarioCriador": 0,
            "Numero": "",
            "Order": "desc",
            "Page": page,
            "PalavrasChave": "",
            "TipoPesquisa": 1,
            "ano": str(year),
            "consolidado": False,
            "searchEmenta": False,
            "searchTexto": False,
            "termos": "",
        }

    def _get_docs_links(self, payload: dict) -> list:
        """Get document links from search request. Returns list of document dictionaries"""        
        response = self._make_request(self.search_url, method="POST", json=payload)
        if not response:
            return []
        
        try:
            data = response.json()
        except Exception as e:
            if self.verbose:
                print(f"Error parsing JSON response: {e}")
            return []
        
        if "result" not in data:
            return []
        
        docs = []
        for item in data["result"]:
            # Extract document information
            doc_id = item.get("id")
            numero = item.get("numero", "")
            data_ato = item.get("dataAto", "")
            ementa = item.get("ementa", "")
            tipo_ato = item.get("tipoAto", {})
            tipo_descricao = tipo_ato.get("descricao", "") if tipo_ato else ""
            
            # Format title
            title = f"{tipo_descricao} {numero}"
            if data_ato:
                # Extract year from date (format: "1989-12-21T00:00:00")
                year_from_date = data_ato.split("-")[0] if "-" in data_ato else ""
                title += f" de {year_from_date}"
            
            # Determine situation based on situacao ID
            situacao = item.get("situacao", {})
            situation = "Não consta"  # Default
            if "id" in situacao:
                situation = self.situations.get(situacao["id"], "Não consta")
            
            doc = {
                "id": str(doc_id),
                "title": title,
                "summary": ementa,
                "date": data_ato,
                "situation": situation,
                "doc_id": doc_id,  # Keep original ID for getting content
            }
            docs.append(doc)
        
        return docs

    def _get_doc_data(self, doc_info: dict) -> Optional[dict]:
        """Get document data by fetching PDF content and converting to markdown"""
        doc_id = doc_info.pop("doc_id", None)
        if not doc_id:
            return None
        
        retries = 3
        response = None
        
        content_url = f"{self.doc_content_url}?atosIds={doc_id}"
        for attempt in range(retries):
        # Get document content info
            response = self._make_request(content_url)
            if response and response.status_code == 200:
                break
            if self.verbose:
                print(f"Failed to get document content: Attempt {attempt + 1}/{retries}")
            time.sleep(5 ** attempt)  # Exponential backoff
        
        if not response:
            return None
        
        try:
            data = response.json()
        except Exception as e:
            if self.verbose:
                print(f"Error parsing content JSON response: {e}")
            return None
        
        # Get PDF path from files section
        if "files" in data and data["files"]:
            file_data = data["files"][0]
            caminho_pdf = file_data.get("caminhoPDF")
            
            if caminho_pdf:
                # Construct PDF URL
                pdf_url = f"{self.base_url}/uploads/atos/{doc_id}/{caminho_pdf}"
                
                # Download and process PDF
                pdf_response = self._make_request(pdf_url)
                if pdf_response:
                    # Convert PDF to markdown
                    text_markdown = self._get_markdown(response=pdf_response)
                    
                    if not text_markdown or not text_markdown.strip():
                        # Try image extraction if regular PDF extraction fails
                        text_markdown = self._get_pdf_image_markdown(pdf_response.content)
                    
                    if text_markdown and text_markdown.strip():
                        doc_info.update({
                            "text_markdown": text_markdown,
                            "document_url": pdf_url,
                        })
                        return doc_info
        
        return None

    def _get_total_count(self, norm_type_id: str, year: int) -> int:
        """Get total count of documents for a type and year"""
        payload = self._format_search_payload(norm_type_id, year, 1)
        
        retries = 5
        response = None
        for attempt in range(retries):
            response = self._make_request(self.search_url, method="POST", json=payload)
            if not response or response.status_code != 200:
                print("Failed to get total count: No response")
                if response:
                    print(f"Response error: {response.status_code} - {response.text}")
                time.sleep(5 ** attempt)  # Exponential backoff
                continue
            
        if not response:
            return 0
        
        try:
            data = response.json()
            return data.get("count", 0)
        except Exception:
            return 0

    def _get_all_pages_docs(self, norm_type_id: str, year: int) -> list:
        """Get documents from all pages for a given type and year"""
        # Get total count first
        total_count = self._get_total_count(norm_type_id, year)
        if total_count == 0:
            return []
        
        # Calculate total pages (assuming 10 items per page based on API behavior)
        page_size = 10 
        total_pages = (total_count + page_size - 1) // page_size
        
        all_docs = []
        
        # Get all pages in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            page_futures = [
                executor.submit(
                    self._get_docs_links,
                    self._format_search_payload(norm_type_id, year, page)
                )
                for page in range(1, total_pages + 1)
            ]
            
            for future in tqdm(
                as_completed(page_futures),
                desc="SERGIPE | Get document links (pages)",
                total=len(page_futures),
                disable=not self.verbose,
            ):
                docs = future.result()
                if docs:
                    all_docs.extend(docs)
        
        return all_docs
    
    def _fetch_constitution(self):
        """Fetch the state constitution document"""
        constitution_url = f"{self.base_url}/Public/GetConstituicao"
        response = self._make_request(constitution_url)
        
        data = response.json() if response else {}
        
        file_path = data.get("arquivoAtualizado")
        doc_id = data.get("id")
        if file_path:
            file_url = f"{self.base_url}/uploads/constituicao/{doc_id}/atualizado/{file_path}"
            text_markdown = self._get_markdown(url=file_url)
            
            doc_info = {
                "id": doc_id,
                "year": 1989,  # Sergipe's constitution year
                "title": "Constituição Estadual de Sergipe",
                "type": "Constituição Estadual",
                "summary": data.get("ementa", ""),
                "date": data.get("dataAto", ""),
                "situation": self.situations.get(data.get("idSituacao"), "Em Vigor"),
                "text_markdown": text_markdown,
                "document_url": file_url
            }
            
            self.queue.put(doc_info)
            self.results.append(doc_info)
            self.count += 1
            if self.verbose:
                print(f"Fetched constitution: {doc_info['title']} | ID: {doc_id} | URL: {file_url}")
        else:
            if self.verbose:
                print("No constitution file found in the response.")
             

    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
        for norm_type, norm_type_id in tqdm(
            self.types.items() if isinstance(self.types, dict) else [],
            desc=f"SERGIPE | Year: {year} | Types",
            total=len(self.types) if isinstance(self.types, dict) else 0,
            disable=not self.verbose,
        ):
            try:
                # Get all documents for this type and year
                documents = self._get_all_pages_docs(norm_type_id, year)
                
                if not documents:
                    continue

                # Process documents with threading
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    doc_data_futures = [
                        executor.submit(self._get_doc_data, doc_info.copy())
                        for doc_info in documents
                    ]

                    results = []
                    for future in tqdm(
                        as_completed(doc_data_futures),
                        desc="SERGIPE | Get document data",
                        total=len(doc_data_futures),
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
                        f"Finished scraping for Year: {year} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                    )
                    
            except Exception as e:
                if self.verbose:
                    print(f"Error scraping Year: {year} | Type: {norm_type} | Error: {e}")
                continue
