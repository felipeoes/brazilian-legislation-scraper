from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, List, Optional

from bs4 import BeautifulSoup, Tag
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper


# Type mappings for Tocantins
TYPES = {
    "Lei Ordinária": "ordinaria",
    "Lei Complementar": "complementar",
}

# For Tocantins, we cannot determine situation
VALID_SITUATIONS = [
    "Não consta"
]

INVALID_SITUATIONS = []

SITUATIONS = VALID_SITUATIONS  + INVALID_SITUATIONS


class TocantinsScraper(BaseScaper):
    """Webscraper for Tocantins state legislation website (https://www.al.to.leg.br/)

    Example search request: POST to https://www.al.to.leg.br/legislacaoEstadual
    """

    def __init__(
        self,
        base_url: str = "https://www.al.to.leg.br",
        **kwargs: Any,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "TOCANTINS"
        self.search_url = f"{self.base_url}/legislacaoEstadual"
        self._initialize_saver()
        self._fetch_constitution()

    def _format_search_url(self, norm_type_id: str, year: int, page: int = 1) -> str:
        """Format url for search request - returns the search URL"""
        return self.search_url

    def _format_search_payload(self, norm_type_id: str, year: int, page: int = 1) -> dict:
        """Format payload for search request"""
        return {
            "pagPaginaAtual": str(page),
            "documento.texto": "",
            "documento.numero": "",
            "documento.ano": str(year),
            "documento.dataInicio": "",
            "documento.dataFinal": "",
            "documento.tipo": norm_type_id,
        }

    def _get_docs_links_page(self, norm_type_id: str, year: int, page: int = 1) -> List[dict]:
        """Get document links from a single page"""
        payload = self._format_search_payload(norm_type_id, year, page)
        
        response = self._make_request(self.search_url, method="POST", payload=payload)
        if not response:
            return []
        
        return self._extract_docs_from_soup(response.content)

    def _extract_docs_from_soup(self, html_content: bytes) -> List[dict]:
        """Extract document information from HTML content"""
        soup = BeautifulSoup(html_content, "html.parser")
        
        docs = []
        # Find all document boxes
        rows = soup.find_all("div", class_="row")
        
        for row in rows:
            try:
                # Extract title and link from h4 > a
                title_link = row.find("h4")
                if not title_link or not isinstance(title_link, Tag):
                    continue
                
                link_tag = title_link.find("a")
                if not link_tag or not isinstance(link_tag, Tag):
                    continue
                
                title = link_tag.get_text(strip=True)
                doc_link = link_tag.get("href", "")
                
                if not isinstance(doc_link, str):
                    continue
                    
                if not doc_link.startswith("http"):
                    doc_link = f"{self.base_url}{doc_link}"
                
                # Extract date from the small text
                date_text = ""
                small_tags = row.find_all("small")
                for small in small_tags:
                    if isinstance(small, Tag):
                        text = small.get_text(strip=True)
                        if "Data:" in text:
                            date_text = text.replace("Data:", "").strip()
                            # Clean up any extra characters like "|"
                            date_text = date_text.replace("|", "").strip()
                            break
                
                # Extract summary from em > strong
                summary = ""
                em_tag = row.find("em")
                if em_tag and isinstance(em_tag, Tag):
                    strong_tag = em_tag.find("strong")
                    if strong_tag and isinstance(strong_tag, Tag):
                        summary = strong_tag.get_text(strip=True)
                
                # Extract PDF download link
                pdf_link = ""
                pdf_link_tag = row.find("a", {"title": "Download"})
                if pdf_link_tag and isinstance(pdf_link_tag, Tag):
                    pdf_href = pdf_link_tag.get("href", "")
                    if isinstance(pdf_href, str):
                        if not pdf_href.startswith("http"):
                            pdf_link = f"{self.base_url}{pdf_href}"
                        else:
                            pdf_link = pdf_href
                
                
                doc = {
                    "title": title,
                    "summary": summary,
                    "date": date_text,
                    "situation": VALID_SITUATIONS[0],
                    "pdf_link": pdf_link,
                }
                docs.append(doc)
                
            except Exception as e:
                if self.verbose:
                    print(f"Error extracting document from box: {e}")
                continue
        
        return docs

    def _get_total_pages(self, norm_type_id: str, year: int) -> int:
        """Get total number of pages for a search"""
        payload = self._format_search_payload(norm_type_id, year, 1)
        
        response = self._make_request(self.search_url, method="POST", payload=payload)
        if not response:
            return 1
        
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Look for pagination navigation with "Grupo paginação"
        nav = soup.find("nav", {"aria-label": "Grupo paginação"})
        if not nav or not isinstance(nav, Tag):
            return 1
        
        # Find pagination links
        pagination_links = nav.find_all("a", class_="page-link")
        max_page = 1
        
        for link in pagination_links:
            if not isinstance(link, Tag):
                continue
            text = link.get_text(strip=True)
            # Look for patterns like "1-10", "11-20", etc.
            if "-" in text and text.replace("-", "").isdigit():
                # Extract the end number
                try:
                    end_num = int(text.split("-")[1])
                    max_page = max(max_page, end_num)
                except (ValueError, IndexError):
                    continue
        
        return max_page

    def _get_docs_links(self, norm_type_id: str, year: int) -> List[dict]:
        """Get all document links for a type and year using parallel processing"""
        # Get total pages first
        total_pages = self._get_total_pages(norm_type_id, year)
        
        if total_pages <= 1:
            # Single page, process directly
            return self._get_docs_links_page(norm_type_id, year, 1)
        
        all_docs = []
        
        # Process all pages in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            page_futures = [
                executor.submit(self._get_docs_links_page, norm_type_id, year, page)
                for page in range(1, total_pages + 1)
            ]
            
            for future in tqdm(
                as_completed(page_futures),
                desc="TOCANTINS | Get document links (pages)",
                total=len(page_futures),
                disable=not self.verbose,
            ):
                docs = future.result()
                if docs:
                    all_docs.extend(docs)
        
        return all_docs

    def _get_doc_data(self, doc_info: dict) -> Optional[dict]:
        """Get document data by downloading PDF and converting to markdown"""
        pdf_link = doc_info.get("pdf_link")
        if not pdf_link:
            return None
        
        try:
            # Download PDF
            pdf_response = self._make_request(pdf_link)
            if not pdf_response:
                return None
            
            # Convert PDF to markdown
            text_markdown = self._get_markdown(response=pdf_response)
            
            if not text_markdown or not text_markdown.strip():
                # Try image extraction if regular PDF extraction fails
                text_markdown = self._get_pdf_image_markdown(pdf_response.content)
            
            if not text_markdown or not text_markdown.strip():
                return None
            
            # Remove pdf_link from doc_info and add processed data
            doc_info.pop("pdf_link", None)
            doc_info.update({
                "text_markdown": text_markdown,
                "document_url": pdf_link,
            })
            
            return doc_info
            
        except Exception as e:
            if self.verbose:
                print(f"Error processing document {doc_info.get('title', 'Unknown')}: {e}")
            return None
        
    def _fetch_constitution(self):
        """Fetch the Tocantins state constitution"""
        pdf_link = f"{self.base_url}/arquivos/documento_68367.PDF#dados"
        text_markdown = self._get_markdown(pdf_link)
        if not text_markdown or not text_markdown.strip():
            print("Failed to fetch Tocantins constitution text")
            return
        
        doc_info = {
            "title": "Constituição Estadual de Tocantins",
            "summary": "Constituição do Estado do Tocantins",
            "type": "Constituição Estadual",
            "date": "05/10/1989",
            "year": 1989,
            "situation": "Não consta revogação expressa",
            "text_markdown": text_markdown,
            "document_url": pdf_link,
        }
         
        self.queue.put(doc_info)
        self.results.append(doc_info)
        self.count += 1
        print("Fetched Tocantins constitution successfully")
        


    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
        for norm_type, norm_type_id in tqdm(
            self.types.items() if isinstance(self.types, dict) else [],
            desc=f"TOCANTINS | Year: {year} | Types",
            total=len(self.types) if isinstance(self.types, dict) else 0,
            disable=not self.verbose,
        ):
            try:
                # Get all documents for this type and year
                documents = self._get_docs_links(norm_type_id, year)
                
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
                        desc="TOCANTINS | Get document data",
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
