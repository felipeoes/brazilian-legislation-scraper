import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper


TYPES = {
    "Decreto-Lei": "declei",
    "Lei Complementar": "leicomp",
    "Lei Ordinária": "leiord",
    'Decreto Numerado': "decnum",
}

# Cannot determine revocation status from the website, so the situation is hardcoded as "Não consta"
SITUATIONS = []


class RondoniaCotelScraper(BaseScaper):
    """Webscraper for Rondônia state legislation website (http://ditel.casacivil.ro.gov.br/)

    Example search request: http://ditel.casacivil.ro.gov.br/COTEL/Livros/listdeclei.aspx?ano=2025
    """

    def __init__(
        self,
        base_url: str = "http://ditel.casacivil.ro.gov.br/COTEL",
        **kwargs: Any,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "RONDONIA"
        self._initialize_saver()
        self._fetch_constitution()

    def _format_search_url(self, norm_type_id: str, year: int) -> str:
        """Format url for search request"""
        return f"{self.base_url}/Livros/list{norm_type_id}.aspx?ano={year}"

    def _get_docs_links(self, url: str) -> list:
        """Get document links from search request."""
        soup = self._get_soup(url)
        docs = []

        if not soup:
            return []

        # Find the main table with id="ContentPlaceHolder1_DataList1"
        table = soup.find("table", {"id": "ContentPlaceHolder1_DataList1"})
        if not table or not isinstance(table, Tag):
            return []

        tbody = table.find("tbody")
        if tbody and isinstance(tbody, Tag):
            rows = tbody.find_all("tr")
        else:
            rows = table.find_all("tr")
        
        for row in rows:
            if not isinstance(row, Tag):
                continue
                
            cell = row.find("td")
            if not cell or not isinstance(cell, Tag):
                continue

            div = cell.find("div")
            if not div or not isinstance(div, Tag):
                continue

            # Extract title and norm number from the main link
            title_links = div.find_all("a")
            title_link = None
            pdf_link = None
            
            for link in title_links:
                if not isinstance(link, Tag):
                    continue
                    
                href = link.get("href")
                if href and isinstance(href, str):
                    if "detalhes.aspx" in href:
                        title_link = link
                    elif href.endswith(".pdf"):
                        pdf_link = link

            if not title_link or not pdf_link:
                continue

            title = title_link.get_text(strip=True)
            
            pdf_href = pdf_link.get("href")
            if pdf_href and isinstance(pdf_href, str):
                if not pdf_href.startswith("http"):
                    pdf_href = urljoin(self.base_url, pdf_href)
            else:
                continue

            # Extract summary (ementa)
            summary_spans = div.find_all("span")
            summary = ""
            doc_id = ""
            
            for span in summary_spans:
                if not isinstance(span, Tag):
                    continue
                    
                span_id = span.get("id")
                if span_id and isinstance(span_id, str):
                    if "ementadoc" in span_id:
                        summary = span.get_text(strip=True)
                    elif "coddocLabel" in span_id:
                        doc_id = span.get_text(strip=True)

            # get last part of pdf (filename) to join
            pdf_href = pdf_href.split("/")[-1]
            doc = {
                "id": doc_id,
                "title": title,
                "situation": "Não consta", # we cannot determine revocation status
                "summary": summary,
                "pdf_link": f"{self.base_url}/Livros/Files/{pdf_href}",
            }
            docs.append(doc)

        return docs

    def _process_pdf(self, pdf_link: str) -> Optional[dict]:
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
                text_markdown = self._get_pdf_image_markdown(response.content)

        if not text_markdown or not text_markdown.strip():
            return None

        return {
            "text_markdown": text_markdown,
            "document_url": pdf_link,
        }

    def _get_doc_data(self, doc_info: dict) -> Optional[dict]:
        """Get document data"""
        pdf_link = doc_info.pop("pdf_link")
        processed_pdf = self._process_pdf(pdf_link)

        if processed_pdf is None:
            return None

        doc_info.update(processed_pdf)
        return doc_info
    
    def _fetch_constitution(self):
        """Fetch the state constitution if available."""
        pdf_url = f"{self.base_url}/Livros/CE1989-2014.pdf"
        
        text_markdown = self._get_markdown(url=pdf_url)
        
        doc_info = {
            "year": datetime.now().year,
            "type": "Constituição Estadual",
            "title": "Constituição do Estado de Rondônia",
            "norm_number": "CE1989-2014",
            "situation": "Não consta revogação expressa",
            "summary": "Constituição do Estado de Rondônia",
            "text_markdown": text_markdown,
            "document_url": pdf_url,
        }
        
        self.queue.put(doc_info)
        self.results.append(doc_info)
        self.count += 1
        print("Scraped state constitution")
         

    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
        for norm_type, norm_type_id in tqdm(
            self.types.items() if isinstance(self.types, dict) else [],
            desc=f"RONDONIA | Year: {year} | Types",
            total=len(self.types) if isinstance(self.types, dict) else 0,
            disable=not self.verbose,
        ):
            url = self._format_search_url(norm_type_id, year)
            
            try:
                documents = self._get_docs_links(url)
                
                if not documents:
                    continue

                # Process documents with threading
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    doc_data_futures = [
                        executor.submit(self._get_doc_data, doc_info)
                        for doc_info in documents
                    ]

                    results = []
                    for future in tqdm(
                        as_completed(doc_data_futures),
                        desc="RONDONIA | Get document data",
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