import re
from typing import Any

from bs4 import BeautifulSoup, Tag
from tqdm import tqdm
from loguru import logger
from src.scraper.base.scraper import StateScraper


TYPES = {
    "Constituição Estadual": "Estadual",
    "Decreto-Lei": "Decreto-Lei",
    "Decreto Executivo": "Decreto Executivo",
    "Decreto Legislativo": "Decreto Legislativo",
    "Emenda Constitucional": "Emenda Constitucional",
    "Instrução Normativa": "Normativa",
    "Lei Ordinária": "Ordinaria",
    "Lei Complementar": "Lei Complementar",
    "Portaria": "Portaria",
    "Resolução": "Resolucao",
}

# Cannot determine revocation status from the website, so the situation is hardcoded as "Não consta"
SITUATIONS = []


class SantaCatarinaScraper(StateScraper):
    """Webscraper for Santa Catarina state legislation website (http://server03.pge.sc.gov.br/pge/normasjur.asp)

    Example search request: POST to http://server03.pge.sc.gov.br/pge/normasjur.asp
    """

    def __init__(
        self,
        base_url: str = "http://server03.pge.sc.gov.br/pge/normasjur.asp",
        **kwargs: Any,
    ):
        super().__init__(
            base_url,
            types=TYPES,
            situations=SITUATIONS,
            name="SANTA_CATARINA",
            **kwargs,
        )

    def _format_search_url(self, _norm_type_id: str, _year: int) -> str:
        """Format url for search request - returns base URL since we use POST"""
        return self.base_url

    def _format_search_payload(self, norm_type_id: str, year: int) -> dict:
        """Format payload for search request"""
        return {
            "pTipoNorma": norm_type_id,
            "pVigente": "Todas",
            "pNumero": "",
            "pEmissaoInicio": str(year),
            "pEmissaoFim": str(year),
            "pArtigo": "",
            "pEmenta": "",
            "pIndex": "",
            "pCatalogo": "",
            "pConteudo": "",
            "Action": "Pesquisar",
        }

    def _format_pagination_params(
        self, norm_type_id: str, year: int, page: int
    ) -> dict:
        """Format parameters for pagination GET request"""
        return {
            "qu": f"%40TipoNorma+{norm_type_id}+AND+%24AnoNorma+%3E%3D+{year}+AND+%24AnoNorma+%3C%3D+{year}",
            "pTipoNorma": norm_type_id,
            "pNumero": "",
            "pEmissaoInicio": "",
            "pEmissaoFim": "",
            "pConteudo": "",
            "pEmenta": "",
            "pIndex": "",
            "pVigente": "Todas",
            "pArtigo": "",
            "pInicio": "",
            "pFim": "",
            "pCatalogo": "",
            "FreeText": "",
            "sc": "D%3A%5CSiteLegEstIIS%5Cwwwroot%5CLegislacaoEstadual",
            "RankBase": "1000",
            "pg": str(page),
        }

    async def _get_docs_links_page(
        self, norm_type_id: str, year: int, page: int = 1
    ) -> list:
        """Get document links from a specific page."""
        if page == 1:
            # First page - POST request
            payload = self._format_search_payload(norm_type_id, year)
            soup = await self.request_service.get_soup(
                self.base_url, method="POST", payload=payload
            )
        else:
            # Additional pages - GET requests
            params = self._format_pagination_params(norm_type_id, year, page)
            soup = await self.request_service.get_soup(
                self.base_url, method="GET", params=params
            )

        if not soup:
            return []

        docs = self._extract_docs_from_soup(soup)
        return docs

    async def _get_docs_links(self, norm_type_id: str, year: int) -> list:
        """Get document links from search request with pagination support."""
        # First, get the first page to determine total pages
        payload = self._format_search_payload(norm_type_id, year)
        # make get request to create a session
        soup = await self.request_service.get_soup(self.base_url, method="GET")
        soup = await self.request_service.get_soup(
            self.base_url, method="POST", payload=payload
        )

        if not soup:
            return []

        # Get documents from first page
        first_page_docs = self._extract_docs_from_soup(soup)

        if not first_page_docs:
            return []

        # Check for pagination
        total_pages = self._get_total_pages(soup)

        if total_pages <= 1:
            return first_page_docs

        # Get remaining pages
        ## Needs to be sequential because session state is needed to fetch the next pages

        all_docs = first_page_docs.copy()
        for page in tqdm(
            range(2, total_pages + 1),
            desc=f"SANTA CATARINA | Year: {year} | Type: {norm_type_id} | Fetching pages",
            total=total_pages - 1,
            disable=not self.verbose,
        ):
            docs = await self._get_docs_links_page(norm_type_id, year, page)
            all_docs.extend(docs)

        return all_docs

    def _get_total_pages(self, soup: BeautifulSoup) -> int:
        """Extract total number of pages from pagination info"""
        # Look for pagination text like "Página 2 de 2".

        # regex for "Página {x}" first
        page_regex = re.compile(r"Página\s+(\d+)")
        page_font = soup.find_all("font", string=page_regex)[0]
        next_font = page_font.find_next("font")
        if not next_font:
            return 1

        # Extract the total pages from the next font
        total_pages_text = next_font.get_text(strip=True)
        total_pages_match = re.search(r"de\s+(\d+)", total_pages_text)
        if total_pages_match:
            total_pages = int(total_pages_match.group(1))
            return total_pages

        return 1

    def _extract_docs_from_soup(self, soup: BeautifulSoup) -> list:
        """Extract document information from BeautifulSoup object"""
        docs = []

        if not soup:
            return []

        # Find all record title rows
        title_rows = soup.find_all("tr", class_="RecordTitle")

        for i, title_row in enumerate(title_rows):
            if not isinstance(title_row, Tag):
                continue

            title_tag = title_row.find("b", class_="RecordTitle")
            if not title_tag or not isinstance(title_tag, Tag):
                continue

            title_text = title_tag.get_text(strip=True).replace("\xa0", " ")

            # Parse title like "LEI-006191  9/12/1982"
            parts = title_text.split()
            if len(parts) < 2:
                continue

            doc_id = parts[0]  # LEI-006191
            date_part = parts[-1]  # 9/12/1982

            # Find the next row with ementa (summary) and "Resumo" link
            next_row = title_row.find_next_sibling("tr")
            summary = ""
            doc_info_link = ""

            if next_row and isinstance(next_row, Tag):
                # find summary_tag by searchinf gor TEXTO: or EMENTA:
                summary_tag = next_row.find(
                    lambda tag: (
                        tag.name == "font"
                        and "EMENTA:" in tag.text
                        or "TEXTO:" in tag.text
                    )
                )
                summary = summary_tag.text.strip()

                # Find the "Resumo" link which contains the doc_info_link
                doc_info_link = next_row.find("a", href=True).get("href")

            if not doc_info_link:
                continue  # Skip documents without detail links

            doc = {
                "id": doc_id,
                "title": f"{doc_id} - {date_part}",
                "summary": summary,
                "date": date_part,
                "doc_info_link": doc_info_link,
            }
            docs.append(doc)

        return docs

    async def _get_doc_data(self, doc_info: dict) -> dict | None:
        """Get document data by fetching document info page and final document content"""
        # Get the doc_info_link and remove it from the dict
        doc_info_link = doc_info.pop("doc_info_link", None)
        title = doc_info.get("title", "")
        if not doc_info_link:
            await self._save_doc_error(
                title=title,
                year=doc_info.get("year", ""),
                html_link="",
                error_message="Missing doc_info_link",
            )
            return None

        # First, get the document info page
        info_soup = await self.request_service.get_soup(doc_info_link)
        if not info_soup:
            await self._save_doc_error(
                title=title,
                year=doc_info.get("year", ""),
                html_link=doc_info_link,
                error_message="Failed to get document info page",
            )
            return None

        # Extract situation from VIGENTE field
        situation = "Não consta"
        vigente_tag = info_soup.find(
            lambda tag: tag.name == "td" and "VIGENTE:" in tag.text
        )
        vigent_text = vigente_tag.find_next("td") if vigente_tag else ""
        if vigent_text and "não" in vigent_text.text.lower():
            situation = "Revogada"
        elif vigent_text and "sim" in vigent_text.text.lower():
            situation = "Não consta revogação expressa"

        # Look for "Texto Integral" link to get the actual document
        document_url = info_soup.find("a", string="Texto Integral")["href"]

        if self._is_already_scraped(document_url or doc_info_link, title):
            return None

        # If we have the document URL, fetch the content
        text_markdown = ""
        raw_content = b""
        content_ext = ".html"
        if document_url:
            doc_soup = await self.request_service.get_soup(document_url)

            body = doc_soup.find("div", class_="Section1")
            if not body:
                await self._save_doc_error(
                    title=title,
                    year=doc_info.get("year", ""),
                    html_link=document_url,
                    error_message="No Section1 div found in document",
                )
                return None

            html_string = body.prettify().strip()
            html_string = f"<html><body>{html_string}</body></html>"

            text_markdown = await self._get_markdown(html_content=html_string)
            raw_content = html_string.encode("utf-8")

        if not text_markdown:
            await self._save_doc_error(
                title=title,
                year=doc_info.get("year", ""),
                html_link=document_url or doc_info_link,
                error_message="Empty markdown from document",
            )
            return None

        doc_info.update(
            {
                "situation": situation,
                "text_markdown": text_markdown,
                "document_url": document_url or doc_info_link,
                "_raw_content": raw_content,
                "_content_extension": content_ext,
            }
        )

        saved = await self._save_doc_result(doc_info)
        if saved is not None:
            doc_info = saved

        return doc_info

    async def _scrape_type(
        self, norm_type: str, norm_type_id: str, year: int
    ) -> list[dict]:
        """Scrape norms for a specific type and year"""
        try:
            documents = await self._get_docs_links(norm_type_id, year)

            if not documents:
                return []

            # Process documents concurrently with asyncio
            tasks = [self._get_doc_data(doc_info) for doc_info in documents]
            valid_results = await self._gather_results(
                tasks,
                context={"year": year, "type": norm_type, "situation": "N/A"},
                desc=f"SANTA CATARINA | {norm_type}",
            )
            results = []
            for result in valid_results:
                if result:
                    queue_item = {"year": year, "type": norm_type, **result}
                    results.append(queue_item)

            if self.verbose:
                logger.info(
                    f"Finished scraping for Year: {year} | Type: {norm_type} | Results: {len(results)}"
                )

            return results

        except Exception as e:
            logger.error(
                f"Error scraping Year: {year} | Type: {norm_type} | Error: {e}"
            )
            return []

    # _scrape_year uses default from BaseScraper
