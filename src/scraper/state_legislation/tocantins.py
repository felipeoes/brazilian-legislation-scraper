import re
from io import BytesIO
from typing import Any

from bs4 import BeautifulSoup, Tag
from loguru import logger
from src.scraper.base.scraper import StateScraper


# Type mappings for Tocantins
TYPES = {
    "Lei Ordinária": "ordinaria",
    "Lei Complementar": "complementar",
}

# For Tocantins, we cannot determine situation
VALID_SITUATIONS = ["Não consta"]

INVALID_SITUATIONS = []

SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class TocantinsScraper(StateScraper):
    """Webscraper for Tocantins state legislation website (https://www.al.to.leg.br/)

    Example search request: POST to https://www.al.to.leg.br/legislacaoEstadual
    """

    def __init__(
        self,
        base_url: str = "https://www.al.to.leg.br",
        **kwargs: Any,
    ):
        super().__init__(
            base_url, types=TYPES, situations=SITUATIONS, name="TOCANTINS", **kwargs
        )
        self.search_url = f"{self.base_url}/legislacaoEstadual"

    def _format_search_url(self, _norm_type_id: str, _year: int, _page: int = 1) -> str:
        """Format url for search request - returns the search URL"""
        return self.search_url

    def _format_search_payload(
        self, norm_type_id: str, year: int, page: int = 1
    ) -> dict:
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

    async def _get_docs_links_page(
        self, norm_type_id: str, year: int, page: int = 1
    ) -> list[dict]:
        """Get document links from a single page"""
        payload = self._format_search_payload(norm_type_id, year, page)

        response = await self.request_service.make_request(
            self.search_url, method="POST", payload=payload
        )
        if not response:
            return []

        return self._extract_docs_from_soup(await response.read())

    def _extract_docs_from_soup(self, html_content: bytes) -> list[dict]:
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
                logger.error(f"Error extracting document from box: {e}")
                continue

        return docs

    async def _get_total_pages(self, norm_type_id: str, year: int) -> int:
        """Get total number of pages for a search"""
        payload = self._format_search_payload(norm_type_id, year, 1)

        response = await self.request_service.make_request(
            self.search_url, method="POST", payload=payload
        )
        if not response:
            return 1

        soup = BeautifulSoup(await response.read(), "html.parser")

        # Look for pagination navigation with "Grupo paginação"
        nav = soup.find("nav", {"aria-label": "Grupo paginação"})
        if not nav or not isinstance(nav, Tag):
            return 1

        # Find pagination links
        pagination_links = nav.find_all("a", class_="page-link")
        max_page = 1
        max_range_end = 0

        for link in pagination_links:
            if not isinstance(link, Tag):
                continue
            text = link.get_text(strip=True)
            if text.isdigit():
                max_page = max(max_page, int(text))
                continue

            # Some pages show record ranges like "1-10", "11-20", etc.
            range_match = re.fullmatch(r"(\d+)\s*-\s*(\d+)", text)
            if range_match:
                max_range_end = max(max_range_end, int(range_match.group(2)))

        if max_page == 1 and max_range_end > 0:
            # Range labels refer to record offsets; convert to page count.
            return (max_range_end + 9) // 10

        return max_page

    async def _get_docs_links(self, norm_type_id: str, year: int) -> list[dict]:
        """Get all document links for a type and year using async processing"""
        # Get total pages first
        total_pages = await self._get_total_pages(norm_type_id, year)

        if total_pages <= 1:
            # Single page, process directly
            return await self._get_docs_links_page(norm_type_id, year, 1)

        all_docs = []

        # Process all pages concurrently
        tasks = [
            self._get_docs_links_page(norm_type_id, year, page)
            for page in range(1, total_pages + 1)
        ]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": "N/A", "situation": "N/A"},
            desc="TOCANTINS | get_docs_links",
        )
        for result in valid_results:
            if result:
                all_docs.extend(result)

        return all_docs

    async def _get_doc_data(self, doc_info: dict) -> dict | None:
        """Get document data by downloading PDF and converting to markdown"""
        pdf_link = doc_info.get("pdf_link")
        title = doc_info.get("title", "")
        if not pdf_link:
            await self._save_doc_error(
                title=title,
                year=doc_info.get("year", ""),
                html_link="",
                error_message="Missing PDF link",
            )
            return None

        if self._is_already_scraped(pdf_link, title):
            return None

        try:
            # Download PDF
            pdf_response = await self.request_service.make_request(pdf_link)
            if not pdf_response:
                await self._save_doc_error(
                    title=title,
                    year=doc_info.get("year", ""),
                    html_link=pdf_link,
                    error_message="Failed to download PDF",
                )
                return None

            pdf_content = await pdf_response.read()

            # Convert PDF to markdown
            text_markdown = await self._get_markdown(stream=BytesIO(pdf_content))

            if not text_markdown or not text_markdown.strip():
                await self._save_doc_error(
                    title=title,
                    year=doc_info.get("year", ""),
                    html_link=pdf_link,
                    error_message="Empty markdown from PDF",
                )
                return None

            # Remove pdf_link from doc_info and add processed data
            doc_info.pop("pdf_link", None)
            doc_info.update(
                {
                    "text_markdown": text_markdown,
                    "document_url": pdf_link,
                    "_raw_content": pdf_content,
                    "_content_extension": ".pdf",
                }
            )

            return doc_info

        except Exception as e:
            logger.error(f"Error processing document {title}: {e}")
            await self._save_doc_error(
                title=title,
                year=doc_info.get("year", ""),
                html_link=pdf_link,
                error_message=f"Exception processing document: {e}",
            )
            return None

    async def _fetch_constitution(self):
        """Fetch the Tocantins state constitution"""
        pdf_link = f"{self.base_url}/arquivos/documento_68367.PDF#dados"
        title = "Constituição Estadual de Tocantins"

        if self._is_already_scraped(pdf_link, title):
            if self.verbose:
                logger.info("Tocantins constitution already scraped, skipping")
            return

        text_markdown, raw_content, content_ext = await self._download_and_convert(
            pdf_link
        )
        if not text_markdown or not text_markdown.strip():
            logger.error("Failed to fetch Tocantins constitution text")
            return

        doc_info = {
            "title": title,
            "summary": "Constituição do Estado do Tocantins",
            "type": "Constituição Estadual",
            "date": "05/10/1989",
            "year": 1989,
            "situation": "Não consta revogação expressa",
            "text_markdown": text_markdown,
            "document_url": pdf_link,
            "_raw_content": raw_content,
            "_content_extension": content_ext,
        }

        saved = await self._save_doc_result(doc_info)
        if saved is not None:
            doc_info = saved
        self._track_results([doc_info])
        self.count += 1
        if self.verbose:
            logger.info("Fetched Tocantins constitution successfully")

    async def _scrape_type(
        self, norm_type: str, norm_type_id: str, year: int
    ) -> list[dict]:
        """Scrape norms for a specific type and year"""
        try:
            # Get all documents for this type and year
            documents = await self._get_docs_links(norm_type_id, year)

            if not documents:
                return []

            for doc in documents:
                doc["year"] = year
            ctx = {"year": year, "type": norm_type, "situation": "N/A"}
            tasks = [
                self._with_save(self._get_doc_data(doc_info.copy()), ctx)
                for doc_info in documents
            ]
            results = await self._gather_results(
                tasks,
                context=ctx,
                desc=f"TOCANTINS | {norm_type}",
            )

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

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape norms for a specific year, fetching constitution on first call."""
        if not hasattr(self, "_constitution_fetched"):
            await self._fetch_constitution()
            self._constitution_fetched = True
        return await super()._scrape_year(year)
