"""Base scraper for SAPL (Sistema de Apoio ao Processo Legislativo) API sites.

Many Brazilian state legislatures use SAPL with identical REST API structures.
This base class provides the shared logic for:
  - Paginated norm search via ``/api/norma/normajuridica/``
  - Subject (assunto) fetching via ``/api/norma/assuntonorma/``
  - PDF processing with markitdown → OCR fallback
"""

from __future__ import annotations

from typing import Any

from loguru import logger
from src.scraper.base.scraper import StateScraper


class SAPLBaseScraper(StateScraper):
    """Base scraper for SAPL API-based state legislation sites.

    Subclasses must set:
        - ``TYPES``: dict mapping norm type names to API IDs
        - ``base_url`` (via ``__init__`` default)
        - ``name`` (via ``__init__`` default)

    Subclasses may override:
        - ``_process_pdf`` for state-specific PDF handling strategies
    """

    def __init__(self, base_url: str, name: str, types: dict, **kwargs: Any):
        super().__init__(base_url, types=types, situations=[], name=name, **kwargs)
        self.subjects: dict[int, str] = {}

    # ------------------------------------------------------------------
    # URL formatting
    # ------------------------------------------------------------------

    def _format_search_url(
        self,
        norm_type_id: str,
        year: int,
        page: int = 1,
    ) -> str:
        """Format URL for SAPL norm search API."""
        return f"{self.base_url}/api/norma/normajuridica/?tipo={norm_type_id}&page={page}&ano={year}"

    # ------------------------------------------------------------------
    # Document link extraction
    # ------------------------------------------------------------------

    async def _get_docs_links(self, url: str) -> list:
        """Parse document list from a single SAPL API page."""
        response = await self.request_service.make_request(url)
        if not response:
            return []

        items = (await response.json()).get("results", [])
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

    # ------------------------------------------------------------------
    # PDF processing (override in subclasses for custom strategies)
    # ------------------------------------------------------------------

    async def _process_pdf(self, pdf_link: str, _year: int) -> dict | None:
        """Download and convert a PDF to markdown.

        Default: tries markitdown, then falls back to LLM OCR.
        Override in subclasses for year-based or threshold-based strategies.
        """
        text_markdown, raw_bytes, ext = await self._download_and_convert(pdf_link)
        if not text_markdown or not text_markdown.strip():
            return None
        return {
            "text_markdown": text_markdown.strip(),
            "document_url": pdf_link,
            "_raw_content": raw_bytes,
            "_content_extension": ext,
        }

    async def _get_doc_data(self, doc_info: dict, year: int = 0) -> dict | None:
        """Get full document data by processing the PDF attachment."""
        pdf_link = doc_info.pop("pdf_link")
        title = doc_info.get("title", "")

        if self._is_already_scraped(pdf_link, title):
            return None

        processed = await self._process_pdf(pdf_link, year)
        if processed is None:
            await self._save_doc_error(
                title=title,
                year=year or doc_info.get("date", ""),
                situation=doc_info.get("situation", ""),
                norm_type=doc_info.get("type", ""),
                html_link=pdf_link,
                error_message="PDF processing failed (no text extracted)",
            )
            return None
        doc_info.update(processed)

        saved = await self._save_doc_result(doc_info)
        if saved is not None:
            doc_info = saved

        return doc_info

    # ------------------------------------------------------------------
    # Subject fetching
    # ------------------------------------------------------------------

    async def _fetch_subjects(self) -> None:
        """Fetch all subjects (assuntos) from the SAPL API concurrently."""
        if self.subjects:
            return

        subjects_url = f"{self.base_url}/api/norma/assuntonorma/"
        response = await self.request_service.make_request(subjects_url)
        if not response:
            return

        data = await response.json()
        total_pages = data["pagination"]["total_pages"]

        subjects = {item["id"]: item["assunto"] for item in data["results"]}

        # Fetch remaining pages concurrently
        if total_pages > 1:
            tasks = [
                self.request_service.make_request(f"{subjects_url}?page={page}")
                for page in range(2, total_pages + 1)
            ]
            results = await self._gather_results(
                tasks,
                desc=f"{self.name} | Fetching subjects",
            )
            for resp in results:
                if resp:
                    page_data = await resp.json()
                    subjects.update(
                        {item["id"]: item["assunto"] for item in page_data["results"]}
                    )

        self.subjects = subjects

    # ------------------------------------------------------------------
    # Type-level scraping
    # ------------------------------------------------------------------

    async def _scrape_type(
        self, norm_type: str, norm_type_id: int, year: int
    ) -> list[dict]:
        """Scrape all norms of a given type for a year."""
        url = self._format_search_url(norm_type_id, year)
        response = await self.request_service.make_request(url)

        if not response or response.status not in (200,):
            status = response.status if response else "No response"
            logger.error(
                f"Error fetching data for Year: {year} | Type: {norm_type} | Status: {status}"
            )
            return []

        data = await response.json()
        if not data.get("results"):
            return []

        total_pages = data["pagination"]["total_pages"]

        # Fetch document links from all pages concurrently
        link_tasks = [
            self._get_docs_links(self._format_search_url(norm_type_id, year, page=page))
            for page in range(1, total_pages + 1)
        ]
        link_results = await self._gather_results(
            link_tasks,
            context={"year": year, "type": norm_type, "situation": "NA"},
            desc=f"{self.name} | {norm_type} | get_docs_links",
        )
        documents = [doc for result in link_results for doc in (result or [])]

        # Skip already-scraped documents to avoid unnecessary work
        documents = [
            doc
            for doc in documents
            if not self._is_already_scraped(
                doc.get("pdf_link", ""), doc.get("title", "")
            )
        ]

        # Process all documents concurrently
        doc_tasks = [self._get_doc_data(doc, year) for doc in documents]
        doc_results = await self._gather_results(
            doc_tasks,
            context={"year": year, "type": norm_type, "situation": "NA"},
            desc=f"{self.name} | {norm_type}",
        )

        results = [{"year": year, "type": norm_type, **r} for r in doc_results if r]

        if self.verbose:
            logger.info(
                f"Finished scraping for Year: {year} | Type: {norm_type} | Results: {len(results)}"
            )

        return results

    # ------------------------------------------------------------------
    # Year-level orchestration
    # ------------------------------------------------------------------

    async def _scrape_year(self, year: int) -> list[dict]:
        """Fetch subjects then scrape all types concurrently."""
        await self._fetch_subjects()
        return await super()._scrape_year(year)
