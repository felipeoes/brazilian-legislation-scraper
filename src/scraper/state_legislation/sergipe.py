from io import BytesIO
from typing import Any, Optional

from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from src.scraper.base.scraper import BaseScraper, STATE_LEGISLATION_SAVE_DIR


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


class SergipeLegsonScraper(BaseScraper):
    """Webscraper for Sergipe state legislation website (https://legison.pge.se.gov.br/)

    Example search request: POST to https://legison.pge.se.gov.br/Public/Consulta
    """

    def __init__(
        self,
        base_url: str = "https://legison.pge.se.gov.br",
        **kwargs: Any,
    ):
        if STATE_LEGISLATION_SAVE_DIR:
            kwargs.setdefault("docs_save_dir", STATE_LEGISLATION_SAVE_DIR)
        super().__init__(
            base_url, types=TYPES, situations=SITUATIONS, name="SERGIPE", **kwargs
        )
        self.search_url = f"{self.base_url}/Public/Consulta"
        self.doc_content_url = f"{self.base_url}/Public/GetConteudoAto"

    def _format_search_url(self, _norm_type_id: str, _year: int, _page: int = 1) -> str:
        """Format url for search request - returns the search URL"""
        return self.search_url

    def _format_search_payload(
        self, norm_type_id: str, year: int, page: int = 1
    ) -> dict:
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

    async def _get_docs_links(self, payload: dict) -> list:
        """Get document links from search request. Returns list of document dictionaries"""
        response = await self.request_service.make_request(
            self.search_url, method="POST", json=payload
        )
        if not response:
            return []

        try:
            data = await response.json()
        except Exception as e:
            logger.error(f"Error parsing JSON response: {e}")
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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=15),
        reraise=True,
    )
    async def _get_doc_data(self, doc_info: dict) -> Optional[dict]:
        """Get document data by fetching PDF content and converting to markdown"""
        doc_id = doc_info.pop("doc_id", None)
        if not doc_id:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link="",
                error_message="Missing doc_id",
            )
            return None

        content_url = f"{self.doc_content_url}?atosIds={doc_id}"
        # Get document content info
        response = await self.request_service.make_request(content_url)
        if not response or response.status != 200:
            raise Exception(f"Failed to get document content for doc_id={doc_id}")

        try:
            data = await response.json()
        except Exception as e:
            logger.error(f"Error parsing content JSON response: {e}")
            raise

        # Get PDF path from files section
        if "files" in data and data["files"]:
            file_data = data["files"][0]
            caminho_pdf = file_data.get("caminhoPDF")

            if caminho_pdf:
                # Construct PDF URL
                pdf_url = f"{self.base_url}/uploads/atos/{doc_id}/{caminho_pdf}"

                # Download and process PDF
                pdf_response = await self.request_service.make_request(pdf_url)
                if pdf_response:
                    # Convert PDF to markdown
                    text_markdown = await self._get_markdown(response=pdf_response)

                    if not text_markdown or not text_markdown.strip():
                        # Try image extraction if regular PDF extraction fails
                        text_markdown = await self._get_markdown(
                            stream=BytesIO(await pdf_response.read())
                        )

                    if text_markdown and text_markdown.strip():
                        doc_info.update(
                            {
                                "text_markdown": text_markdown,
                                "document_url": pdf_url,
                            }
                        )
                        return doc_info

        await self._save_doc_error(
            title=doc_info.get("title", ""),
            year=doc_info.get("year", ""),
            html_link=content_url,
            error_message="No text extracted from document",
        )
        return None

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        reraise=True,
    )
    async def _get_total_count(self, norm_type_id: str, year: int) -> int:
        """Get total count of documents for a type and year"""
        payload = self._format_search_payload(norm_type_id, year, 1)

        response = await self.request_service.make_request(
            self.search_url, method="POST", json=payload
        )
        if not response or response.status != 200:
            error_msg = "Failed to get total count: No response"
            if response:
                error_msg = f"Response error: {response.status}"
            raise Exception(error_msg)

        try:
            data = await response.json()
            return data.get("count", 0)
        except Exception:
            return 0

    async def _get_all_pages_docs(self, norm_type_id: str, year: int) -> list:
        """Get documents from all pages for a given type and year"""
        # Get total count first
        total_count = await self._get_total_count(norm_type_id, year)
        if total_count == 0:
            return []

        # Calculate total pages (assuming 10 items per page based on API behavior)
        page_size = 10
        total_pages = (total_count + page_size - 1) // page_size

        all_docs = []

        # Get all pages in parallel
        tasks = [
            self._get_docs_links(self._format_search_payload(norm_type_id, year, page))
            for page in range(1, total_pages + 1)
        ]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": "N/A", "situation": "N/A"},
            desc="SERGIPE | get_docs_links",
        )
        for result in valid_results:
            if result:
                all_docs.extend(result)

        return all_docs

    async def _fetch_constitution(self):
        """Fetch the state constitution document"""
        constitution_url = f"{self.base_url}/Public/GetConstituicao"
        response = await self.request_service.make_request(constitution_url)

        data = await response.json() if response else {}

        file_path = data.get("arquivoAtualizado")
        doc_id = data.get("id")
        if file_path:
            file_url = (
                f"{self.base_url}/uploads/constituicao/{doc_id}/atualizado/{file_path}"
            )
            text_markdown = await self._get_markdown(url=file_url)

            doc_info = {
                "id": doc_id,
                "year": 1989,  # Sergipe's constitution year
                "title": "Constituição Estadual de Sergipe",
                "type": "Constituição Estadual",
                "summary": data.get("ementa", ""),
                "date": data.get("dataAto", ""),
                "situation": (
                    self.situations.get(data.get("idSituacao"), "Em Vigor")
                    if isinstance(self.situations, dict)
                    else "Em Vigor"
                ),
                "text_markdown": text_markdown,
                "document_url": file_url,
            }

            await self.saver.save([doc_info])
            self.results.append(doc_info)
            if self.verbose:
                logger.info(
                    f"Fetched constitution: {doc_info['title']} | ID: {doc_id} | URL: {file_url}"
                )
        else:
            logger.warning("No constitution file found in the response.")

    async def _scrape_type(
        self, norm_type: str, norm_type_id: int, year: int
    ) -> list[dict]:
        """Scrape norms for a specific type in a year"""
        try:
            # Get all documents for this type and year
            documents = await self._get_all_pages_docs(norm_type_id, year)

            if not documents:
                return []

            # Process documents concurrently
            tasks = [self._get_doc_data(doc_info.copy()) for doc_info in documents]
            valid_results = await self._gather_results(
                tasks,
                context={"year": year, "type": norm_type, "situation": "N/A"},
                desc=f"SERGIPE | {norm_type}",
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

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape norms for a specific year, fetching constitution on first call."""
        if not hasattr(self, "_constitution_fetched"):
            await self._fetch_constitution()
            self._constitution_fetched = True
        return await super()._scrape_year(year)
