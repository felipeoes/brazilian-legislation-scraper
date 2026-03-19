from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument
import re
from io import BytesIO
from typing import TYPE_CHECKING, Any, cast

import aiohttp
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from src.scraper.base.converter import calc_pages, clean_markdown, valid_markdown
from src.scraper.base.scraper import StateScraper

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument


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


class SergipeLegsonScraper(StateScraper):
    """Webscraper for Sergipe state legislation website (https://legison.pge.se.gov.br/)

    Year start (earliest on source): 1940

    Example search request: POST to https://legison.pge.se.gov.br/Public/Consulta
    """

    def __init__(
        self,
        base_url: str = "https://legison.pge.se.gov.br",
        **kwargs: Any,
    ):
        super().__init__(
            base_url, types=TYPES, situations=SITUATIONS, name="SERGIPE", **kwargs
        )
        self.search_url = f"{self.base_url}/Public/Consulta"
        self.doc_content_url = f"{self.base_url}/Public/GetConteudoAto"

    def _clean_legison_markdown(self, text_markdown: str) -> str:
        """Remove LegisOn portal boilerplate from extracted text."""
        cleaned = clean_markdown(
            text_markdown,
            replace=[
                (
                    r"\n?Extra[ií]do do Portal de Legisla[cç][aã]o do Governo de Sergipe - LegisOn\s*https?://legislacao\.se\.gov\.br/?\s*",
                    "\n",
                ),
                (r"\n?https?://legislacao\.se\.gov\.br/?\s*", "\n"),
                (
                    r"\n?Este texto n[aã]o substitui o publicado no Di[aá]rio Oficial do Estado\.?\s*",
                    "\n",
                ),
                (r"\n?P[aá]gina\s+\d+(?:\s+de\s+\d+)?\s*", "\n"),
            ],
        )
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

    def _extract_content_markdown(self, data: dict) -> str:
        """Extract plain-text content exposed directly by the API."""
        contents = data.get("content")
        if not isinstance(contents, list):
            return ""

        text_parts = []
        for item in contents:
            if not isinstance(item, dict):
                continue
            text = item.get("conteudo")
            if isinstance(text, str) and text.strip():
                text_parts.append(text.strip())

        if not text_parts:
            return ""

        return self._clean_legison_markdown("\n\n".join(text_parts))

    def _format_search_url(self, _norm_type_id: str, _year: int, _page: int = 1) -> str:
        """Format url for search request - returns the search URL"""
        return self.search_url

    def _format_search_payload(
        self, norm_type_id: int | str, year: int, page: int = 1
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
            client_response = cast(aiohttp.ClientResponse, response)
            data = await client_response.json()
        except Exception as e:
            logger.error(f"Error parsing JSON response: {e}")
            return []

        if not isinstance(data, dict) or "result" not in data:
            return []

        docs = []
        for item in data["result"]:
            if not isinstance(item, dict):
                continue

            item = cast(dict[str, Any], item)

            # Extract document information
            doc_id = item.get("id")
            numero = item.get("numero", "")
            data_ato = item.get("dataAto", "")
            ementa = item.get("ementa", "")
            tipo_ato = item.get("tipoAto", {})
            tipo_descricao = (
                tipo_ato.get("descricao", "") if isinstance(tipo_ato, dict) else ""
            )

            # Format title
            title = f"{tipo_descricao} {numero}"
            if data_ato:
                # Extract year from date (format: "1989-12-21T00:00:00")
                year_from_date = data_ato.split("-")[0] if "-" in data_ato else ""
                title += f" de {year_from_date}"

            # Determine situation based on situacao ID
            situacao = item.get("situacao", {})
            situation = "Não consta"  # Default
            if (
                isinstance(situacao, dict)
                and isinstance(self.situations, dict)
                and "id" in situacao
            ):
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
    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        """Get document data by fetching PDF content and converting to markdown"""
        doc_info = dict(doc_info)
        doc_id = doc_info.pop("doc_id", None)
        title = doc_info.get("title", "")
        if not doc_id:
            await self._save_doc_error(
                title=title,
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
            client_response = cast(aiohttp.ClientResponse, response)
            data = await client_response.json()
        except Exception as e:
            logger.error(f"Error parsing content JSON response: {e}")
            raise

        if not isinstance(data, dict):
            raise TypeError(f"Unexpected content payload for doc_id={doc_id}")

        if self._is_already_scraped(content_url, title):
            return None

        content_markdown = self._extract_content_markdown(data)
        if content_markdown:
            doc_info.update(
                {
                    "text_markdown": content_markdown,
                    "document_url": content_url,
                    "raw_content": content_markdown.encode("utf-8"),
                    "content_extension": ".txt",
                }
            )
            from src.scraper.base.schemas import ScrapedDocument

            return ScrapedDocument(**doc_info)

        # Get PDF path from files section
        if "files" in data and data["files"]:
            file_data = data["files"][0]
            caminho_pdf = file_data.get("caminhoPDF")

            if caminho_pdf:
                # Construct PDF URL
                pdf_url = f"{self.base_url}/uploads/atos/{doc_id}/{caminho_pdf}"

                if self._is_already_scraped(pdf_url, title):
                    return None

                # Download and process PDF
                pdf_response = await self.request_service.make_request(pdf_url)
                if pdf_response:
                    pdf_client_response = cast(aiohttp.ClientResponse, pdf_response)
                    pdf_content = await pdf_client_response.read()
                    # Convert PDF to markdown
                    text_markdown = await self._get_markdown(
                        stream=BytesIO(pdf_content)
                    )
                    text_markdown = self._clean_legison_markdown(text_markdown)

                    valid, reason = valid_markdown(text_markdown)
                    if valid:
                        doc_info.update(
                            {
                                "text_markdown": text_markdown,
                                "document_url": pdf_url,
                                "raw_content": pdf_content,
                                "content_extension": ".pdf",
                            }
                        )

                        from src.scraper.base.schemas import ScrapedDocument

                        return ScrapedDocument(**doc_info)

        await self._save_doc_error(
            title=title,
            year=doc_info.get("year", ""),
            html_link=content_url,
            error_message="No API content or extractable PDF found for document",
        )
        return None

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        reraise=True,
    )
    async def _get_total_count(self, norm_type_id: int | str, year: int) -> int:
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
            client_response = cast(aiohttp.ClientResponse, response)
            data = await client_response.json()
            if isinstance(data, dict):
                return int(data.get("count", 0) or 0)
            return 0
        except Exception as e:
            logger.warning(f"Failed to parse Sergipe total count response: {e}")
            return 0

    async def _get_all_pages_docs(self, norm_type_id: int | str, year: int) -> list:
        """Get documents from all pages for a given type and year"""
        # Get total count first
        total_count = await self._get_total_count(norm_type_id, year)
        if total_count == 0:
            return []

        # Calculate total pages (assuming 10 items per page based on API behavior)
        page_size = 10
        total_pages = calc_pages(total_count, page_size)

        all_docs = []

        # Get all pages in parallel
        tasks = [
            self._get_docs_links(self._format_search_payload(norm_type_id, year, page))
            for page in range(1, total_pages + 1)
        ]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": "NA", "situation": "NA"},
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

        if response:
            constitution_response = cast(aiohttp.ClientResponse, response)
            data = await constitution_response.json()
        else:
            data = {}

        file_path = data.get("arquivoAtualizado")
        doc_id = data.get("id")
        if file_path:
            file_url = (
                f"{self.base_url}/uploads/constituicao/{doc_id}/atualizado/{file_path}"
            )
            title = "Constituição Estadual de Sergipe"

            if self._is_already_scraped(file_url, title):
                logger.debug("State constitution already scraped, skipping")
                return

            text_markdown, raw_content, content_ext = await self._download_and_convert(
                file_url
            )
            text_markdown = self._clean_legison_markdown(text_markdown)

            valid, reason = valid_markdown(text_markdown)
            if not valid:
                logger.warning(
                    f"Constitution markdown invalid: {reason} | URL: {file_url}"
                )
                return

            doc_info = {
                "id": doc_id,
                "year": 1989,  # Sergipe's constitution year
                "title": title,
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
                "_raw_content": raw_content,
                "_content_extension": content_ext,
            }

            saved = await self._save_doc_result(doc_info)
            if saved is not None:
                doc_info = saved
            self._track_results([doc_info])
            self.count += 1
            logger.debug(
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

            for doc in documents:
                doc["year"] = year
            return await self._process_documents(
                [doc.copy() for doc in documents],
                year=year,
                norm_type=norm_type,
            )

        except Exception as e:
            logger.error(
                f"Error scraping Year: {year} | Type: {norm_type} | Error: {e}"
            )
            return []

    async def _before_scrape(self) -> None:
        await self._fetch_constitution()
