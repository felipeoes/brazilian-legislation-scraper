import re
from collections.abc import AsyncIterator
from typing import Any

from loguru import logger

from src.core.convert import (
    bytes_to_markdown,
    calc_pages,
    clean_markdown,
    valid_markdown,
)
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

TYPES = {
    "Lei Ordinária": 3,
    "Lei Complementar": 2,
    "Decreto": 4,
    "Emenda Constitucional": 8,
}
VALID_SITUATIONS = {0: "Não consta", 1: "Em Vigor"}
INVALID_SITUATIONS = {2: "Revogado"}
SITUATIONS = VALID_SITUATIONS | INVALID_SITUATIONS


class SergipeLegsonScraper(BaseScraper):
    """Webscraper for Sergipe state legislation website (https://legison.pge.se.gov.br/)

    Year start (earliest on source): 1940

    Example search request: POST to https://legison.pge.se.gov.br/Public/Consulta
    """

    types = TYPES
    situations = SITUATIONS
    name = "SERGIPE"
    base_url = "https://legison.pge.se.gov.br"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.search_url = f"{self.base_url}/Public/Consulta"
        self.doc_content_url = f"{self.base_url}/Public/GetConteudoAto"

    def _clean_legison_markdown(self, text_markdown: str) -> str:
        """Remove LegisOn portal boilerplate from extracted text."""
        cleaned = clean_markdown(
            text_markdown,
            replace=[
                (
                    "\\n?Extra[ií]do do Portal de Legisla[cç][aã]o do Governo de Sergipe - LegisOn\\s*https?://legislacao\\.se\\.gov\\.br/?\\s*",
                    "\n",
                ),
                ("\\n?https?://legislacao\\.se\\.gov\\.br/?\\s*", "\n"),
                (
                    "\\n?Este texto n[aã]o substitui o publicado no Di[aá]rio Oficial do Estado\\.?\\s*",
                    "\n",
                ),
                ("\\n?P[aá]gina\\s+\\d+(?:\\s+de\\s+\\d+)?\\s*", "\n"),
            ],
        )
        cleaned = re.sub("\\n{3,}", "\n\n", cleaned)
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
        response = await self.http.request(self.search_url, method="POST", json=payload)
        try:
            client_response = response
            data = client_response.json()
        except Exception as e:
            logger.error(f"Error parsing JSON response: {e}")
            raise
        if not isinstance(data, dict) or "result" not in data:
            raise ValueError("Invalid Sergipe listing response")
        docs = []
        for item in data["result"]:
            if not isinstance(item, dict):
                continue
            doc_id = item.get("id")
            numero = item.get("numero", "")
            data_ato = item.get("dataAto", "")
            ementa = item.get("ementa", "")
            tipo_ato = item.get("tipoAto", {})
            tipo_descricao = (
                tipo_ato.get("descricao", "") if isinstance(tipo_ato, dict) else ""
            )
            title = f"{tipo_descricao} {numero}"
            if data_ato:
                year_from_date = data_ato.split("-")[0] if "-" in data_ato else ""
                title += f" de {year_from_date}"
            situacao = item.get("situacao", {})
            situation = "Não consta"
            if (
                isinstance(situacao, dict)
                and isinstance(self.situations, dict)
                and ("id" in situacao)
            ):
                situation = self.situations.get(situacao["id"], "Não consta")
            doc = {
                "id": str(doc_id),
                "title": title,
                "summary": ementa,
                "date": data_ato,
                "situation": situation,
                "doc_id": doc_id,
            }
            docs.append(doc)
        return docs

    async def fetch(self, doc_info: dict) -> Document | None:
        """Get document data by fetching PDF content and converting to markdown"""
        if doc_info.get("constitution"):
            data = (await self.http.request(doc_info["document_url"])).json()
            ref = {
                **doc_info,
                "pdf_link": f"{self.base_url}/uploads/constituicao/{data['id']}/atualizado/{data['arquivoAtualizado']}",
            }
            return await self.fetch_pdf(ref)
        doc_info = dict(doc_info)
        doc_id = doc_info.pop("doc_id", None)
        if not doc_id:
            raise ValueError("Missing doc_id")
        content_url = f"{self.doc_content_url}?atosIds={doc_id}"
        response = await self.http.request(content_url)
        try:
            client_response = response
            data = client_response.json()
        except Exception as e:
            logger.error(f"Error parsing content JSON response: {e}")
            raise
        if not isinstance(data, dict):
            raise TypeError(f"Unexpected content payload for doc_id={doc_id}")
        content_markdown = self._extract_content_markdown(data)
        if content_markdown:
            doc_info.update(
                {
                    "text_markdown": content_markdown,
                    "document_url": content_url,
                    "raw": content_markdown.encode("utf-8"),
                    "raw_ext": ".txt",
                }
            )
            from src.core.document import Document

            return Document(**doc_info)
        if "files" in data and data["files"]:
            file_data = data["files"][0]
            caminho_pdf = file_data.get("caminhoPDF")
            if caminho_pdf:
                pdf_url = f"{self.base_url}/uploads/atos/{doc_id}/{caminho_pdf}"
                pdf_response = await self.http.request(pdf_url)
                pdf_client_response = pdf_response
                pdf_content = pdf_client_response.content
                text_markdown = await bytes_to_markdown(
                    pdf_content, "", self.base_url, self.http, self.ocr
                )
                text_markdown = self._clean_legison_markdown(text_markdown)
                valid, reason = valid_markdown(text_markdown)
                if valid:
                    doc_info.update(
                        {
                            "text_markdown": text_markdown,
                            "document_url": pdf_url,
                            "raw": pdf_content,
                            "raw_ext": ".pdf",
                        }
                    )
                    from src.core.document import Document

                    return Document(**doc_info)
        raise ValueError("No API content or extractable PDF found for document")

    async def _get_total_count(self, norm_type_id: int | str, year: int) -> int:
        """Get total count of documents for a type and year"""
        payload = self._format_search_payload(norm_type_id, year, 1)
        response = await self.http.request(self.search_url, method="POST", json=payload)
        try:
            client_response = response
            data = client_response.json()
            if isinstance(data, dict):
                return int(data["count"])
            raise TypeError("Invalid Sergipe count response")
        except Exception as e:
            logger.warning(f"Failed to parse Sergipe total count response: {e}")
            raise

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        if year == self.years.start:
            url = f"{self.base_url}/Public/GetConstituicao"
            yield {
                "year": 1989,
                "type": "Constituição Estadual",
                "title": "Constituição Estadual de Sergipe",
                "document_url": url,
                "situation": DEFAULT_VALID_SITUATION,
                "constitution": True,
            }
        for norm_type, norm_type_id in self.types.items():
            pages = calc_pages(await self._get_total_count(norm_type_id, year), 10)

            async def fetch_page(page: int) -> list[dict]:
                return await self._get_docs_links(
                    self._format_search_payload(norm_type_id, year, page)
                )

            async for ref in self.paginate(fetch_page, pages):
                yield {
                    "year": year,
                    "type": norm_type,
                    "situation": DEFAULT_VALID_SITUATION,
                    **ref,
                    "document_url": f"{self.doc_content_url}?atosIds={ref['doc_id']}",
                }
