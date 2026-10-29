import base64
import re
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from urllib.parse import quote

from loguru import logger

from src.core.convert import (
    bytes_to_markdown,
    calc_pages,
    infer_type_by_prefix,
    valid_markdown,
)
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

TYPES = {
    "Constituição Estadual": "TIP080",
    "Decreto": "TIP002",
    "Decreto Autônomo": "TIP045",
    "Emenda Constitucional": "TIP081",
    "Ementário": "TIP108",
    "Lei Complementar": "TIP042",
    "Lei Delegada": "TIP044",
    "Lei Ordinária": "TIP043",
}
_EMENTA_STRUCTURAL_RE = re.compile(
    "((?:LEI|DECRETO|EMENDA|RESOLU[ÇC][ÃA]O|CONSTITUIÇÃO|EMENTÁRIO|PORTARIA|ATO)[^\\n]*)\\n+[\\s\\S]+?(?=\\nO GOVERNADOR\\b)",
    re.IGNORECASE | re.DOTALL,
)
_DISCLAIMER_LINE_RE = re.compile(
    "\\n?Est[ea]\\s+texto\\s+n[aã]o\\s+substitui[^\\n]*", re.IGNORECASE
)


class AlagoasSefazScraper(BaseScraper):
    "Webscraper for Alagoas Sefaz website."

    name = "ALAGOAS"
    types = TYPES
    situations = {}
    base_url = "https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/administrativo/documento/consultar"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.view_doc_url = "https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/documentos/visualizarDocumento?"

    @staticmethod
    def _infer_type_from_title(title: str) -> str:
        """Infer a canonical type from a title when the API omits it."""
        aliases = {
            "Lei Complementar": "Lei Complementar",
            "Lei Delegada": "Lei Delegada",
            "Lei ": "Lei Ordinária",
            "Decreto Autônomo": "Decreto Autônomo",
            "Decreto ": "Decreto",
            "Emenda Constitucional": "Emenda Constitucional",
            "Constituição Estadual": "Constituição Estadual",
            "Ementário": "Ementário",
        }
        return infer_type_by_prefix(title, aliases)

    def _build_params(self, year: int) -> dict:
        """Build a fresh POST body for an unfiltered year query."""
        return {
            "periodoInicial": f"{year}-01-01T00:00:00.000-0300",
            "periodoFinal": f"{year}-12-31T00:00:00.000-0300",
            "numero": None,
            "codigoCategoria": "CAT017",
            "codigoSetor": None,
        }

    def _build_url(self, page: int = 1) -> str:
        """Build the request URL, appending the pagination query param when needed."""
        if page > 1:
            return self.base_url + f"?pagina={page}"
        return self.base_url

    async def _fetch_page_norms(self, page: int, params: dict) -> list[dict]:
        """Fetch a single page of norm listings and return the document rows.

        Returns an empty list on failure so callers can safely extend their
        collection without additional error handling.
        """
        try:
            response = await self.http.request(
                self._build_url(page), method="POST", json=params
            )
            data = response.json()
            return data.get("documentos") or []
        except Exception as e:
            logger.warning(f"Failed to fetch Alagoas norm page: {e}")
            raise

    async def fetch(self, doc_info: dict) -> Document | None:
        "Fetch and convert a single Alagoas norm."
        key = quote(quote(doc_info["link"]["key"]))
        doc_link = f"{self.view_doc_url}acess={doc_info['link']['acess']}&key={key}"
        norm_type = (
            (doc_info.get("tipoDocumento") or {}).get("descricao") or ""
        ).strip()
        number = doc_info["numeroDocumento"]
        year = int(doc_info.get("_year") or 0)
        title = f"{norm_type} {number} de {year}".strip()
        if not norm_type:
            norm_type = self._infer_type_from_title(title)
            if norm_type:
                title = f"{norm_type} {number} de {year}".strip()
        ext = ".pdf"
        pdf_bytes = b""
        try:
            response = await self.http.request(doc_link)
            data = response.json()
            full_filename = data["arquivo"]["nomeArquivo"]
            ext = Path(full_filename).suffix.lower() or ".pdf"
            if not norm_type:
                norm_type = self._infer_type_from_title(full_filename.replace("_", " "))
                if norm_type:
                    title = f"{norm_type} {number} de {year}".strip()
            pdf_bytes = base64.b64decode(data["arquivo"]["base64"])
            if year < 2000:
                if self.ocr:
                    text_markdown = await self.ocr.pdf(pdf_bytes)
                else:
                    logger.warning(
                        f"ALAGOAS | {year} | No OCR service configured for pre-2000 scanned PDF — trying digital text extraction."
                    )
                    text_markdown = await bytes_to_markdown(
                        pdf_bytes, "", self.base_url, self.http, self.ocr
                    )
            else:
                text_markdown = await bytes_to_markdown(
                    pdf_bytes, "", self.base_url, self.http, self.ocr
                )
        except Exception as e:
            raise ValueError(f"Error processing document: {e}")
        if not text_markdown:
            raise ValueError("Empty markdown extracted from document")
        valid, reason = valid_markdown(text_markdown)
        if not valid:
            raise ValueError(f"Invalid markdown: {reason}")
        text_markdown = _DISCLAIMER_LINE_RE.sub("", text_markdown).strip()
        from src.core.document import Document

        return Document(
            year=year,
            id=doc_info["numeroDocumento"],
            number=doc_info["numeroDocumento"],
            title=title,
            type=norm_type or "Desconhecido",
            summary=doc_info.get("textoEmenta", ""),
            category=(doc_info.get("categoria") or {}).get("descricao", ""),
            publication_date=doc_info.get("dataPublicacao", ""),
            text_markdown=text_markdown,
            document_url=doc_link,
            raw=pdf_bytes,
            raw_ext=ext,
            situation="Não consta",
        )

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        params = self._build_params(year)
        data = (
            await self.http.request(self._build_url(), method="POST", json=params)
        ).json()
        pages = calc_pages(
            data["registrosTotais"], data.get("registrosPorPagina") or 10
        )

        async def fetch_page(page: int) -> list[dict]:
            return (
                data.get("documentos", [])
                if page == 1
                else await self._fetch_page_norms(page, params)
            )

        async for ref in self.paginate(fetch_page, pages):
            norm_type = (ref.get("tipoDocumento") or {}).get(
                "descricao"
            ) or "Desconhecido"
            url = f"{self.view_doc_url}acess={ref['link']['acess']}&key={quote(quote(ref['link']['key']))}"
            yield {
                **ref,
                "year": year,
                "_year": year,
                "type": norm_type,
                "title": f"{norm_type} {ref['numeroDocumento']} de {year}",
                "document_url": url,
                "situation": DEFAULT_VALID_SITUATION,
            }
