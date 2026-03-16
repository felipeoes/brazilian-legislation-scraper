from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument
import base64
import re
from io import BytesIO
from pathlib import Path
from urllib.parse import quote

from loguru import logger

from src.scraper.base.converter import calc_pages, valid_markdown
from src.scraper.base.scraper import StateScraper

# Documentation-only reference — the API does not require filtering by type.
# An unfiltered request per year returns all norm types at once; each row
# carries its type in ``tipoDocumento.descricao``.
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

# Structural ementa removal: matches the norm title line followed by the ementa
# block (any content, including blank lines) up to the body opener.
_EMENTA_STRUCTURAL_RE = re.compile(
    r"((?:LEI|DECRETO|EMENDA|RESOLU[ÇC][ÃA]O|CONSTITUIÇÃO|EMENTÁRIO|PORTARIA|ATO)[^\n]*)"
    r"\n+[\s\S]+?(?=\nO GOVERNADOR\b)",
    re.IGNORECASE | re.DOTALL,
)

# Footer disclaimer common to Alagoas PDFs: "Este texto não substitui..."
_DISCLAIMER_LINE_RE = re.compile(
    r"\n?Est[ea]\s+texto\s+n[aã]o\s+substitui[^\n]*",
    re.IGNORECASE,
)


class AlagoasSefazScraper(StateScraper):
    """Webscraper for Alagoas Sefaz website.

    Portal: https://gcs2.sefaz.al.gov.br/#/administrativo/documentos/consultar-gabinete

    Example search request (POST):
        https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/administrativo/documento/consultar

    Payload::

        {
            "periodoInicial": "2024-01-01T00:00:00.000-0300",
            "periodoFinal":   "2024-12-31T00:00:00.000-0300",
            "numero":         null,
            "codigoCategoria":"CAT017",
            "codigoSetor":    null
        }

    The API does not require an ``especieLegislativa`` (type) filter — a
    single unfiltered POST per year returns all norm types at once.  Each
    document row carries its type in ``tipoDocumento.descricao``.

    Year start (earliest on source): 1942

    ``year_start`` is set to 1942 in the scraper config.  Earlier years
    exist on the website but their document texts contain incorrect dates,
    so 1942 is the safe starting point for reliable content.

    The site has no *situation* (revogação) concept, so ``situations`` is
    empty and no situation iteration is performed.

    **Rate limit:** The API enforces a server-side limit of 5 req/s.  Exceeding
    that ceiling triggers throttling which, combined with retry back-off, makes
    the overall scrape significantly slower.  ``rps`` is capped at 5 in
    ``__init__`` regardless of the caller-supplied value.

    **Markdown extraction strategy (year boundary):**

    * ``year < 2000`` — documents are scanned PDF images.  Markitdown can
      open them but yields many OCR errors.  When ``llm_config`` is supplied,
      the LLM OCR service is invoked directly (``ocr_service.pdf_to_markdown``)
      for these years, bypassing markitdown entirely.  Without ``llm_config``
      the scraper falls back to ``_get_markdown`` and logs a warning.
    * ``year >= 2000`` — documents carry a proper text layer; ``_get_markdown``
      handles them correctly without LLM assistance.

    **Summary vs. text_markdown:** The ``summary`` field (``textoEmenta``) is
    intentionally **not** stripped from ``text_markdown``.  Alagoas documents
    fall into two structural categories:

    * *Laws and long decrees* — the ementa appears as a standalone ALL-CAPS
      block between the title line and the body opener.  A reliable structural
      regex would need to handle PDF column-layout artifacts that scramble word
      order, making any text-based match fragile.
    * *Short administrative decrees* (exonerations, appointments, etc.) — the
      ementa is derived directly from the body sentence (e.g. "RESOLVE exonerar
      …").  Removing it would corrupt the body text, leaving orphaned
      punctuation and broken sentences.

    Given these two mutually exclusive cases there is no single safe strategy
    to strip the ementa from ``text_markdown`` for all document types.  The
    ``summary`` field is kept for search and indexing purposes; consumers
    should treat it as a standalone excerpt rather than expecting it to be
    absent from ``text_markdown``.
    """

    def __init__(
        self,
        base_url: str = "https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/administrativo/documento/consultar",
        **kwargs,
    ):
        kwargs["rps"] = min(kwargs.get("rps", 5), 5)
        super().__init__(base_url, name="ALAGOAS", types=TYPES, situations={}, **kwargs)
        self.view_doc_url = "https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/documentos/visualizarDocumento?"

    @staticmethod
    def _infer_type_from_title(title: str) -> str:
        """Infer a canonical type from a title when the API omits it."""
        normalized_title = " ".join(title.split())
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
        for prefix, canonical in sorted(
            aliases.items(), key=lambda item: len(item[0]), reverse=True
        ):
            if normalized_title.casefold().startswith(prefix.casefold()):
                return canonical
        return ""

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
            response = await self.request_service.make_request(
                self._build_url(page), method="POST", json=params
            )
            if not response:
                return []
            data = await response.json()
            return data.get("documentos") or []
        except Exception as e:
            logger.warning(f"Failed to fetch Alagoas norm page: {e}")
            return []

    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        """Fetch and convert a single Alagoas norm.

        Download URL pattern:
            https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/documentos/visualizarDocumento?acess={acess}&key={key}

        The document key must be double-URL-encoded, otherwise the API
        returns 404.  The response contains a base64-encoded file payload.

        **Markdown extraction strategy:**

        * ``year < 2000`` — documents are scanned PDF images.  Markitdown can
          open them but produces many OCR errors.  When an ``ocr_service`` is
          configured (i.e. ``llm_config`` was supplied to the scraper), LLM OCR
          is used directly via ``ocr_service.pdf_to_markdown()``, bypassing
          markitdown entirely.  If no ``ocr_service`` is available the scraper
          falls back to ``_get_markdown`` with a warning.
        * ``year >= 2000`` — documents have a proper text layer and
          ``_get_markdown`` works correctly without LLM assistance.
        """
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

        if self._is_already_scraped(doc_link, title):
            return None

        ext = ".pdf"
        pdf_bytes = b""

        try:
            response = await self.request_service.make_request(doc_link)
            if not response:
                await self._save_doc_error(
                    title=title,
                    year=year,
                    html_link=doc_link,
                    error_message="Failed request fetching document",
                )
                return None

            data = await response.json()
            full_filename = data["arquivo"]["nomeArquivo"]
            ext = Path(full_filename).suffix.lower() or ".pdf"

            if not norm_type:
                norm_type = self._infer_type_from_title(full_filename.replace("_", " "))
                if norm_type:
                    title = f"{norm_type} {number} de {year}".strip()

            pdf_bytes = base64.b64decode(data["arquivo"]["base64"])

            if year < 2000:
                if self.ocr_service:
                    text_markdown = await self.ocr_service.pdf_to_markdown(pdf_bytes)
                else:
                    logger.warning(
                        f"ALAGOAS | {year} | No OCR service configured for pre-2000 "
                        "scanned PDF — falling back to _get_markdown (expect errors)."
                    )
                    text_markdown = await self._get_markdown(
                        stream=BytesIO(pdf_bytes), filename=full_filename
                    )
            else:
                text_markdown = await self._get_markdown(
                    stream=BytesIO(pdf_bytes), filename=full_filename
                )

        except Exception as e:
            await self._save_doc_error(
                title=title,
                year=year,
                html_link=doc_link,
                error_message=f"Error processing document: {e}",
            )
            return None

        if not text_markdown:
            await self._save_doc_error(
                title=title,
                year=year,
                html_link=doc_link,
                error_message="Empty markdown extracted from document",
            )
            return None

        valid, reason = valid_markdown(text_markdown)
        if not valid:
            await self._save_doc_error(
                title=title,
                year=year,
                html_link=doc_link,
                error_message=f"Invalid markdown: {reason}",
            )
            return None

        # Remove "Este texto não substitui..." footer disclaimer
        text_markdown = _DISCLAIMER_LINE_RE.sub("", text_markdown).strip()

        from src.scraper.base.schemas import ScrapedDocument

        return ScrapedDocument(
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
            raw_content=pdf_bytes,
            content_extension=ext,
            situation="Não consta",
        )

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape all norms for a given year in a single unfiltered request batch."""
        params = self._build_params(year)

        response = await self.request_service.make_request(
            self._build_url(), method="POST", json=params
        )
        if not response:
            return []

        try:
            data = await response.json()
        except Exception as e:
            logger.warning(f"Failed to parse Alagoas year response JSON: {e}")
            return []

        total_norms = data.get("registrosTotais")
        if not total_norms:
            return []

        per_page = data.get("registrosPorPagina") or 10
        pages = calc_pages(total_norms, per_page)

        norms: list[dict] = list(data.get("documentos") or [])

        # Fetch pages 2..N concurrently
        if pages > 1:
            extra_pages = await self._gather_results(
                [self._fetch_page_norms(page, params) for page in range(2, pages + 1)],
                context={"year": year, "type": "NA", "situation": "NA"},
                desc=f"ALAGOAS | {year} | get_docs_links",
            )
            for page_rows in extra_pages:
                if isinstance(page_rows, list):
                    norms.extend(page_rows)

        # Inject year so _get_doc_data can use it in error logs
        for doc in norms:
            doc["_year"] = year

        return await self._process_documents(
            norms,
            year=year,
            norm_type="ALL",
            situation="",
        )
