import base64
from io import BytesIO
from pathlib import Path
from urllib.parse import quote

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
    """

    def __init__(
        self,
        base_url: str = "https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/administrativo/documento/consultar",
        **kwargs,
    ):
        super().__init__(base_url, name="ALAGOAS", types=TYPES, situations={}, **kwargs)
        self.view_doc_url = "https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/documentos/visualizarDocumento?"

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
        except Exception:
            return []

    async def _get_doc_data(self, doc_info: dict) -> dict | None:
        """Fetch and convert a single Alagoas norm.

        Download URL pattern:
            https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/documentos/visualizarDocumento?acess={acess}&key={key}

        The document key must be double-URL-encoded, otherwise the API
        returns 404.  The response contains a base64-encoded file payload.
        """
        key = quote(quote(doc_info["link"]["key"]))
        doc_link = f"{self.view_doc_url}acess={doc_info['link']['acess']}&key={key}"

        if self._is_already_scraped(doc_link):
            return None

        year = doc_info.get("_year", "")
        title = ""
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
            title = Path(full_filename).stem

            pdf_bytes = base64.b64decode(data["arquivo"]["base64"])
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

        valid, reason = self._valid_markdown(text_markdown)
        if not valid:
            await self._save_doc_error(
                title=title,
                year=year,
                html_link=doc_link,
                error_message=f"Invalid markdown: {reason}",
            )
            return None

        return {
            "id": doc_info["numeroDocumento"],
            "number": doc_info["numeroDocumento"],
            "title": title,
            "type": (doc_info.get("tipoDocumento") or {}).get("descricao", ""),
            "summary": doc_info.get("textoEmenta", ""),
            "category": (doc_info.get("categoria") or {}).get("descricao", ""),
            "publication_date": doc_info.get("dataPublicacao", ""),
            "text_markdown": text_markdown,
            "document_url": doc_link,
            "_raw_content": pdf_bytes,
            "_content_extension": ext,
        }

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
        except Exception:
            return []

        total_norms = data.get("registrosTotais")
        if not total_norms:
            return []

        per_page = data.get("registrosPorPagina") or 10
        pages = self._calc_pages(total_norms, per_page)

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
            norm_type="NA",
            situation="NA",
        )
