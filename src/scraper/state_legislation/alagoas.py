import base64
import math
from urllib.parse import quote

from loguru import logger
from src.scraper.base.scraper import StateScraper

TYPES = {
    "Consituição Estadual": "TIP080",
    "Decreto": "TIP002",
    "Decreto Autônomo": "TIP045",
    "Emenda Constitucional": "TIP081",
    "Ementário": "TIP108",
    "Lei Complementar": "TIP042",
    "Lei Delegada": "TIP044",
    "Lei Ordinária": "TIP043",
}

VALID_SITUATIONS = [
    "Não consta"
]  # Conama does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class AlagoasSefazScraper(StateScraper):
    """Webscraper for Alagoas Sefaz website (https://gcs2.sefaz.al.gov.br/#/administrativo/documentos/consultar-gabinete)

    Example search request: https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/administrativo/documento/consultar?pagina=1

    Payload: {
            "palavraChave": null,
            "periodoInicial": "2024-01-01T03:00:00.000+0000",
            "periodoFinal": "2024-12-31T03:00:00.000+0000",
            "numero": null,
            "especieLegislativa": "TIP002",
            "codigoCategoria": "CAT017",
            "codigoSetor": null
        }

    Observation: Alagoas Sefaz does not have a situation field
    """

    def __init__(
        self,
        base_url: str = "https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/administrativo/documento/consultar",
        **kwargs,
    ):
        super().__init__(
            base_url, name="ALAGOAS", types=TYPES, situations=SITUATIONS, **kwargs
        )
        self.view_doc_url = "https://gcs2.sefaz.al.gov.br/sfz-gcs-api/api/documentos/visualizarDocumento?"

    def _build_params(self, norm_type_id: str, year: int) -> dict:
        """Build a fresh params dict for a specific type/year query (no shared state)."""
        return {
            "periodoInicial": f"{year}-01-01T00:00:00.000-0300",
            "periodoFinal": f"{year}-12-31T00:00:00.000-0300",
            "numero": None,
            "especieLegislativa": norm_type_id,
            "codigoCategoria": "CAT017",
            "codigoSetor": None,
        }

    def _build_url(self, page: int = 1) -> str:
        """Build the request URL, adding pagination query param when needed."""
        if page > 1:
            return self.base_url + f"?pagina={page}"
        return self.base_url

    async def _get_docs_links(
        self, url: str, params: dict, norms: list
    ) -> list[dict | None]:
        """Get document links from search request"""
        try:
            response = await self.request_service.make_request(
                url, method="POST", json=params
            )

            if response is None:
                return

            data = await response.json()
            norms.extend(data["documentos"])

        except Exception as e:
            logger.error(f"Error getting document links from url: {url} | Error: {e}")

    async def _get_doc_data(self, doc_info: dict) -> dict | None:
        """Get document data from norm dict. Download url for pdf will follow the pattern: ttps://gcs2.sefaz.al.gov.br/#/documentos/visualizar-documento?acess={acess}&key={key}"""

        key = quote(
            quote(doc_info["link"]["key"])
        )  # need to double encode otherwise it will return 404
        doc_link = f"{self.view_doc_url}acess={doc_info['link']['acess']}&key={key}"
        if self._is_already_scraped(doc_link):
            return None
        filename = ""
        ext = ".pdf"
        pdf_bytes = b""
        try:
            # get text markdown
            response = await self.request_service.make_request(doc_link)
            if response is None:
                raise RuntimeError(f"No response received for {doc_link}")
            response = await response.json()
            base64_data = response["arquivo"]["base64"]

            full_filename = response["arquivo"]["nomeArquivo"]
            from pathlib import Path

            ext = Path(full_filename).suffix.lower()
            if not ext:
                ext = ".pdf"
            filename = Path(full_filename).stem

            pdf_bytes = base64.b64decode(base64_data)
            from io import BytesIO

            text_markdown = await self._get_markdown(
                stream=BytesIO(pdf_bytes), filename=full_filename
            )

        except Exception as e:
            logger.error(f"Error getting markdown from url: {doc_link} | Error: {e}")
            text_markdown = None

        if text_markdown is None:
            await self._save_doc_error(
                title=doc_info.get("title", filename),
                year=doc_info.get("year", ""),
                html_link=doc_link,
                error_message="Failed to extract markdown from document",
            )
            return None

        return {
            "id": doc_info["numeroDocumento"],
            "title": filename,
            "summary": doc_info["textoEmenta"],
            "category": doc_info["categoria"]["descricao"],
            "publication_date": doc_info["dataPublicacao"],
            "text_markdown": text_markdown,
            "document_url": doc_link,
            "_raw_content": pdf_bytes,
            "_content_extension": ext,
        }

    async def _scrape_situation_type(
        self, situation: str, norm_type: str, norm_type_id: str, year: str
    ) -> list:
        """Scrape norms for a specific situation and type"""
        params = self._build_params(norm_type_id, year)
        url = self._build_url()

        response = await self.request_service.make_request(
            url, method="POST", json=params
        )

        if response is None:
            return []

        data = await response.json()
        total_norms = data["registrosTotais"]

        if not total_norms:
            return []

        pages = math.ceil(total_norms / 10)

        norms = []
        norms.extend(data["documentos"])

        # get all norms (page 1 already fetched above; fetch pages 2…pages)
        tasks = [
            self._get_docs_links(
                self._build_url(page),
                params,
                norms,
            )
            for page in range(2, pages + 1)
        ]
        await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": situation},
            desc=f"ALAGOAS | {norm_type} | get_docs_links",
        )

        results = []

        # get all norm data
        tasks = [self._get_doc_data(norm) for norm in norms]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": situation},
            desc=f"ALAGOAS | {norm_type}",
        )
        for result in valid_results:
            queue_item = {
                "year": year,
                "type": norm_type,
                "situation": situation,
                **result,
            }
            await self._save_doc_result(queue_item)
            results.append(queue_item)

        if self.verbose:
            logger.info(
                f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)}"
            )

        return results

    async def _scrape_year(self, year: str) -> list[dict]:
        """Scrape norms for a specific year"""
        tasks = [
            self._scrape_situation_type(sit, nt, ntid, year)
            for sit in self.situations
            for nt, ntid in self.types.items()
        ]
        valid = await self._gather_results(
            tasks,
            context={"year": year, "type": "NA", "situation": "NA"},
            desc=f"{self.name} | Year {year}",
        )
        return [
            item
            for result in valid
            for item in (result if isinstance(result, list) else [result])
        ]
