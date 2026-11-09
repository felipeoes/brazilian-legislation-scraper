import json
import re
from collections.abc import AsyncIterator
from urllib.parse import urlencode

from src.core.convert import clean_markdown
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

API_URL = "https://api.legislacao.se.gov.br/api/v1/public"
PAGE_SIZE = 50  # the API silently caps Quantity at 50
TYPES = {
    "Lei Ordinária": 3,
    "Lei Complementar": 2,
    "Decreto": 4,  # "Decreto Numerado" on the portal; label kept for archive continuity
    "Emenda Constitucional": 8,
}
VALID_SITUATIONS = {1: "Em Vigor"}
INVALID_SITUATIONS = {2: "Revogado", 3: "Declarado Inconstitucional"}
SITUATIONS = VALID_SITUATIONS | INVALID_SITUATIONS


class SergipeLegsonScraper(BaseScraper):
    """Sergipe state legislation from the PGE LegisOn API (https://legislacao.se.gov.br).

    Year start (earliest on source): 1940

    Example listing request: https://api.legislacao.se.gov.br/api/v1/public/acts?Year=2012&TypeAct=3&Page=1&Quantity=50
    """

    types = TYPES
    situations = SITUATIONS
    name = "SERGIPE"
    base_url = API_URL

    def _clean_legison_markdown(self, text_markdown: str) -> str:
        """Remove LegisOn portal boilerplate from extracted text."""
        cleaned = clean_markdown(
            text_markdown,
            replace=[
                (
                    "\\n?Extra[ií]do do Portal de Legisla[cç][aã]o do Governo de Sergipe - LegisOn(?:\\s*https?://legislacao\\.se\\.gov\\.br?/?)?\\s*",
                    "\n",
                ),
                ("\\n?https?://legislacao\\.se\\.gov\\.br/?\\s*", "\n"),
                (
                    "\\n?Este texto n[aã]o substitui o publicado no Di[aá]rio Oficial do Estado\\.?\\s*",
                    "\n",
                ),
                ("\\n?P[aá]gina\\s+\\d+(?:\\s+de\\s+\\d+)?\\s*", "\n"),
                (
                    "\\n\\n\\s*\\d{1,3}\\s*\\n\\n",
                    "\n\n",
                ),  # bare page numbers between blank lines
            ],
        )
        return re.sub("\\n{3,}", "\n\n", cleaned).strip()

    async def _acts_page(
        self, year: int, type_id: int, page: int
    ) -> tuple[list[dict], int]:
        """Return one listing page and the total page count from ``x-pagination``."""
        query = urlencode(
            {"Year": year, "TypeAct": type_id, "Page": page, "Quantity": PAGE_SIZE}
        )
        response = await self.http.request(f"{API_URL}/acts?{query}")
        payload = response.json()
        pagination = response.headers.get("x-pagination")
        if (
            not pagination
            or not isinstance(payload, dict)
            or payload.get("success") is not True
            or not isinstance(payload.get("data"), list)
        ):
            raise ValueError(f"Malformed Sergipe listing: {response.url}")
        return payload["data"], json.loads(pagination)["totalPages"]

    async def list_type(self, year: int, name: str, value: int) -> AsyncIterator[dict]:
        first, total_pages = await self._acts_page(year, value, 1)

        async def fetch_page(page: int) -> list[dict]:
            return first if page == 1 else (await self._acts_page(year, value, page))[0]

        async for act in self.paginate(fetch_page, total_pages):
            yield {
                "year": year,
                "type": name,
                "title": f"{name} {act['number']} de {year}",
                "summary": act["summary"] or "",
                "number": act["number"],
                "date": act["actDate"][:10],
                "publication_date": (act["publicationDate"] or "")[:10],
                # By id: decree numbers restarted in 2022, so number is not unique.
                "document_url": f"{API_URL}/{act['id']}/act",
            }

    async def fetch(self, ref: dict) -> Document:
        act = (await self.http.request(ref["document_url"])).json()["data"]
        # Prefer the consolidated text; originals are often scans.
        file = act["arquivoAtualizadoPdf"] or act["arquivo"]
        if not file:
            raise ValueError("Act has no PDF file")
        # The file URL is presigned and expires in an hour, so it is not stored.
        markdown, raw, ext = await self.download(file["caminho"])
        situation = act["idSituacao"]
        return Document(
            **{
                **ref,
                "situation": SITUATIONS[situation]
                if situation
                else DEFAULT_VALID_SITUATION,
                "summary": act["ementa"] or ref["summary"],
                "text_markdown": self._clean_legison_markdown(markdown),
                "raw": raw,
                "raw_ext": ext,
            }
        )
