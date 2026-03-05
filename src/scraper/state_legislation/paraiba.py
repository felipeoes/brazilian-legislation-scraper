from io import BytesIO
from typing import Any

from src.scraper.base.sapl_scraper import SAPLBaseScraper


# gotten from https://sapl3.al.pb.leg.br/api/norma/tiponormajuridica/
TYPES = {
    "Ação Direta de Inconstitucionalidade Estadual": 15,
    "Constituição Estadual": 5,
    "Decreto Executivo": 18,
    "Decreto Legislativo": 6,  # the record with id == 3 is invalid, thus using 6
    "Decreto-Lei": 17,
    "Emenda Constitucional": 9,
    "Lei Complementar": 1,
    "Lei Ordinária": 2,
    "Lei Ordinária Promulgada": 8,
    "Regimento Interno": 14,
    "Resolução": 4,
}


class ParaibaAlpbScraper(SAPLBaseScraper):
    """Webscraper for Paraíba state legislation website (https://sapl3.al.pb.leg.br/)

    Example search request: https://sapl3.al.pb.leg.br/api/norma/normajuridica/?tipo=2&page=3&ano=2025
    """

    def __init__(
        self,
        base_url: str = "https://sapl3.al.pb.leg.br",
        **kwargs: Any,
    ):
        super().__init__(base_url, name="PARAIBA", types=TYPES, **kwargs)

    async def _process_pdf(self, pdf_link: str, year: int) -> dict | None:
        """Year-aware PDF processing: old PDFs (≤1990) go straight to OCR."""
        response = await self.request_service.make_request(pdf_link)
        if not response:
            return None
        content = await response.read()
        if not content:
            return None

        if year <= 1990:
            text_markdown = await self._get_markdown(stream=BytesIO(content))
        else:
            text_markdown = await self._get_markdown(response=response)
            if not text_markdown:
                text_markdown = await self._get_markdown(stream=BytesIO(content))

        if not text_markdown or not text_markdown.strip():
            return None

        return {
            "text_markdown": text_markdown,
            "document_url": pdf_link,
            "_raw_content": content,
            "_content_extension": ".pdf",
        }
