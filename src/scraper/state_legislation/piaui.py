from typing import Any

from src.scraper.base.sapl_scraper import SAPLBaseScraper


# gotten from https://sapl.al.pi.leg.br/api/norma/tiponormajuridica/
TYPES = {
    "Decreto": 7,
    "Decreto Legislativo": 3,
    "Emenda Constitucional": 5,
    "Lei": 1,
    "Lei Complementar": 2,
    "Lei Delegada": 6,
    "Resolução": 4,
}


class PiauiAlpbScraper(SAPLBaseScraper):
    """Webscraper for Piauí state legislation website (https://sapl.al.pi.leg.br/)

    Example search request: https://sapl.al.pi.leg.br/api/norma/normajuridica/?tipo=2&page=3&ano=2025
    """

    def __init__(
        self,
        base_url: str = "https://sapl.al.pi.leg.br",
        **kwargs: Any,
    ):
        super().__init__(base_url, name="PIAUI", types=TYPES, **kwargs)

    async def _process_pdf(self, pdf_link: str, _year: int = 0) -> dict | None:
        """Threshold-based PDF processing: try markitdown, fallback to OCR if too short."""
        text_markdown, raw_content, content_ext = await self._download_and_convert(
            pdf_link
        )
        if not text_markdown or len(text_markdown.strip()) < 149:
            return None
        return {
            "text_markdown": text_markdown.strip(),
            "document_url": pdf_link,
            "_raw_content": raw_content,
            "_content_extension": content_ext,
        }
