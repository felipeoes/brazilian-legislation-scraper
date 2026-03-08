from typing import Any

from src.scraper.base.sapl_scraper import SAPLBaseScraper


# gotten from https://sapl.al.pi.leg.br/api/norma/tiponormajuridica/
TYPES = {
    "Constituição Estadual": 10,
    "Decreto": 7,
    "Decreto Legislativo": 3,
    "Emenda Constitucional": 5,
    "Lei": 1,
    "Lei Complementar": 2,
    "Lei Delegada": 6,
    "Resolução": 4,
}


class PiauiAlepiScraper(SAPLBaseScraper):
    """Webscraper for Piauí state legislation website (https://sapl.al.pi.leg.br/)

    Year start (earliest on source): 1922

    Example search request: https://sapl.al.pi.leg.br/api/norma/normajuridica/?page=1&ano=2025&page_size=100
    """

    def __init__(
        self,
        base_url: str = "https://sapl.al.pi.leg.br",
        **kwargs: Any,
    ):
        super().__init__(base_url, name="PIAUI", types=TYPES, **kwargs)
