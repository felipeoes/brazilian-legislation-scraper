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

    Year start (earliest on source): 1991

    Example search request: https://sapl3.al.pb.leg.br/api/norma/normajuridica/?page=1&ano=2025
    """

    def __init__(
        self,
        base_url: str = "https://sapl3.al.pb.leg.br",
        max_workers: int = 2,
        rps: float = 1,
        **kwargs: Any,
    ):
        super().__init__(
            base_url,
            name="PARAIBA",
            types=TYPES,
            max_workers=max_workers,
            rps=rps,
            **kwargs,
        )
