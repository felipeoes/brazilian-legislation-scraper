from typing import Any

from src.scraper.base.sapl_scraper import SAPLBaseScraper


# gotten from https://sapl.al.rr.leg.br/api/norma/tiponormajuridica/
TYPES = {
    "Ato da Mesa Diretora": 9,
    "Ação Direta de Inconstitucionalidade": 11,
    "Constituição Estadual": 10,
    "Código de Ética Parlamentar - Resolução 29/1995": 12,
    "Decreto Legislativo": 1,
    "Emenda à Constituição": 6,
    "Lei Complementar": 3,
    "Lei Delegada": 7,
    "Lei Ordinária": 2,
    "Questões de Ordem": 13,
}


class RoraimaAlpbScraper(SAPLBaseScraper):
    """Webscraper for Roraima state legislation website (https://sapl.al.rr.leg.br/)

    Example search request: https://sapl.al.rr.leg.br/api/norma/normajuridica/?tipo=2&page=3&ano=2025
    """

    def __init__(
        self,
        base_url: str = "https://sapl.al.rr.leg.br",
        **kwargs: Any,
    ):
        super().__init__(base_url, name="RORAIMA", types=TYPES, **kwargs)
