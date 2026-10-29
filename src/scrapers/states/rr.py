from src.core.sapl import SAPLBaseScraper

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


class RoraimaAlerScraper(SAPLBaseScraper):
    """Webscraper for Roraima state legislation website (https://sapl.al.rr.leg.br/)

    Year start (earliest on source): 1991

    Example search request: https://sapl.al.rr.leg.br/api/norma/normajuridica/?tipo=2&page=3&ano=2025
    """

    name = "RORAIMA"
    types = TYPES
    base_url = "https://sapl.al.rr.leg.br"
    _FOOTER_BLOCK_PATTERNS = SAPLBaseScraper._FOOTER_BLOCK_PATTERNS + (
        "sei/grr",
        "al.rr.leg.br",
        "al.rr. lea.br",
        "ale-rr na internet",
        "praça do centro cívico",
        "praca do centro civico",
    )
