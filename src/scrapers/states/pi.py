from src.core.sapl import SAPLBaseScraper

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

    name = "PIAUI"
    types = TYPES
    base_url = "https://sapl.al.pi.leg.br"
