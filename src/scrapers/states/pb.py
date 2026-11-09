from src.core.sapl import SAPLBaseScraper

TYPES = {
    "Ação Direta de Inconstitucionalidade Estadual": 15,
    "Constituição Estadual": 5,
    "Decreto Executivo": 18,
    "Decreto Legislativo": 6,
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

    name = "PARAIBA"
    types = TYPES
    base_url = "https://sapl3.al.pb.leg.br"
