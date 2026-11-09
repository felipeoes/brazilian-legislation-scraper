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
    # Piauí PDFs often append the governor's message and protocol stamps after the law.
    _EXTRA_DOC_START_PATTERNS = (
        "governo do estado do piauí",
        "secretaria de governo do estado do piauí",
        "setor de protocolo alepi",
        "expediente ",
        "ofício pres. sgm",
        "oficio pres. sgm",
        "proposição ",
        "proposicao ",
    )
