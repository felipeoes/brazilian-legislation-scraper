import urllib.parse
import re
from typing import Optional
from bs4 import BeautifulSoup

from loguru import logger
from src.scraper.base.scraper import BaseScraper

TYPES = {
    "Resolução": 1,
    "Moção": 2,
    "Recomendação": 3,
    "Proposição": 4,
    "Decisão": 5,
    "Portaria": 6,
}

VALID_SITUATIONS = [
    "Não consta"
]  # Conama does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = (
    []
)  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class ConamaScraper(BaseScraper):
    """Webscraper for Conama (Conselho Nacional do Meio Ambiente) website (https://conama.mma.gov.br/atos-normativos-sistema)

    Example search request: https://conama.mma.gov.br/?option=com_sisconama&order=asc&offset=0&limit=30&task=atosnormativos.getList&tipo=6&ano=1984

    Observation: Conama does not have a situation field, invalid norms will have an indication in the document text
    """

    def __init__(
        self,
        base_url: str = "https://conama.mma.gov.br/",
        **kwargs,
    ):
        super().__init__(
            base_url,
            name="CONAMA",
            types=list(TYPES.keys()),
            situations=SITUATIONS,
            **kwargs,
        )
        self.params = {
            "option": "com_sisconama",
            "order": "asc",
            "offset": 0,
            "limit": 100,
            "task": "atosnormativos.getList",
        }
        self._situation_regex = re.compile(r"Revogad|Revogação", re.IGNORECASE)

    def _format_search_url(
        self, norm_type: str, offset: int = 0, year: str | None = None
    ) -> str:
        """Format url for search request"""
        ano = year or self.params["ano"]
        return f"{self.base_url}?option={self.params['option']}&order={self.params['order']}&offset={offset}&limit={self.params['limit']}&task={self.params['task']}&tipo={TYPES[norm_type]}&ano={ano}"

    async def _get_doc_data(self, doc_info: dict) -> Optional[dict]:
        """Get document data from norm dict. Download url for pdf will follow the pattern: https://conama.mma.gov.br/?option=com_sisconama&task=arquivo.download&id={id}"""
        doc_id = doc_info.get("aid")
        doc_number = doc_info.get("numero", "")
        doc_type = doc_info.get("nomeato", "")
        doc_year = doc_info.get("ano", "")

        if doc_id is None:
            logger.info(
                f"Skipping {doc_type} CONAMA Nº {doc_number}/{doc_year} as it has no document ID attached (aid is null)."
            )
            return None

        doc_description = doc_info.get("descricao", "")
        doc_status = doc_info.get("status")
        doc_keyword = doc_info.get("palavra_chave", "")
        doc_origin = doc_info.get("porigem", "")
        doc_url = urllib.parse.urljoin(
            self.base_url,
            f"?option=com_sisconama&task=arquivo.download&id={doc_id}",
        )

        # Fetch the document once to detect content type and avoid double requests
        resp = await self.request_service.make_request(doc_url)
        if resp is None:
            logger.warning(
                f"Failed to fetch document for {doc_type} CONAMA Nº {doc_number}/{doc_year}"
            )
            await self.saver.save_error(
                {
                    "title": f"{doc_type} CONAMA Nº {doc_number}/{doc_year}",
                    "year": doc_year,
                    "situation": "",
                    "type": doc_type,
                    "html_link": doc_url,
                },
                error_message="Failed to fetch document URL",
            )
            return None

        content_type = (resp.content_type or "").lower()

        if "html" in content_type:
            # HTML document — strip hyperlinks and convert to markdown
            body = await resp.read()
            soup = BeautifulSoup(body, "html.parser")
            for a_tag in soup.find_all("a"):
                a_tag.unwrap()
            text_markdown = await self._get_markdown(
                html_content=soup.prettify(),
            )
        else:
            # PDF or other binary — let _get_markdown detect format and use ocr_service
            text_markdown = await self._get_markdown(response=resp)

        if text_markdown is None or not text_markdown.strip():
            logger.warning(
                f"Empty markdown for {doc_type} CONAMA Nº {doc_number}/{doc_info['ano']}"
            )
            await self.saver.save_error(
                {
                    "title": f"{doc_type} CONAMA Nº {doc_number}/{doc_info['ano']}",
                    "year": doc_info["ano"],
                    "situation": "",
                    "type": doc_type,
                    "html_link": doc_url,
                },
                error_message="Empty markdown after conversion",
            )
            return None

        # Detect PHP server-error pages returned instead of the PDF
        # (e.g. "Warning: file_get_contents(...): failed to open stream: HTTP request failed!")
        if (
            "failed to open stream" in text_markdown
            or "HTTP request failed" in text_markdown
        ):
            logger.warning(
                f"Server error response for {doc_type} CONAMA Nº {doc_number}/{doc_info['ano']}: "
                f"{text_markdown}"
            )
            await self.saver.save_error(
                {
                    "title": f"{doc_type} CONAMA Nº {doc_number}/{doc_info['ano']}",
                    "year": doc_info["ano"],
                    "situation": "",
                    "type": doc_type,
                    "html_link": doc_url,
                },
                error_message=f"Server error response: {text_markdown}",
            )
            return None

        # get situation from doc_status. If "Revogad" or "Revogação" in doc_status, situation is "Revogada", otherwise "Não consta"
        situation = "Não consta revogação expressa"
        if doc_status and self._situation_regex.search(doc_status):
            situation = "Revogada"

        text_markdown = self._clean_markdown(
            text_markdown,
            replace=[
                (
                    re.escape(
                        "*Obs:** não há registro no sítio http://portal.imprensanacional.gov.br/ devido a data de publicação ser anterior ao ano de 1990."
                    ),
                    "",
                ),
                (
                    re.escape(
                        "*Obs: não há registro no sítio http://portal.imprensanacional.gov.br/ devido a data de publicação ser anterior ao ano de 1990."
                    ),
                    "",
                ),
                (
                    r"Esse texto não substitui o publicado no Boletim de Serviço (.*?), do Ministério do Interior",
                    "",
                ),
                (
                    r"Este texto não substitui o publicado no Boletim de Serviço, de (.*?)\.",
                    "",
                ),
                (
                    r"Este texto não substitui o publicado no DOU, de (.*?)\.",
                    "",
                ),
            ],
        )

        if not self._check_text_length(text_markdown, 200):
            logger.warning(
                f"Markdown text for {doc_type} CONAMA Nº {doc_number}/{doc_info['ano']} is very short. Length: {len(text_markdown)} chars."
            )
            await self.saver.save_error(
                {
                    "title": f"{doc_type} CONAMA Nº {doc_number}/{doc_info['ano']}",
                    "year": doc_info["ano"],
                    "situation": situation,
                    "type": doc_type,
                    "html_link": doc_url,
                },
                error_message="Markdown text very short after cleaning, may indicate conversion issues",
            )
            return None

        # title will be like Resolução CONAMA Nº 501/2021
        return {
            "title": f"{doc_type} CONAMA Nº {doc_number}/{doc_info['ano']}",
            "id": doc_id,
            "number": doc_number,
            "summary": doc_description,
            "situation": situation,
            "keyword": doc_keyword,
            "origin": doc_origin,
            "text_markdown": text_markdown,
            "document_url": doc_url,
        }

    async def _fetch_page_norms(
        self, norm_type: str, offset: int, year_str: str
    ) -> list[dict]:
        """Fetch norms from a single pagination page."""
        url = self._format_search_url(norm_type, offset=offset, year=year_str)
        response = await self.request_service.make_request(url)
        if response is None:
            return []
        try:
            json_response = await response.json(content_type=None)
            return json_response["data"]["rows"]
        except (KeyError, ValueError, TypeError) as e:
            logger.error(
                f"Failed to parse pagination JSON for {norm_type} {year_str}: {e}"
            )
            return []

    async def _scrape_situation_type(
        self, year: int, situation: str, norm_type: str, _norm_type_id: int
    ) -> list[dict]:
        """Scrape data for a specific year, situation, and type combination"""
        year_str = str(year)
        url = self._format_search_url(norm_type, offset=0, year=year_str)

        response = await self.request_service.make_request(url)
        if response is None:
            return []

        # CONAMA API may return JSON with text/html content-type, so parse with content_type=None
        try:
            json_response = await response.json(content_type=None)
            data = json_response["data"]
            total_norms = data["total"]
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Failed to parse JSON for {norm_type} {year_str}: {e}")
            return []

        norms = list(data["rows"])
        limit = self.params["limit"]

        # Fetch remaining pages in parallel
        if total_norms > limit:
            offsets = list(range(limit, total_norms, limit))
            page_tasks = [
                self._fetch_page_norms(norm_type, offset, year_str)
                for offset in offsets
            ]
            page_results = await self._gather_results(
                page_tasks,
                context={"year": year_str, "type": norm_type, "situation": situation},
                desc=f"CONAMA | {norm_type} | pagination",
            )
            for result in page_results:
                if isinstance(result, list):
                    norms.extend(result)

        type_results = []
        # get all norm data
        tasks = [self._get_doc_data(norm) for norm in norms]
        valid_results = await self._gather_results(
            tasks,
            context={
                "year": year_str,
                "type": norm_type,
                "situation": situation,
            },
            desc=f"CONAMA | {norm_type}",
        )
        for result in valid_results:
            queue_item = {
                "year": year_str,
                "type": norm_type,
                "situation": situation,
                **result,
            }
            type_results.append(queue_item)

        if self.verbose:
            logger.info(
                f"Year: {year_str} | Type: {norm_type} | Situation: {situation} | Total: {len(type_results)}"
            )

        return type_results

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape norms for a specific year"""
        tasks = [
            self._scrape_situation_type(year, situation, norm_type, TYPES[norm_type])
            for situation in self.situations
            for norm_type in self.types
        ]
        valid = await self._gather_results(
            tasks,
            context={"year": year, "type": "NA", "situation": "NA"},
            desc=f"{self.name} | Year {year}",
        )
        return [
            item
            for result in valid
            for item in (result if isinstance(result, list) else [result])
        ]
