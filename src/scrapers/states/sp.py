from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlencode

from src.core.convert import calc_pages
from src.core.document import Document
from src.core.scraper import BaseScraper

API_URL = "https://baleg-api-prd.al.sp.gov.br"
SITE_URL = "https://www.al.sp.gov.br"
PAGE_SIZE = 500
_HOME_LINKS = {"http://www.al.sp.gov.br", "https://www.al.sp.gov.br"}

# Ids from the API's /situacoesNormas endpoint.
VALID_SITUATIONS = {"Sem revogação expressa": 1}
INVALID_SITUATIONS = {
    "Declarada inconstitucional": 2,
    "Eficácia suspensa": 3,
    "Eficácia exaurida": 4,
    "Revogada": 5,
    "Anulada": 6,
}
SITUATIONS = {**VALID_SITUATIONS, **INVALID_SITUATIONS}
# idTipoNorma values used by the site's search form; the API returns the name in "tipo".
TYPES = {
    "Constituição Estadual": 59,
    "Emenda Constitucional": 55,
    "Decreto": 3,
    "Decreto-Lei": 25,
    "Decreto-Lei Complementar": 1,
    "Decreto Legislativo": 28,
    "Lei": 9,
    "Lei Complementar": 2,
    "Resolução": 14,
    "Resolução da Alesp": 19,
    "Ato da Mesa": 21,
    "Ato do Presidente": 22,
    "Decisão da Mesa": 12,
    "Decisão do Presidente": 23,
}


class SaoPauloAlespScraper(BaseScraper):
    """São Paulo legislation from ALESP (Assembleia Legislativa do Estado de São Paulo), https://www.al.sp.gov.br/legislacao

    Year start (earliest on source): 1835

    Example listing request: https://baleg-api-prd.al.sp.gov.br/normas?ano=2012&idsSituacao=1&page=0&size=500&sort=desc
    """

    name = "SAO_PAULO"
    base_url = SITE_URL
    types = TYPES
    situations = SITUATIONS

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        # Listing rows lack the situation; filtering by it avoids a detail request per norm.
        for situation, situation_id in SITUATIONS.items():
            try:
                async for ref in self._list_situation(year, situation, situation_id):
                    yield ref
            except Exception as exc:
                await self.error({"year": year, "situation": situation}, exc)

    async def _search(self, year: int, situation_id: int, page: int) -> dict[str, Any]:
        query = urlencode(
            {
                "ano": year,
                "idsSituacao": situation_id,
                "page": page,
                "size": PAGE_SIZE,
                "sort": "desc",
            }
        )
        data = (await self.http.request(f"{API_URL}/normas?{query}")).json()
        if not (
            isinstance(data, dict)
            and isinstance(data.get("content"), list)
            and isinstance(data.get("totalElements"), int)
        ):
            raise ValueError(
                f"Malformed ALESP listing: year {year}, situation {situation_id}, page {page}"
            )
        return data

    async def _list_situation(
        self, year: int, situation: str, situation_id: int
    ) -> AsyncIterator[dict]:
        first = await self._search(year, situation_id, 0)

        async def fetch_page(page: int) -> list[dict]:
            data = first if page == 0 else await self._search(year, situation_id, page)
            return data["content"]

        pages = calc_pages(first["totalElements"], PAGE_SIZE)
        async for row in self.paginate(fetch_page, pages - 1, start=0):
            yield {
                "year": year,
                "type": row["tipo"],
                "title": row["nomeNorma"],
                "summary": (row.get("dsEmenta") or "").strip(),
                "situation": situation,
                "number": row.get("nuNorma"),
                "date": row.get("data"),
                "publication_date": row.get("dataPublicacao"),
                "publication_url": row.get("urlPublicacaoDO"),
                "document_url": f"{SITE_URL}/norma/{row['idNorma']}",
                # Same preference as the site's result list.
                "text_url": row.get("urlTextoAtualizado")
                or row.get("urlIntegraListaPesquisa"),
            }

    async def fetch(self, ref: dict) -> Document:
        url = ref["text_url"]
        if not url:
            raise ValueError("ALESP publishes no text for this norm")
        if url.lower().endswith(".pdf"):
            markdown, raw, ext = await self.download(url)
        else:
            response = await self.http.request(url)
            # soup(), not text: pages declare their latin-1 charset only in a meta tag.
            soup = response.soup()
            for link in soup.find_all("a"):
                href = link.get("href", "").rstrip("/")
                if (
                    href in _HOME_LINKS
                    or link.get_text(strip=True) == "Ficha informativa"
                ):
                    link.decompose()
            markdown = await self.markdown(html=str(soup), base_url=url)
            raw, ext = response.content, ".html"
        return Document(**ref, text_markdown=markdown, raw=raw, raw_ext=ext)
