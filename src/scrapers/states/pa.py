import re
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.core.convert import normalize_type
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

TYPES = {
    "Decreto Estadual": 2,
    "Decreto Legislativo": 1,
    "Emenda Constitucional": 11,
    "Lei Complementar": 4,
    "Lei Ordinária": 3,
    "Resolução": 10,
}
SITUATIONS = {"Não consta": "Não consta"}
_TYPE_NORMALIZE: dict[str, str] = {name.casefold(): name for name in TYPES}
_TYPE_NORMALIZE.update(
    {
        "emendas constitucionais estaduais": "Emenda Constitucional",
        "resoluções": "Resolução",
    }
)
_FEDERAL_TYPES = {"emendas constitucionais federais"}


class ParaAlepaScraper(BaseScraper):
    """Webscraper for Para state legislation website (http://bancodeleis.alepa.pa.gov.br)

    Year start (earliest on source): 1990

    Optimization: a single POST with ``tipo=""`` fetches all document types
    at once. The type is extracted per-document from the HTML response.

    ``tipo=12`` ("Emendas Constitucionais Federais") exists on the server
    but is intentionally excluded — federal legislation is handled by the
    separate federal legislation scraper.

    Example search request: http://bancodeleis.alepa.pa.gov.br/index.php

    payload = {
        numero:
        anoLei: 2000
        tipo:
        pChave:
        verifica: 1
        button: Buscar
    }

    """

    types = TYPES
    situations = {}
    name = "PARA"
    base_url = "http://bancodeleis.alepa.pa.gov.br"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.regex_total_count = re.compile("Total de Registros:\\s+(\\d+)")

    def _normalize_type(self, raw_type: str) -> str | None:
        """Normalize an HTML type label to the canonical TYPES name.

        Returns ``None`` for federal types (to be filtered out).
        Unknown types fall back to the raw string (future-proof).
        """
        cleaned = re.sub("\\s+", " ", raw_type or "").strip()
        if not cleaned:
            return ""
        if cleaned.casefold() in _FEDERAL_TYPES:
            return None
        return normalize_type(raw_type, aliases=_TYPE_NORMALIZE, known_types=self.types)

    def _build_params(self, year: int, norm_type_id: int | str = "") -> dict:
        """Build a fresh params dict for a specific type/year query (no shared state)."""
        return {
            "numero": "",
            "anoLei": year,
            "tipo": norm_type_id,
            "pChave": "",
            "verifica": 1,
            "button": "Buscar",
        }

    async def _get_docs_links(self, url: str, params: dict) -> list:
        """Get documents html links from given page.

        Extracts type and number per-document from the HTML. Federal types
        are filtered out. Returns a list of dicts with keys ``title``,
        ``summary``, ``pdf_link``, ``type``, and ``norm_number``.
        """
        response = await self.http.request(url, method="POST", data=params)
        soup = BeautifulSoup(response.content, "lxml")
        total_count = self.regex_total_count.search(str(soup))
        if total_count is None or int(total_count.group(1)) == 0:
            return []
        docs = []
        table = soup.find_all("table")[-1]
        items = table.find_all("tr")
        for item in items:
            tds = item.find_all("td")
            if len(tds) == 2:
                raw_type = ""
                norm_number = ""
                for strong in tds[0].find_all("strong"):
                    label = strong.get_text(strip=True)
                    value = ""
                    if strong.next_sibling:
                        value = str(strong.next_sibling).strip()
                    if "Tipo da Lei:" in label:
                        raw_type = value
                    elif "Nº da Lei:" in label:
                        norm_number = value
                norm_type = self._normalize_type(raw_type)
                if norm_type is None:
                    continue
                if not norm_number:
                    first_strong = tds[0].find("strong")
                    if first_strong and first_strong.next_sibling:
                        norm_number = str(first_strong.next_sibling).strip()
                title = (
                    f"{norm_type} {norm_number}".strip() if norm_type else norm_number
                )
                pdf_link = tds[1].find("a")
                summary = pdf_link.text.strip()
                pdf_link = pdf_link["href"]
                docs.append(
                    {
                        "title": title,
                        "summary": summary,
                        "pdf_link": pdf_link,
                        "type": norm_type,
                        "norm_number": norm_number,
                    }
                )
        return docs

    async def fetch(self, doc_info: dict) -> Document | None:
        """Get document data from given document dict."""
        return await self.fetch_pdf(doc_info)

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        refs = await self._get_docs_links(
            f"{self.base_url}/index.php", self._build_params(year)
        )
        for ref in refs:
            yield {
                "year": year,
                "situation": DEFAULT_VALID_SITUATION,
                **ref,
                "document_url": urljoin(self.base_url, ref["pdf_link"]),
            }
