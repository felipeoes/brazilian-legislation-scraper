import re

from bs4 import BeautifulSoup
from loguru import logger

from src.scraper.base.scraper import StateScraper

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

# Federal types to skip (handled by federal legislation scraper)
_FEDERAL_TYPES = {"emendas constitucionais federais"}


class ParaAlepaScraper(StateScraper):
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

    def __init__(
        self,
        base_url: str = "http://bancodeleis.alepa.pa.gov.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations={}, name="PARA", **kwargs)
        self.regex_total_count = re.compile(r"Total de Registros:\s+(\d+)")

    def _normalize_type(self, raw_type: str) -> str | None:
        """Normalize an HTML type label to the canonical TYPES name.

        Returns ``None`` for federal types (to be filtered out).
        Unknown types fall back to the raw string (future-proof).
        """
        cleaned = re.sub(r"\s+", " ", raw_type or "").strip()
        if not cleaned:
            return ""
        if cleaned.casefold() in _FEDERAL_TYPES:
            return None
        return super()._normalize_type(raw_type, aliases=_TYPE_NORMALIZE)

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
        response = await self.request_service.make_request(
            url, method="POST", payload=params
        )
        if not response:
            logger.error(f"Error fetching page: {url}")
            return []

        soup = BeautifulSoup(await response.read(), "html.parser")

        #   Total de Registros:                      0
        # check if empty page
        total_count = self.regex_total_count.search(soup.prettify())
        if total_count is None or int(total_count.group(1)) == 0:
            return []

        docs = []

        # items will be in the last table of the page
        table = soup.find_all("table")[-1]
        items = table.find_all("tr")

        for item in items:
            tds = item.find_all("td")
            if len(tds) == 2:
                # Extract type from "Tipo da Lei:" strong tag
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
                    # Federal type — skip
                    continue

                # Fallback: use norm_number from the first strong's sibling
                # (old format where the first strong has no label text)
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

    async def _get_doc_data(self, doc_info: dict) -> dict | None:
        """Get document data from given document dict."""
        return await self._process_pdf_doc(doc_info)

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape all types for a year in a single POST with tipo=''.

        Results are grouped by type and processed via ``_process_documents``
        for proper context/logging per type.
        """
        params = self._build_params(year)
        url = f"{self.base_url}/index.php"
        all_docs = await self._get_docs_links(url, params)
        if not all_docs:
            return []

        # Group by type for proper context in _process_documents
        docs_by_type: dict[str, list[dict]] = {}
        for doc in all_docs:
            docs_by_type.setdefault(doc.get("type", "Desconhecido"), []).append(doc)

        tasks = [
            self._process_documents(
                docs, year=year, norm_type=nt, situation="Não consta"
            )
            for nt, docs in docs_by_type.items()
        ]
        results = await self._gather_results(
            tasks,
            context={"year": year, "type": "NA", "situation": "Não consta"},
            desc=f"PARA | Year {year}",
        )
        return self._flatten_results(results)
