"""Paraná state legislation scraper using direct HTTP requests.

Scrapes the Casa Civil legislation system at ``legislacao.pr.gov.br``
by posting search forms and fetching document pages via HTTP — no
browser or VPN required.
"""

import re
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.core.convert import valid_markdown, wrap_html
from src.core.document import Document
from src.core.scraper import (
    DEFAULT_INVALID_SITUATION,
    DEFAULT_VALID_SITUATION,
    BaseScraper,
)

TYPES = {
    "Lei": 1,
    "Lei Complementar": 3,
    "Constituição Estadual": 10,
    "Decreto": 11,
    "Emenda Constitucional": 9,
    "Resolução": 13,
    "Portaria": 14,
}
VALID_SITUATIONS: list[str] = []
INVALID_SITUATIONS: list[str] = []
SITUATIONS = {s: s for s in VALID_SITUATIONS + INVALID_SITUATIONS}
_RE_LIST_ITEMS = re.compile("list_cor_(sim|nao)")
_RE_INVALID = re.compile("(Revogado pelo|Revogada pela|Revogado por|Revogada por)")
_RE_TOTAL_PAGES = re.compile("Página \\d+ de (\\d+)")
_RE_TOTAL_RECORDS = re.compile("Total de (\\d+) registros")
_RE_ACT_NUMBER = re.compile(r"[^\d-]*?(\d[\d.]*)\s+-")
_MAX_ACT_NUMBER = 99_999_999
_ACTS_PER_QUERY = 15  # below the 20-row page, so uneven ranges still fit


def _split(low: int, high: int, parts: int) -> list[tuple[int, int]]:
    """Split low..high into about ``parts`` contiguous ranges (at least two)."""
    size = max(1, -(-(high - low + 1) // max(2, parts)))
    return [
        (start, min(start + size - 1, high)) for start in range(low, high + 1, size)
    ]


class ParanaCVScraper(BaseScraper):
    """Scraper for Paraná state legislation (Casa Civil).

    Year start (earliest on source): 2025

    Uses plain HTTP POST requests against the J2EE search form at
    ``legislacao.pr.gov.br``.  No Playwright / VPN needed.
    """

    types = TYPES
    situations = SITUATIONS
    name = "PARANA"
    base_url = "https://www.legislacao.pr.gov.br"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._base_form_data = {
            "pesquisou": "true",
            "opcaoAno": "2",
            "opcaoNro": "1",
            "optPesquisa": "tm",
            "tiposAtoStr": "",
            "site": "1",
            "codigoTipoAto": "",
            "tipoOrdenacao": "",
            "ordAsc": "false",
            "optTexto": "2",
            "texto": "",
            "anoInicialAto": "",
            "anoFinalAto": "",
            "nroInicialAto": "",
            "nroFinalAto": "",
            "tipoAto": "",
            "nroAto": "",
            "anoAto": "",
            "tema": "0",
            "anoInicialAtoTema": "",
            "anoFinalAtoTema": "",
            "nroInicialAtoTema": "",
            "nroFinalAtoTema": "",
        }

    def _build_form_data(self, year: int, norm_type_id: int) -> dict:
        """Return form data dict for a specific year + norm type."""
        data = self._base_form_data.copy()
        data["tiposAtoStr"] = str(norm_type_id)
        data["tiposAtoTema"] = str(norm_type_id)
        data["anoInicialAtoTema"] = str(year)
        data["anoFinalAtoTema"] = str(year)
        return data

    def _search_url(self, page: int = 1, total_records: int | None = None) -> str:
        """Build the search URL with pagination parameters."""
        base = f"{self.base_url}/legislacao/pesquisarAto.do?action=listar&opt=tm"
        if total_records is not None:
            return f"{base}&indice={page}&totalRegistros={total_records}#resultado"
        return f"{base}&indice={page}&site=1"

    async def _fetch_search_page(
        self,
        year: int,
        norm_type_id: int,
        page: int = 1,
        total_records: int | None = None,
        numbers: tuple[int, int] | None = None,
    ) -> BeautifulSoup:
        """POST the search form and return parsed HTML."""
        url = self._search_url(page, total_records)
        form_data = self._build_form_data(year, norm_type_id)
        if numbers is not None:
            form_data["nroInicialAtoTema"], form_data["nroFinalAtoTema"] = map(
                str, numbers
            )
        response = await self.http.request(
            url=url, method="POST", data=form_data, timeout=30
        )
        html = response.text
        return BeautifulSoup(html, "lxml")

    @staticmethod
    def _parse_results_table(soup: BeautifulSoup) -> list[dict]:
        """Extract document metadata from the results table."""
        table = soup.find("table", id="list_tabela")
        if not table:
            return []
        docs: list[dict] = []
        rows = table.find_all("tr", class_=_RE_LIST_ITEMS)
        for row in rows:
            tds = row.find_all("td")
            if len(tds) < 4:
                continue
            link_tag = tds[0].find("a", href=True)
            if not link_tag:
                continue
            try:
                cod_ato = link_tag["href"].split("'")[1]
            except IndexError, KeyError:
                continue
            docs.append(
                {
                    "id": cod_ato,
                    "title": tds[1].text.strip(),
                    "summary": tds[2].text.strip(),
                    "date": tds[3].text.strip(),
                }
            )
        return docs

    async def fetch(self, doc_info: dict) -> Document:
        """Fetch a single document's HTML and convert to markdown.

        Plain HTTP: the HTML is complete without JavaScript, and the site
        temporarily blocks fast clients (403 "Acesso temporariamente
        bloqueado"), so one request per act beats a full browser page load.
        """
        cod_ato = doc_info.get("id", "")
        html_link = urljoin(
            self.base_url, f"/legislacao/pesquisarAto.do?action=exibir&codAto={cod_ato}"
        )
        response = await self.http.request(html_link)
        soup = response.soup()
        form = soup.find("form", attrs={"name": "pesquisarAtoForm"})
        if not form:
            raise ValueError("No pesquisarAtoForm found")
        table = form.find("table", id="list_tabela")
        if table:
            table.decompose()
        html_string = str(form).replace("\n ANEXOS:", "").strip()
        html_string = html_string.replace("javascript:listarAssinaturas();", "")
        html_string = wrap_html(html_string)
        situation = self._infer_situation(soup)
        text_markdown = (await self.markdown(html=html_string)).strip()
        valid, reason = valid_markdown(text_markdown)
        if not valid:
            raise ValueError(f"Invalid markdown: {reason}")
        result = {
            **doc_info,
            "text_markdown": text_markdown,
            "document_url": html_link,
            "situation": situation,
            "raw": response.content,
            "raw_ext": ".html",
        }
        return Document(**result)

    @staticmethod
    def _totals(soup: BeautifulSoup) -> tuple[int, int]:
        """Return ``(records, pages)`` of a search result page."""
        # Zero hits show only as a script-injected message, invisible to get_text().
        if "Nenhum registro encontrado" in str(soup):
            return (0, 0)
        text = soup.get_text()
        records, pages = _RE_TOTAL_RECORDS.search(text), _RE_TOTAL_PAGES.search(text)
        if records and int(records.group(1)) == 0:
            return (0, 0)
        if not (records and pages):
            raise ValueError("Could not parse Paraná pagination")
        return (int(records.group(1)), int(pages.group(1)))

    async def _list_numbers(
        self, year: int, norm_type_id: str, low: int, high: int, counted: list[int]
    ) -> AsyncIterator[dict]:
        """Rows of acts numbered low..high, splitting the range until it fits one page.

        Appends each final query's record total to ``counted``.
        """
        soup = await self._fetch_search_page(year, norm_type_id, numbers=(low, high))
        records, pages = self._totals(soup)
        if pages > 1 and low < high:
            for sub_low, sub_high in _split(low, high, -(-records // _ACTS_PER_QUERY)):
                async for row in self._list_numbers(
                    year, norm_type_id, sub_low, sub_high, counted
                ):
                    yield row
            return
        counted.append(records)
        for row in self._parse_results_table(soup):
            yield row
        # More acts than a page under one number (never seen): page anyway.
        for page in range(2, pages + 1):
            soup = await self._fetch_search_page(
                year, norm_type_id, page, records, numbers=(low, high)
            )
            for row in self._parse_results_table(soup):
                yield row

    async def _rows(
        self,
        year: int,
        norm_type_id: str,
        first: BeautifulSoup,
        records: int,
        pages: int,
    ) -> AsyncIterator[dict]:
        """Result rows, possibly repeated; streamed so workers start before listing ends."""
        rows = self._parse_results_table(first)
        for row in rows:
            yield row
        if pages <= 1:
            return
        # Date order puts the newest acts first and the oldest last: number bounds.
        last = self._parse_results_table(
            await self._fetch_search_page(year, norm_type_id, pages, records)
        )
        for row in last:
            yield row
        numbers = [
            int(match.group(1).replace(".", ""))
            for row in rows + last
            if (match := _RE_ACT_NUMBER.match(row["title"]))
        ]
        if not numbers:
            raise ValueError("Paraná acts have no numbers to split the listing by")
        low, high = min(numbers), max(numbers)
        ranges = [(0, low - 1), (high + 1, _MAX_ACT_NUMBER)]
        counted: list[int] = []
        for range_low, range_high in ranges + _split(
            low, high, -(-records // _ACTS_PER_QUERY)
        ):
            if range_low <= range_high:
                async for row in self._list_numbers(
                    year, norm_type_id, range_low, range_high, counted
                ):
                    yield row
        # Single-page queries show all they will ever show, so only acts outside every
        # number range can be missing. The site's own counts can exceed the rows it
        # shows (Lei 22.530/2025 counts twice); those cancel out here.
        if sum(counted) < records:
            raise ValueError(
                f"{records - sum(counted)} Paraná acts have no number; only those "
                "on the first and last result pages are listed"
            )

    async def list_type(
        self, year: int, norm_type: str, norm_type_id: str
    ) -> AsyncIterator[dict]:
        # Results sort by date without a tiebreak, so consecutive pages overlap and skip
        # acts sharing a date (2025: 649 of 682 Leis listed). Query by act-number
        # ranges that each fit one page instead.
        soup = await self._fetch_search_page(year, norm_type_id)
        records, pages = self._totals(soup)
        seen: set[str] = set()
        async for ref in self._rows(year, norm_type_id, soup, records, pages):
            if ref["id"] in seen:
                continue
            seen.add(ref["id"])
            yield {
                **ref,
                "year": year,
                "type": norm_type,
                "situation": DEFAULT_VALID_SITUATION,
                "document_url": urljoin(
                    self.base_url,
                    f"/legislacao/pesquisarAto.do?action=exibir&codAto={ref['id']}",
                ),
            }

    @staticmethod
    def _infer_situation(soup: BeautifulSoup) -> str:
        status = soup.find(string=re.compile("^\\s*Situa[çc][ãa]o\\s*:", re.IGNORECASE))
        if status is not None:
            text = status.parent.get_text(" ", strip=True)
            sibling = status.parent.find_next_sibling()
            if sibling is not None and (not text.partition(":")[2].strip()):
                text += " " + sibling.get_text(" ", strip=True)
            if _RE_INVALID.search(text):
                return DEFAULT_INVALID_SITUATION
        return DEFAULT_VALID_SITUATION
