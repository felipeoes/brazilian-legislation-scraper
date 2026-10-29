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
from loguru import logger

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
    ) -> BeautifulSoup | None:
        """POST the search form and return parsed HTML."""
        url = self._search_url(page, total_records)
        form_data = self._build_form_data(year, norm_type_id)
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

    async def fetch(self, doc_info: dict) -> Document | None:
        """Fetch a single document's HTML and convert to markdown."""
        cod_ato = doc_info.get("id", "")
        html_link = urljoin(
            self.base_url, f"/legislacao/pesquisarAto.do?action=exibir&codAto={cod_ato}"
        )
        try:
            soup, mhtml = await self.browser.snapshot(html_link)
        except Exception as exc:
            logger.warning(f"Failed to fetch doc {cod_ato}: {exc}")
            raise ValueError(str(exc))
        form = soup.find("form", attrs={"name": "pesquisarAtoForm"})
        if not form:
            logger.warning(f"No document form found for {cod_ato}")
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
            logger.warning(f"Invalid markdown for doc {cod_ato}: {reason}")
            return None
        result = {
            **doc_info,
            "text_markdown": text_markdown,
            "document_url": html_link,
            "situation": situation,
            "raw": mhtml,
            "raw_ext": ".mhtml",
        }
        from src.core.document import Document

        return Document(**result)

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        for norm_type, norm_type_id in self.types.items():
            soup = await self._fetch_search_page(year, norm_type_id)
            text = soup.get_text()
            page_match = _RE_TOTAL_PAGES.search(text)
            records_match = _RE_TOTAL_RECORDS.search(text)
            if records_match and int(records_match.group(1)) == 0:
                continue
            if not page_match:
                raise ValueError("Could not parse Paraná pagination")
            records = int(records_match.group(1)) if records_match else None
            for page in range(1, int(page_match.group(1)) + 1):
                if page > 1:
                    soup = await self._fetch_search_page(
                        year, norm_type_id, page, records
                    )
                for ref in self._parse_results_table(soup):
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
