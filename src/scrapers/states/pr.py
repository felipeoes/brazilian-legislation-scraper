"""Paraná state legislation scraper using direct HTTP requests.

Scrapes the Casa Civil legislation system at ``legislacao.pr.gov.br``: one
unpaginated year listing per type, then one page per act.
"""

import re
from collections.abc import AsyncIterator
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
_RE_TOTAL_RECORDS = re.compile("Total de (\\d+) registros")
_RE_COD_ATO = re.compile(r"codAto=(\d+)")
_RE_SITE_ERROR = re.compile(r'class="msg_aviso">([^<]*)')


class ParanaCVScraper(BaseScraper):
    """Scraper for Paraná state legislation (Casa Civil).

    Year start (earliest on source): 2025

    Lists each type and year with one unpaginated request to the site's year
    listing (``listarAtosAno.do``), then fetches each act's page over HTTP.
    """

    types = TYPES
    situations = SITUATIONS
    name = "PARANA"
    base_url = "https://www.legislacao.pr.gov.br"

    @staticmethod
    def _parse_results_table(soup: BeautifulSoup) -> list[dict]:
        """Extract document metadata from the results table."""
        table = soup.find("table", id="list_tabela")
        if not table:
            return []
        docs: list[dict] = []
        for row in table.find_all("tr", class_=_RE_LIST_ITEMS):
            tds = row.find_all("td")
            link_tag = tds[0].find("a", href=True) if len(tds) >= 4 else None
            match = _RE_COD_ATO.search(link_tag["href"]) if link_tag else None
            if not match:
                continue
            docs.append(
                {
                    "id": match.group(1),
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
    def _total(soup: BeautifulSoup) -> int:
        """Return the act count a year listing reports."""
        html = str(soup)
        # The site's error page also says "Nenhum registro encontrado".
        if error := _RE_SITE_ERROR.search(html):
            raise ValueError(f"Paraná listing failed: {error.group(1).strip()}")
        if match := _RE_TOTAL_RECORDS.search(soup.get_text()):
            return int(match.group(1))
        if "Nenhum registro encontrado" in html:
            return 0
        raise ValueError("Unrecognized Paraná listing page")

    async def list_type(
        self, year: int, norm_type: str, norm_type_id: int
    ) -> AsyncIterator[dict]:
        # One unpaginated request lists every act of the type and year, from every
        # agency. The paginated search sorts by date without a tiebreak, so its pages
        # overlapped and skipped acts (2025: 649 of 682 Leis).
        response = await self.http.request(
            f"{self.base_url}/legislacao/listarAtosAno.do"
            f"?action=listarAtos&anoAto={year}&isPaginado=false",
            method="POST",
            data={
                "codTipoAto": str(norm_type_id),
                "codTipoAtoSelecionado": str(norm_type_id),
                "codOrgaoUnidade": "",
                "codOrgaoUnidadeSelecionado": "",
                "pesquisou": "",
                "anoAtoPesquisado": "0",
                "paginaAtual": "",
                "isPaginado": "true",
                "site": "1",
            },
            # A year of resoluções (15k rows) takes the server ~2 minutes.
            timeout=600,
        )
        soup = response.soup()
        total = self._total(soup)
        rows = self._parse_results_table(soup)
        if len(rows) != total:
            raise ValueError(f"Paraná listed {len(rows)} of {total} acts")
        seen: set[str] = set()
        for row in rows:
            if row["id"] in seen:  # the site repeats a few rows and counts them
                continue
            seen.add(row["id"])
            yield {
                **row,
                "year": year,
                "type": norm_type,
                "situation": DEFAULT_VALID_SITUATION,
                "document_url": urljoin(
                    self.base_url,
                    f"/legislacao/pesquisarAto.do?action=exibir&codAto={row['id']}",
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
