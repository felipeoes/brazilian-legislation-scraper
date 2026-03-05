"""Paraná state legislation scraper using direct HTTP requests.

Scrapes the Casa Civil legislation system at ``legislacao.pr.gov.br``
by posting search forms and fetching document pages via HTTP — no
browser or VPN required.
"""

from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from loguru import logger

from src.scraper.base.scraper import (
    StateScraper,
    DEFAULT_VALID_SITUATION,
    DEFAULT_INVALID_SITUATION,
)

TYPES = {
    "Lei": 1,
    "Lei Complementar": 3,
    "Consituição Estadual": 10,
    "Decreto": 11,
    "Emenda Constitucional": 9,
    "Resolução": 13,
    "Portaria": 14,
}

# Casa Civil does not expose a "situation" field — validity is inferred
# from the document text (e.g. "Revogado pelo …").
VALID_SITUATIONS: list[str] = []
INVALID_SITUATIONS: list[str] = []
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS

_RE_LIST_ITEMS = re.compile(r"list_cor_(sim|nao)")
_RE_INVALID = re.compile(r"(Revogado pelo|Revogada pela|Revogado por|Revogada por)")
_RE_TOTAL_PAGES = re.compile(r"Página \d+ de (\d+)")
_RE_TOTAL_RECORDS = re.compile(r"Total de (\d+) registros")


class ParanaCVScraper(StateScraper):
    """Scraper for Paraná state legislation (Casa Civil).

    Uses plain HTTP POST requests against the J2EE search form at
    ``legislacao.pr.gov.br``.  No Playwright / VPN needed.
    """

    def __init__(
        self,
        base_url: str = "https://www.legislacao.pr.gov.br",
        **kwargs,
    ):
        super().__init__(
            base_url,
            types=TYPES,
            situations=SITUATIONS,
            name="PARANA",
            **kwargs,
        )
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

    # ── Search helpers ─────────────────────────────────────────────

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
        return f"{base}&indice{page}&site=1"

    # ── Page fetching ──────────────────────────────────────────────

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

        response = await self.request_service.make_request(
            url=url,
            method="POST",
            payload=form_data,
            timeout=30,
        )
        if not response or response.status != 200:
            status = response.status if response else "No response"
            logger.warning(f"Search page {page} failed: {status}")
            return None

        html = await response.text()
        return BeautifulSoup(html, "html.parser")

    # ── Link extraction ────────────────────────────────────────────

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
            except (IndexError, KeyError):
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

    async def _get_all_docs_links(self, year: int, norm_type_id: int) -> list[dict]:
        """Fetch all pages of search results for a year + norm type."""
        # Page 1 — also determines total pages / records
        soup = await self._fetch_search_page(year, norm_type_id)
        if not soup:
            return []

        text = soup.get_text()
        total_pages_match = _RE_TOTAL_PAGES.search(text)
        total_records_match = _RE_TOTAL_RECORDS.search(text)

        if not total_pages_match:
            return []

        total_pages = int(total_pages_match.group(1))
        total_records = (
            int(total_records_match.group(1)) if total_records_match else None
        )

        # Parse page 1 results
        all_docs = self._parse_results_table(soup)

        if self.verbose:
            logger.info(
                f"PARANA | Type {norm_type_id} | Year {year}: "
                f"{total_records or '?'} records, {total_pages} pages"
            )

        # Fetch remaining pages concurrently
        if total_pages > 1:
            tasks = [
                self._fetch_search_page(year, norm_type_id, p, total_records)
                for p in range(2, total_pages + 1)
            ]
            page_results = await self._gather_results(
                tasks,
                context={"year": year, "type": str(norm_type_id), "situation": "N/A"},
                desc=f"PARANA | type {norm_type_id} | pages",
            )
            for page_soup in page_results:
                if page_soup:
                    all_docs.extend(self._parse_results_table(page_soup))

        return all_docs

    # ── Document content ───────────────────────────────────────────

    @staticmethod
    def _infer_situation(soup: BeautifulSoup) -> str:
        """Infer validity from document text (revogado/revogada patterns)."""
        text = soup.get_text()
        if _RE_INVALID.search(text):
            return DEFAULT_INVALID_SITUATION
        return DEFAULT_VALID_SITUATION

    async def _get_doc_data(self, doc_info: dict) -> dict | None:
        """Fetch a single document's HTML and convert to markdown."""
        cod_ato = doc_info.get("id", "")
        doc_title = doc_info.get("title", "")
        html_link = urljoin(
            self.base_url,
            f"/legislacao/pesquisarAto.do?action=exibir&codAto={cod_ato}",
        )

        if self._is_already_scraped(html_link, doc_title):
            return None

        response = await self.request_service.make_request(html_link, timeout=30)
        if not response or response.status != 200:
            status = response.status if response else "No response"
            logger.warning(f"Failed to fetch doc {cod_ato}: {status}")
            await self._save_doc_error(
                title=doc_title,
                year=doc_info.get("date", "")[-4:],
                situation="",
                norm_type="",
                html_link=html_link,
                error_message=f"HTTP {status}",
            )
            return None

        html = await response.text()
        soup = BeautifulSoup(html, "html.parser")
        form = soup.find("form", attrs={"name": "pesquisarAtoForm"})

        if not form:
            logger.warning(f"No document form found for {cod_ato}")
            await self._save_doc_error(
                title=doc_title,
                year=doc_info.get("date", "")[-4:],
                situation="",
                norm_type="",
                html_link=html_link,
                error_message="No pesquisarAtoForm found",
            )
            return None

        # Remove the results table if present in the form
        table = form.find("table", id="list_tabela")
        if table:
            table.decompose()

        html_string = form.prettify().replace("\n ANEXOS:", "").strip()
        html_string = html_string.replace("javascript:listarAssinaturas();", "")
        html_string = self._wrap_html(html_string)

        situation = self._infer_situation(soup)

        text_markdown = (await self._get_markdown(html_content=html_string)).strip()
        if not text_markdown:
            logger.warning(f"Empty markdown for doc {cod_ato}")
            return None

        result = {
            **doc_info,
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": html_link,
            "situation": situation,
            "_raw_content": html_string.encode("utf-8"),
            "_content_extension": ".html",
        }

        return result

    # ── Year-level orchestration ───────────────────────────────────

    async def _scrape_type(
        self, norm_type: str, norm_type_id: int, year: int
    ) -> list[dict]:
        """Scrape all documents of a given type for a year."""
        docs = await self._get_all_docs_links(year, norm_type_id)

        if not docs:
            return []

        for doc in docs:
            doc["year"] = year
        ctx = {"year": year, "type": norm_type, "situation": "N/A"}
        tasks = [self._with_save(self._get_doc_data(doc), ctx) for doc in docs]
        results = await self._gather_results(
            tasks,
            context=ctx,
            desc=f"PARANA | {norm_type}",
        )

        if self.verbose:
            logger.info(
                f"Finished scraping Year: {year} | Type: {norm_type} | "
                f"Results: {len(results)} | Total: {self.count}"
            )

        return results

    # _scrape_year uses default from StateScraper
