from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument
import re
from urllib.parse import unquote, urljoin, urlparse
from typing import cast

from bs4 import BeautifulSoup, Tag
from loguru import logger
from src.scraper.base.converter import (
    calc_pages,
    infer_type_from_title,
    strip_html_chrome,
)
from src.scraper.base.scraper import BaseScraper, flatten_results

# ---------------------------------------------------------------------------
# Situation constants — kept for documentation and downstream filtering.
# NOT used as search query filters anymore; situations are read directly from
# each listing-page result item (p.busca-resultados__situacao).
# ---------------------------------------------------------------------------

VALID_SITUATIONS = [
    "Não consta revogação expressa",
    "Não Informado",  # no explicit revocation info → treated as valid
    "Convertida em Lei",
    "Reeditada",
    "Reeditada com alteração",
]

INVALID_SITUATIONS = [
    "Arquivada",
    "Rejeitada",
    "Revogada",
    "Sem Eficácia",
]


# Empty string → search returns all coverage (Toda legislação).
# OPTIONS: 'Legislação Interna' or 'Legislação Federal'
COVERAGE = ""

# Human-readable label → URL-encoded value used as the `tipo` query param.
TYPES = {
    "Alvará": "Álvará",
    "Ato": "Ato",
    "Ato Conjunto": "Ato+Conjunto",
    "Ato da Mesa": "Ato+da+Mesa",
    "Ato da Presidência Sem Número": "Ato+da+Presid%C3%AAncia+Sem+N%C3%BAmero",
    "Ato Declaratório do Presidente da Mesa": "Ato+Declaratório+do+Presidente+da+Mesa",
    "Ato do Presidente da Mesa": "Ato+do+Presidente+da+Mesa",
    "Ato do Presidente Sem Número": "Ato+do+Presidente+Sem+N%C3%BAmero",
    "Ato Sem Número": "Ato+Sem+N%C3%BAmero",
    "Carta Régia": "Carta%20Régia",
    "Carta Imperial": "Carta+Imperial",
    "Constituição": "Constitui%C3%A7%C3%A3o",
    "Decisão da Mesa Sem Número": "Decis%C3%A3o+da+Mesa+Sem+N%C3%BAmero",
    "Decisão": "Decisão",
    "Decreto": "Decreto",
    "Decreto Legislativo": "Decreto+Legislativo",
    "Decreto Sem Número": "Decreto+Sem+N%C3%BAmero",
    "Decreto-Lei": "Decreto-Lei",
    "Emenda Constitucional": "Emenda+Constitucional",
    "Instrução": "Instrução",
    "Lei Complementar": "Lei+Complementar",
    "Lei Ordinária": "Lei+Ordin%C3%A1ria",
    "Manifesto": "Manifesto",
    "Mensagem": "Mensagem",
    "Pacto": "Pacto",
    "Proclamação": "Proclamação",
    "Protocolo": "Protocolo",
    "Medida Provisória": "Medida+Provis%C3%B3ria",
    "Ordem de Serviço": "Ordem+de+Serviço",
    "Portaria": "Portaria",
    "Regulamento": "Regulamento",
    "Resolução": "Resolu%C3%A7%C3%A3o",
    "Resolução da Assembleia Nacional Constituinte": "Resolu%C3%A7%C3%A3o+da+Assembl%C3%A9ia+Nacional+Constituinte",
    "Resolução da Câmara dos Deputados": "Resolu%C3%A7%C3%A3o+da+C%C3%A2mara+dos+Deputados",
    "Resolução da Mesa": "Resolução+da+Mesa",
    "Resolução do Congresso Nacional": "Resolu%C3%A7%C3%A3o+do+Congresso+Nacional",
    "Resolução do Senado Federal": "Resolu%C3%A7%C3%A3o+do+Senado+Federal",
}

_FEDERAL_TYPE_SLUG_MAP = {
    "alvara": "Alvará",
    "ata_sn": "Ata",
    "ato": "Ato",
    "ato_sn": "Ato Sem Número",
    "atocom": "Ato Complementar",
    "atocon": "Ato Conjunto",
    "atocon_sn": "Ato Conjunto",
    "atocsr": "Ato do Comando Supremo da Revolução",
    "atodec": "Ato Declaratório",
    "atodec_sn": "Ato Declaratório",
    "atodecpm": "Ato Declaratório do Presidente da Mesa",
    "atoins": "Ato Institucional",
    "atomes": "Ato da Mesa",
    "atomes_sn": "Ato da Mesa",
    "atonor": "Ato Normativo",
    "atopre_sn": "Ato da Presidência Sem Número",
    "atoprt": "Ato do Presidente",
    "atoprt_sn": "Ato do Presidente Sem Número",
    "atoprtm": "Ato do Presidente da Mesa",
    "atoprtm_sn": "Ato do Presidente da Mesa",
    "atopvp": "Ato do Primeiro-Vice-Presidente",
    "carimp": "Carta Imperial",
    "carlei": "Carta de Lei",
    "carlei_sn": "Carta de Lei",
    "carpat_sn": "Carta Patente",
    "carreg_sn": "Carta Régia",
    "carta_sn": "Carta",
    "circul": "Circular",
    "comuni_sn": "Comunicado",
    "conadc": "Ato das Disposições Constitucionais Transitórias",
    "consti": "Constituição",
    "decisa": "Decisão",
    "decisa_sn": "Decisão",
    "declcn": "Decreto Legislativo",
    "decleg": "Decreto Legislativo",
    "declei": "Decreto-Lei",
    "decmin": "Decreto do Conselho de Ministros",
    "decpre_sn": "Decisão do Presidente",
    "decres": "Decreto Reservado",
    "decret": "Decreto",
    "decret_sn": "Decreto Sem Número",
    "dmsn": "Decisão da Mesa Sem Número",
    "dpsn": "Decisão da Presidência",
    "emecon": "Emenda Constitucional",
    "emecon_sn": "Emenda Constitucional",
    "emecrv": "Emenda Constitucional de Revisão",
    "instno": "Instrução Normativa",
    "instru": "Instrução",
    "instse": "Instrução de Serviço",
    "lei": "Lei Ordinária",
    "lei_sn": "Lei Ordinária",
    "leicom": "Lei Complementar",
    "leicon": "Lei Constitucional",
    "leidel": "Lei Delegada",
    "leimp": "Lei Ordinária",
    "manife_sn": "Manifesto",
    "medpro": "Medida Provisória",
    "mensag": "Mensagem",
    "mensag_sn": "Mensagem",
    "ordsec": "Ordem de Serviço Conjunta",
    "ordser": "Ordem de Serviço",
    "ordser-sn": "Ordem de Serviço",
    "pacto_sn": "Pacto",
    "pactorep": "Pacto Republicano",
    "portar": "Portaria",
    "portar_con": "Portaria Conjunta",
    "portar_sn": "Portaria",
    "procla_sn": "Proclamação",
    "protoc_sn": "Protocolo",
    "regim_sn": "Regimento",
    "regula": "Regulamento",
    "regula_sn": "Regulamento",
    "resaco": "Resolução da Assembleia Nacional Constituinte",
    "rescad": "Resolução da Câmara dos Deputados",
    "rescon": "Resolução do Congresso Nacional",
    "resmes": "Resolução da Mesa",
    "resmes_sn": "Resolução da Mesa",
    "resolu": "Resolução",
    "resolu_sn": "Resolução",
    "ressen": "Resolução do Senado Federal",
}

_FEDERAL_TITLE_PREFIX_ALIASES = {
    "Ato Declaratório do Presidente da Mesa": "Ato Declaratório do Presidente da Mesa",
    "Ato da Presidência": "Ato da Presidência Sem Número",
    "Ato do Presidente da Mesa": "Ato do Presidente da Mesa",
    "Ato do Presidente": "Ato do Presidente",
    "Ato da Mesa": "Ato da Mesa",
    "Ato Conjunto": "Ato Conjunto",
    "Ato Complementar": "Ato Complementar",
    "Ato Institucional": "Ato Institucional",
    "Ato Normativo": "Ato Normativo",
    "Ato": "Ato",
    "Carta de Lei": "Carta de Lei",
    "Carta Régia": "Carta Régia",
    "Carta Imperial": "Carta Imperial",
    "Carta Patente": "Carta Patente",
    "Carta": "Carta",
    "Comunicado": "Comunicado",
    "Circular": "Circular",
    "Constituição. ADCT": "Ato das Disposições Constitucionais Transitórias",
    "Constituição": "Constituição",
    "Decisão da Mesa": "Decisão da Mesa Sem Número",
    "Decisão da Presidência": "Decisão da Presidência",
    "Decisão do Presidente": "Decisão do Presidente",
    "Decisão": "Decisão",
    "Decreto do Conselho de Ministros": "Decreto do Conselho de Ministros",
    "Decreto Legislativo": "Decreto Legislativo",
    "Decreto-Lei": "Decreto-Lei",
    "Decreto Reservado": "Decreto Reservado",
    "Decreto": "Decreto",
    "Emenda Constitucional de Revisão": "Emenda Constitucional de Revisão",
    "Emenda Constitucional": "Emenda Constitucional",
    "Instrução Normativa": "Instrução Normativa",
    "Instrução de Serviço": "Instrução de Serviço",
    "Instrução": "Instrução",
    "Lei Complementar": "Lei Complementar",
    "Lei Constitucional": "Lei Constitucional",
    "Lei Delegada": "Lei Delegada",
    "Lei": "Lei Ordinária",
    "Manifesto": "Manifesto",
    "Mensagem": "Mensagem",
    "Medida Provisória": "Medida Provisória",
    "Ordem de Serviço Conjunta": "Ordem de Serviço Conjunta",
    "Ordem de Serviço": "Ordem de Serviço",
    "Pacto Republicano": "Pacto Republicano",
    "Pacto": "Pacto",
    "Portaria Conjunta": "Portaria Conjunta",
    "Portaria": "Portaria",
    "Proclamação": "Proclamação",
    "Protocolo": "Protocolo",
    "Regimento": "Regimento",
    "Regulamento": "Regulamento",
    "Resolução da Assembleia Nacional Constituinte": "Resolução da Assembleia Nacional Constituinte",
    "Resolução da Câmara dos Deputados": "Resolução da Câmara dos Deputados",
    "Resolução da Mesa": "Resolução da Mesa",
    "Resolução do Congresso Nacional": "Resolução do Congresso Nacional",
    "Resolução do Senado Federal": "Resolução do Senado Federal",
    "Resolução": "Resolução",
}

_FEDERAL_TYPE_PREFIX_RE = re.compile(
    r"(.+?)(?:\s+(?:n[º°o]|n\.|número)\s*[\dA-Za-z]|\s+\d|\s+de\s)",
    re.IGNORECASE,
)

ORDERING = "data%3AASC"
YEAR_START = 1808
EXPORT_MAX_DOCS = 300


class CamaraDepScraper(BaseScraper):
    """Scraper for the Câmara dos Deputados legislation portal.

    Source: https://www.camara.leg.br/legislacao/
    Earliest year available: 1808
    35+ norm types (see module-level ``TYPES`` dict).

    ## How it works

    ### Entry point
    ``scrape()`` (inherited from ``BaseScraper``) iterates years sequentially,
    calling ``_scrape_year(year)`` for each one.

    ### Year scraping  — ``_scrape_year``
    Delegates immediately to ``_scrape_type("", "", year)``, passing an empty
    ``tipo`` parameter.  This makes the search API return **all types at once**,
    eliminating 35 separate per-type requests.  Each document's norm type is
    then inferred from its title via ``_infer_norm_type``.

    ### Three-phase pipeline  — ``_scrape_type``

    **Phase 1 — listing (collect metadata)**

    Fetches the first search-results page and reads the total result count from
    ``div.busca-info__resultado``.

    - If total ≤ ``export_max_docs`` (default 300): requests the export URL
      (``busca/exportar?…``), which returns **all results in a single page**.
      Falls back to pagination if the export count doesn't match.
    - Otherwise: paginates concurrently across all pages (20 results/page).

    Each result yields: ``{title, summary, metadata_url, situation}``.
    ``situation`` is read from ``p.busca-resultados__situacao`` on the listing
    page and is **never** used as a search filter.

    **Phase 2 — resolve text URLs**

    For each document, visits its metadata page (``metadata_url``) to find the
    actual text URL.  Priority order among ``div.sessao`` anchor links:

    1. ``"texto - republicação"`` (last match)
    2. ``"texto - publicação original"`` (last match)
    3. Any link containing ``"texto"``

    Resolved mappings are cached in ``_metadata_to_text_url`` and persisted
    across restarts by ``_load_scraped_keys``, so already-visited metadata
    pages are skipped on subsequent runs.

    **Phase 3 — fetch, convert, save**

    For each resolved document not yet saved (``_is_already_scraped``):

    1. Fetches the text HTML page (``document_url``).
    2. Extracts the main content area: ``div#content`` >
       ``div.textoNorma`` > whole page.
    3. Strips portal chrome (``strip_html_chrome`` + ``_clean_norm_soup``),
       ``<h1>`` tags (title already captured), and ``<p class="ementa">``
       (summary already captured).
    4. Converts to Markdown via ``_get_markdown``.
    5. If the result is empty but ementa nodes were present, retries with
       ementa included as a fallback.
    6. Saves via ``_process_documents`` (raw HTML + ``data.json`` entry).

    Output schema per document::

        {
            "title": str,
            "summary": str,        # ementa text, stripped of "Ementa:" prefix
            "situation": str,      # e.g. "Não consta revogação expressa"
            "text_markdown": str,
            "document_url": str,   # URL of the full text page
            "metadata_url": str,   # URL of the document detail/metadata page
            "_raw_content": bytes, # MHTML snapshot bytes
            "_content_extension": ".mhtml",
        }

    Example search URL::

        https://www.camara.leg.br/legislacao/busca?abrangencia=&geral=&ano=2020
            &situacao=&origem=&numero=&ordenacao=data%3AASC&tipo=Decreto
    """

    def __init__(
        self,
        base_url: str = "https://www.camara.leg.br/legislacao/",
        **kwargs,
    ):
        coverage = kwargs.pop("coverage", COVERAGE)
        ordering = kwargs.pop("ordering", ORDERING)
        export_max_docs = kwargs.pop("export_max_docs", EXPORT_MAX_DOCS)
        super().__init__(
            base_url,
            name="LEGISLACAO_FEDERAL",
            types=TYPES,
            situations={},  # situations read from listing page, not used as filter
            **kwargs,
        )
        self.coverage = coverage
        self.ordering = ordering
        self.export_max_docs = export_max_docs
        self._metadata_to_text_url: dict[str, str] = {}

    async def _load_scraped_keys(self, year: int) -> None:
        await super()._load_scraped_keys(year)
        self._metadata_to_text_url = {}
        if not self.saver or self.overwrite:
            return

        year_docs = await self.saver.get_year_documents(year)
        for doc in year_docs:
            metadata_url = str(doc.get("metadata_url", ""))
            document_url = str(doc.get("document_url", ""))
            if metadata_url and document_url:
                self._metadata_to_text_url[metadata_url] = document_url

        if self._metadata_to_text_url:
            logger.debug(
                f"{self.__class__.__name__} | Year {year}: loaded "
                f"{len(self._metadata_to_text_url)} metadata->text URL mappings"
            )

    def _format_search_url(self, year: str, norm_type_id: str) -> str:
        """Format search URL for a given year and norm type (URL-encoded value)."""
        params = {
            "abrangencia": self.coverage,
            "geral": "",
            "ano": year,
            "situacao": "",  # no situation filter — all situations returned
            "origem": "",
            "numero": "",
            "ordenacao": self.ordering,
            "tipo": norm_type_id,
        }
        return (
            self.base_url
            + "busca?"
            + "&".join(f"{key}={value}" for key, value in params.items())
        )

    def _format_export_url(self, year: str, norm_type_id: str) -> str:
        """Format export URL for a given year and norm type."""
        params = {
            "geral": "",
            "ano": year,
            "ordenacao": unquote(self.ordering),
            "abrangencia": self.coverage,
            "tipo": norm_type_id,
            "origem": "",
            "situacao": "",
            "numero": "",
        }
        return (
            self.base_url
            + "busca/exportar?&"
            + "&".join(f"{key}={value}" for key, value in params.items())
        )

    @staticmethod
    def _extract_total_results(soup: BeautifulSoup) -> int | None:
        total_element = soup.find(
            "div",
            class_="busca-info__resultado busca-info__resultado--informado",
        )
        if total_element is None:
            return None

        try:
            return int(total_element.get_text(" ", strip=True).split()[-1])
        except (TypeError, ValueError, IndexError):
            return None

    @staticmethod
    def _strip_prefix(value: str, prefix: str) -> str:
        value = value.strip()
        if value.lower().startswith(prefix.lower()):
            return value[len(prefix) :].strip()
        return value

    def _parse_listing_page_documents(self, soup: BeautifulSoup) -> list[dict]:
        """Parse one standard search-results page into document metadata."""
        documents_metadata = []
        documents = soup.find_all("li", class_="busca-resultados__item")
        for document in documents:
            heading = document.find("h3", class_="busca-resultados__cabecalho")
            if not heading:
                continue
            a_tag = heading.find("a", href=True)
            if not a_tag:
                continue

            metadata_url = a_tag["href"]
            title = a_tag.get_text(" ", strip=True)

            summary_tag = document.find(
                "p", class_="busca-resultados__descricao js-fade-read-more"
            )
            summary = ""
            if summary_tag:
                summary = self._strip_prefix(
                    summary_tag.get_text(" ", strip=True), "Ementa:"
                )

            situation_tag = document.find("p", class_="busca-resultados__situacao")
            situation = ""
            if situation_tag:
                situation = self._strip_prefix(
                    situation_tag.get_text(" ", strip=True), "Situação:"
                )

            documents_metadata.append(
                {
                    "title": title,
                    "summary": summary,
                    "metadata_url": metadata_url,
                    "situation": situation,
                }
            )

        return documents_metadata

    def _parse_export_documents(self, soup: BeautifulSoup) -> list[dict]:
        """Parse the export HTML page into document metadata."""
        container = soup.find("div", id="impressaoPDF") or soup
        documents_metadata = []

        for document in container.find_all("li"):
            a_tag = document.find("a", href=True)
            if not a_tag:
                continue

            title = a_tag.get_text(" ", strip=True)
            metadata_url = a_tag["href"]
            summary = ""
            situation = ""

            for paragraph in document.find_all("p"):
                text = paragraph.get_text(" ", strip=True)
                lowered = text.lower()
                if lowered.startswith("ementa:"):
                    summary = self._strip_prefix(text, "Ementa:")
                elif lowered.startswith("situação:") or lowered.startswith("situacao:"):
                    situation = self._strip_prefix(text, "Situação:")
                    if situation == text:
                        situation = self._strip_prefix(text, "Situacao:")

            documents_metadata.append(
                {
                    "title": title,
                    "summary": summary,
                    "metadata_url": metadata_url,
                    "situation": situation,
                }
            )

        return documents_metadata

    async def _get_docs_links(self, url: str) -> list[dict]:
        """Fetch one search-results page and return document metadata.

        Returns a list of dicts:
            {title, summary, metadata_url, situation}

        On failed fetch, logs an error and returns [].
        """
        soup = await self.request_service.get_soup(url)

        if not soup:
            logger.error(f"Could not fetch listing page: {url}")
            await self._save_doc_error(
                title=url,
                error_message="Could not fetch listing page",
            )
            return []

        soup = cast(BeautifulSoup, soup)
        documents_html_links_info = self._parse_listing_page_documents(soup)

        if not documents_html_links_info:
            logger.debug(f"No documents found for url: {url}")

        return documents_html_links_info

    async def _get_export_docs_links(self, url: str) -> list[dict]:
        """Fetch the export page and return document metadata."""
        soup = await self.request_service.get_soup(url)

        if not soup:
            logger.warning(f"Could not fetch export page: {url}")
            return []

        soup = cast(BeautifulSoup, soup)
        documents_metadata = self._parse_export_documents(soup)
        if not documents_metadata:
            logger.debug(f"No documents found in export page: {url}")

        return documents_metadata

    async def _get_document_text_link(
        self,
        doc: dict,
        year: int,
        norm_type: str,
    ) -> dict | None:
        """Resolve the actual text URL from a document's metadata page.

        Priority: "texto - republicação" > "texto - publicação original" > "texto -" > "texto"
        """
        title = doc.get("title", "")
        summary = doc.get("summary", "")
        situation = doc.get("situation", "")
        metadata_url = doc.get("metadata_url", "")

        cached_text_url = self._metadata_to_text_url.get(metadata_url)
        if cached_text_url:
            return {
                "title": title,
                "_norm_type": norm_type,
                "summary": summary,
                "situation": situation,
                "metadata_url": metadata_url,
                "document_url": cached_text_url,
            }

        soup = await self.request_service.get_soup(metadata_url)
        if not soup:
            reason = soup.reason
            status = soup.status
            detail = f"HTTP {status} — {reason}" if status else reason
            logger.error(
                f"Could not fetch metadata page: {title} | {detail} | {metadata_url}"
            )
            await self._save_doc_error(
                title=title,
                year=year,
                situation=situation,
                norm_type=norm_type,
                html_link=metadata_url,
                error_message=f"Could not fetch page HTML: {detail}",
            )
            return None

        soup = cast(BeautifulSoup, soup)
        not_found = any(
            heading.get_text(" ", strip=True) == "Not Found"
            for heading in soup.find_all("h1")
        )
        if not_found:
            logger.warning(f"Document not found: {title}")
            await self._save_doc_error(
                title=title,
                year=year,
                situation=situation,
                norm_type=norm_type,
                html_link=metadata_url,
                error_message="Document text not found (404)",
            )
            return None

        sessao_divs = soup.find_all("div", class_="sessao")
        if not sessao_divs:
            logger.error(f"Could not find text link for document: {title}")
            await self._save_doc_error(
                title=title,
                year=year,
                situation=situation,
                norm_type=norm_type,
                html_link=metadata_url,
                error_message="Could not find text link div.sessao in page",
            )
            return None

        original_links = []
        repub_links = []
        other_links = []

        for sessao_div in sessao_divs:
            for link in sessao_div.find_all("a", href=True):
                link_text = link.get_text(" ", strip=True).lower()
                if "texto - publicação original" in link_text:
                    original_links.append(link)
                elif "texto - republicação" in link_text:
                    repub_links.append(link)
                elif "texto -" in link_text or "texto" in link_text:
                    other_links.append(link)

        # Priority: republicação > publicação original > any "texto" link
        chosen = None
        if repub_links:
            chosen = repub_links[-1]["href"]
        elif original_links:
            chosen = original_links[-1]["href"]
        elif other_links:
            chosen = other_links[-1]["href"]

        if chosen is None:
            logger.error(f"Could not find text link for document: {title}")
            await self._save_doc_error(
                title=title,
                year=year,
                situation=situation,
                norm_type=norm_type,
                html_link=metadata_url,
                error_message="No text link found in page anchors",
            )
            return None

        text_url = urljoin(metadata_url, chosen)
        self._metadata_to_text_url[metadata_url] = text_url
        return {
            "title": title,
            "_norm_type": norm_type,
            "summary": summary,
            "situation": situation,
            "metadata_url": metadata_url,
            "document_url": text_url,
        }

    async def _get_doc_data(
        self, doc: dict, year: int, norm_type: str
    ) -> ScrapedDocument | None:
        """Fetch and convert document text to markdown.

        ``doc`` is a dict with keys: title, summary, metadata_url, document_url, situation.

        Returns:
             {title, summary, situation, text_markdown, document_url,
              _raw_content, _content_extension}
        """
        title = doc.get("title", "")
        summary = doc.get("summary", "")
        metadata_url = doc.get("metadata_url", "")
        document_text_link = doc.get("document_url", "")
        situation = doc.get("situation", "")
        doc_type = norm_type or self._resolve_norm_type(
            title,
            metadata_url=metadata_url,
            document_url=document_text_link,
        )

        try:
            soup, mhtml = await self._fetch_soup_and_mhtml(document_text_link)
            soup = cast(BeautifulSoup, soup)
            # Extract main content area; fall back progressively
            content_div = cast(
                BeautifulSoup | Tag,
                soup.find("div", id="content")
                or soup.find("div", class_="textoNorma")
                or soup,
            )

            # Remove portal chrome and boilerplate
            strip_html_chrome(
                content_div,
                extra_selectors=[
                    {"class_": "vejaTambem"},
                    {"class_": "documentFirstHeading"},
                    {"class_": "rodapeTexto"},
                    {"class_": "publicacoesTI"},
                ],
            )
            self._clean_norm_soup(content_div, remove_images=False)

            # Strip elements already captured in dedicated fields:
            #   <h1>  → duplicates `title`
            #   <p class="ementa"> → duplicates `summary`
            for h1 in content_div.find_all("h1"):
                h1.decompose()

            ementa_nodes = content_div.find_all("p", class_="ementa")
            html_with_ementa = content_div.prettify()

            for ementa in ementa_nodes:
                ementa.decompose()

            html_string = content_div.prettify()

            text_markdown = await self._get_markdown(html_content=html_string)

            if not text_markdown or not text_markdown.strip():
                if ementa_nodes:
                    fallback_markdown = await self._get_markdown(
                        html_content=html_with_ementa
                    )
                    if fallback_markdown and fallback_markdown.strip():
                        text_markdown = fallback_markdown
                        html_string = html_with_ementa

                if not text_markdown or not text_markdown.strip():
                    logger.warning(f"Document text is empty after conversion: {title}")
                    await self._save_doc_error(
                        title=title,
                        year=year,
                        situation=situation,
                        norm_type=doc_type,
                        html_link=document_text_link,
                        metadata_url=metadata_url,
                        error_message="Document text is empty after conversion",
                    )
                    return None

            from src.scraper.base.schemas import ScrapedDocument

            return ScrapedDocument(
                year=year,
                title=title,
                type=doc_type,
                summary=summary,
                situation=situation,
                metadata_url=metadata_url,
                text_markdown=text_markdown.strip(),
                document_url=document_text_link,
                raw_content=mhtml,
                content_extension=".mhtml",
            )
        except Exception as e:
            logger.error(f"Error converting document to markdown: {title} - {e}")
            await self._save_doc_error(
                title=title,
                year=year,
                situation=situation,
                norm_type=doc_type,
                html_link=document_text_link,
                metadata_url=metadata_url,
                error_message=str(e),
            )
            return None

    def _infer_norm_type(self, title: str) -> str:
        """Infer norm type from metadata/document URL slug or title prefix."""
        normalized_title = " ".join(title.split())

        for prefix, canonical in sorted(
            _FEDERAL_TITLE_PREFIX_ALIASES.items(),
            key=lambda item: len(item[0]),
            reverse=True,
        ):
            if normalized_title.casefold().startswith(prefix.casefold()):
                return canonical

        inferred = infer_type_from_title(normalized_title, TYPES)
        if inferred:
            return inferred

        prefix_match = _FEDERAL_TYPE_PREFIX_RE.match(normalized_title)
        if prefix_match:
            return prefix_match.group(1).strip()

        return "Desconhecido"

    @staticmethod
    def _extract_type_slug(*urls: str) -> str:
        """Extract the federal norm-type slug from a metadata/document URL."""
        for url in urls:
            if not url:
                continue
            parts = [part for part in urlparse(url).path.split("/") if part]
            if len(parts) >= 3 and parts[0] == "legin":
                return parts[2]
        return ""

    def _resolve_norm_type(
        self,
        title: str,
        *,
        metadata_url: str = "",
        document_url: str = "",
    ) -> str:
        """Resolve the most reliable norm type from URL slug first, title second."""
        slug = self._extract_type_slug(metadata_url, document_url)
        if slug:
            mapped = _FEDERAL_TYPE_SLUG_MAP.get(slug)
            if mapped:
                return mapped
        return self._infer_norm_type(title)

    async def _scrape_type(
        self,
        norm_type: str,
        norm_type_id: str,
        year: int,
    ) -> list:
        """Scrape all documents for the given year.

        When ``norm_type`` is empty, all types are fetched in a single query
        and the type is inferred from each document's title.

        Phase 1: fetch all listing pages (concurrent) → list of
                 {title, summary, metadata_url, situation}
        Phase 2: resolve text URLs (concurrent) → list of
                 {title, summary, situation, metadata_url, document_url}
        Phase 3: fetch + convert content, save (via _process_documents)
        """
        all_types = not norm_type
        label = f"Year {year}" if all_types else norm_type
        results = []

        url = self._format_search_url(str(year), norm_type_id)
        per_page = 20

        soup = await self.request_service.get_soup(url)
        if not soup:
            logger.warning(f"Could not get soup for url: {url}")
            return results

        soup = cast(BeautifulSoup, soup)
        total = self._extract_total_results(soup)
        if total is None:
            logger.warning(f"Could not find total element for url: {url}")
            return results

        if total == 0:
            return results

        pages = calc_pages(total, per_page)

        # --- Phase 1: collect all listing-page metadata ---
        documents_html_links_info = []
        used_export = False

        if total <= self.export_max_docs:
            export_url = self._format_export_url(str(year), norm_type_id)
            export_docs = await self._get_export_docs_links(export_url)
            if len(export_docs) == total:
                documents_html_links_info = export_docs
                used_export = True
            else:
                logger.warning(
                    f"Export count mismatch for {label}: "
                    f"expected {total}, got {len(export_docs)}; falling back to pagination"
                )

        if not used_export:
            documents_html_links_info = self._parse_listing_page_documents(soup)
            page_tasks = [
                self._get_docs_links(url + f"&pagina={page}")
                for page in range(2, pages + 1)
            ]
            page_results = await self._gather_results(
                page_tasks,
                context={"year": year, "type": label, "situation": ""},
                desc=f"{self.name} | {label} | get_html_links",
            )
            documents_html_links_info.extend(flatten_results(page_results))

        # --- Phase 2: resolve text URLs ---
        # Infer norm_type per document when scraping all types at once.
        if all_types:
            for doc in documents_html_links_info:
                if doc is not None:
                    doc["_norm_type"] = self._resolve_norm_type(
                        doc.get("title", ""),
                        metadata_url=doc.get("metadata_url", ""),
                    )

        resolved_docs = []
        text_link_tasks = []
        for doc in documents_html_links_info:
            if doc is None:
                continue
            doc_type = doc.get("_norm_type", norm_type) if all_types else norm_type
            meta_url = doc.get("metadata_url", "")
            if meta_url in self._metadata_to_text_url:
                resolved_docs.append(
                    {**doc, "document_url": self._metadata_to_text_url[meta_url]}
                )
            else:
                text_link_tasks.append(
                    self._get_document_text_link(doc, year, doc_type)
                )

        text_link_results = await self._gather_results(
            text_link_tasks,
            context={"year": year, "type": label, "situation": ""},
            desc=f"{self.name} | {label} | get_text_links",
        )
        resolved_docs.extend(text_link_results)

        # Filter already-scraped documents before Phase 3.
        documents_to_fetch = [
            doc
            for doc in resolved_docs
            if doc is not None
            and not self._is_already_scraped(
                doc.get("document_url", ""), doc.get("title", "")
            )
        ]

        # --- Phase 3: fetch content, convert to markdown, save ---
        if all_types:
            results = await self._process_documents(
                documents_to_fetch,
                year=year,
                norm_type="",
                situation="",  # each doc carries its own situation field
                desc=f"{self.name} | {label} | get_doc_data",
                doc_data_fn=lambda doc: self._get_doc_data(
                    doc, year=year, norm_type=doc.get("_norm_type", "")
                ),
            )
        else:
            results = await self._process_documents(
                documents_to_fetch,
                year=year,
                norm_type=norm_type,
                situation="",  # each doc carries its own situation field
                desc=f"{self.name} | {label} | get_doc_data",
                doc_data_fn=lambda doc: self._get_doc_data(
                    doc, year=year, norm_type=norm_type
                ),
            )

        logger.debug(
            f"Finished scraping | Year: {year} | Type: {label} "
            f"| Results: {len(results)}"
        )

        return results

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape all documents for a year in a single query (tipo= empty).

        The type is inferred from each document's title via
        ``_infer_norm_type``, eliminating the overhead of 35 per-type queries
        and the need for cross-type URL deduplication.
        """
        return await self._scrape_type("", "", year)
