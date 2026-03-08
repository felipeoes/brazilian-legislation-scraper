from urllib.parse import unquote, urljoin
from typing import cast

from bs4 import BeautifulSoup, Tag
from loguru import logger
from src.scraper.base.scraper import BaseScraper

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

# Kept for reference / downstream consumers.
SITUATIONS = {s: s for s in VALID_SITUATIONS + INVALID_SITUATIONS}

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

ORDERING = "data%3AASC"
YEAR_START = 1808
EXPORT_MAX_DOCS = 300


class CamaraDepScraper(BaseScraper):
    """Webscraper for Camara dos Deputados website (https://www.camara.leg.br/legislacao/)

    Year start (earliest on source): 1808

    Situation is NOT used as a search filter — it is read directly from each
    listing-page result item (p.busca-resultados__situacao).

    Example search request url:
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
        if not self.saver:
            return

        year_docs = await self.saver.get_year_documents(year)
        for doc in year_docs:
            metadata_url = str(doc.get("metadata_url", "") or "")
            document_url = str(doc.get("document_url", "") or "")
            if metadata_url and document_url:
                self._metadata_to_text_url[metadata_url] = document_url

        if self.verbose and self._metadata_to_text_url:
            logger.info(
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

        if not documents_html_links_info and self.verbose:
            logger.info(f"No documents found for url: {url}")

        return documents_html_links_info

    async def _get_export_docs_links(self, url: str) -> list[dict]:
        """Fetch the export page and return document metadata."""
        soup = await self.request_service.get_soup(url)

        if not soup:
            logger.warning(f"Could not fetch export page: {url}")
            return []

        soup = cast(BeautifulSoup, soup)
        documents_metadata = self._parse_export_documents(soup)
        if not documents_metadata and self.verbose:
            logger.info(f"No documents found in export page: {url}")

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
                "summary": summary,
                "situation": situation,
                "metadata_url": metadata_url,
                "document_url": cached_text_url,
            }

        soup = await self.request_service.get_soup(metadata_url)
        if not soup:
            reason = getattr(soup, "reason", "unknown")
            status = getattr(soup, "status", None)
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
            "summary": summary,
            "situation": situation,
            "metadata_url": metadata_url,
            "document_url": text_url,
        }

    async def _get_doc_data(self, doc: dict, year: int, norm_type: str) -> dict | None:
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

        try:
            soup = await self.request_service.get_soup(document_text_link)
            if not soup:
                logger.warning(f"Could not fetch document page: {title}")
                await self._save_doc_error(
                    title=title,
                    year=year,
                    situation=situation,
                    norm_type=norm_type,
                    html_link=document_text_link,
                    metadata_url=metadata_url,
                    error_message="Could not fetch document page",
                )
                return None

            soup = cast(BeautifulSoup, soup)
            # Extract main content area; fall back progressively
            content_div = cast(
                BeautifulSoup | Tag,
                soup.find("div", id="content")
                or soup.find("div", class_="textoNorma")
                or soup,
            )

            # Remove portal chrome and boilerplate
            self._strip_html_chrome(
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
                        norm_type=norm_type,
                        html_link=document_text_link,
                        metadata_url=metadata_url,
                        error_message="Document text is empty after conversion",
                    )
                    return None

            return {
                "title": title,
                "summary": summary,
                "situation": situation,
                "metadata_url": metadata_url,
                "text_markdown": text_markdown.strip(),
                "document_url": document_text_link,
                "_raw_content": html_string.encode("utf-8"),
                "_content_extension": ".html",
            }
        except Exception as e:
            logger.error(f"Error converting document to markdown: {title} - {e}")
            await self._save_doc_error(
                title=title,
                year=year,
                situation=situation,
                norm_type=norm_type,
                html_link=document_text_link,
                metadata_url=metadata_url,
                error_message=str(e),
            )
            return None

    async def _scrape_type(
        self,
        norm_type: str,
        norm_type_id,
        year: int,
        seen_urls: set[str] | None = None,
    ) -> list:
        """Scrape all documents of a single norm type for the given year.

        Phase 1: fetch all listing pages (concurrent) → list of
                 {title, summary, metadata_url, situation}
        Phase 2: resolve text URLs (concurrent) → list of
                 {title, summary, situation, metadata_url, document_url}
        Phase 3: fetch + convert content, save (via _process_documents)

        ``seen_urls`` is a shared set of already-processed text URLs (by
        reference) used to deduplicate across norm types within the same year.
        Documents whose resolved text URL is already in ``seen_urls`` are
        silently skipped before Phase 3, preventing duplicate content when
        the same document appears under multiple ``tipo`` query values.
        """
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

        pages = self._calc_pages(total, per_page)

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
                    f"Export count mismatch for {norm_type} {year}: "
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
                context={"year": year, "type": norm_type, "situation": ""},
                desc=f"{self.name} | {norm_type} | get_html_links",
            )
            documents_html_links_info.extend(self._flatten_results(page_results))

        # --- Phase 2: resolve text URLs ---
        resolved_docs = []
        text_link_tasks = [
            self._get_document_text_link(doc, year, norm_type)
            for doc in documents_html_links_info
            if doc is not None
            and doc.get("metadata_url", "") not in self._metadata_to_text_url
        ]
        resolved_docs.extend(
            {
                **doc,
                "document_url": self._metadata_to_text_url[doc.get("metadata_url", "")],
            }
            for doc in documents_html_links_info
            if doc is not None
            and doc.get("metadata_url", "") in self._metadata_to_text_url
        )
        text_link_results = await self._gather_results(
            text_link_tasks,
            context={"year": year, "type": norm_type, "situation": ""},
            desc=f"{self.name} | {norm_type} | get_text_links",
        )
        resolved_docs.extend(text_link_results)

        # Filter already-scraped documents and cross-type duplicates before Phase 3.
        # seen_urls is mutated in-place so subsequent _scrape_type calls for the
        # same year skip URLs that were already processed by an earlier type.
        documents_to_fetch = []
        for doc in resolved_docs:
            if doc is None:
                continue
            text_url = doc.get("document_url", "")
            if self._is_already_scraped(text_url, doc.get("title", "")):
                continue
            if seen_urls is not None:
                if text_url in seen_urls:
                    if self.verbose:
                        logger.debug(f"Skipping duplicate URL across types: {text_url}")
                    continue
                seen_urls.add(text_url)
            documents_to_fetch.append(doc)

        # --- Phase 3: fetch content, convert to markdown, save ---
        results = await self._process_documents(
            documents_to_fetch,
            year=year,
            norm_type=norm_type,
            situation="",  # each doc carries its own situation field
            desc=f"{self.name} | {norm_type} | get_doc_data",
            doc_data_fn=lambda doc: self._get_doc_data(
                doc, year=year, norm_type=norm_type
            ),
        )

        if self.verbose:
            logger.info(
                f"Finished scraping | Year: {year} | Type: {norm_type} "
                f"| Results: {len(results)}"
            )

        return results

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape all norm types for a year with cross-type URL deduplication.

        Overrides the base implementation to pass a shared ``seen_urls`` set
        into every ``_scrape_type`` call.  Because types are scraped
        concurrently, the set is populated as each Phase-2 result is resolved;
        any text URL that was already claimed by a concurrent or earlier type
        is silently skipped before Phase 3, so no document is fetched or saved
        twice even when the same norm appears under multiple ``tipo`` queries.
        """
        seen_urls: set[str] = set()
        type_items = cast(dict[str, str], self.types)
        tasks = [
            self._scrape_type(nt, nt_id, year, seen_urls=seen_urls)
            for nt, nt_id in type_items.items()
        ]
        valid = await self._gather_results(
            tasks,
            context={"year": year, "type": "NA", "situation": "NA"},
            desc=f"{self.name} | Year {year}",
        )
        return self._flatten_results(valid)
