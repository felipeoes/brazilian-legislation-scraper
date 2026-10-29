import re
from collections.abc import AsyncIterator
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from loguru import logger

from src.core.convert import (
    calc_pages,
    clean_norm_soup,
    infer_type_from_title,
    strip_html_chrome,
)
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

VALID_SITUATIONS = [
    "Não consta revogação expressa",
    "Não Informado",
    "Convertida em Lei",
    "Reeditada",
    "Reeditada com alteração",
]
INVALID_SITUATIONS = ["Arquivada", "Rejeitada", "Revogada", "Sem Eficácia"]
COVERAGE = ""
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
    "(.+?)(?:\\s+(?:n[º°o]|n\\.|número)\\s*[\\dA-Za-z]|\\s+\\d|\\s+de\\s)",
    re.IGNORECASE,
)
ORDERING = "data%3AASC"
YEAR_START = 1808
EXPORT_MAX_DOCS = 300


class CamaraDepScraper(BaseScraper):
    "Scraper for the Câmara dos Deputados legislation portal."

    name = "LEGISLACAO_FEDERAL"
    coverage = ""
    ordering = "data%3AASC"
    types = TYPES
    situations = {}
    base_url = "https://www.camara.leg.br/legislacao/"

    def _format_search_url(self, year: str, norm_type_id: str) -> str:
        """Format search URL for a given year and norm type (URL-encoded value)."""
        params = {
            "abrangencia": self.coverage,
            "geral": "",
            "ano": year,
            "situacao": "",
            "origem": "",
            "numero": "",
            "ordenacao": self.ordering,
            "tipo": norm_type_id,
        }
        return (
            self.base_url
            + "busca?"
            + "&".join((f"{key}={value}" for key, value in params.items()))
        )

    @staticmethod
    def _extract_total_results(soup: BeautifulSoup) -> int | None:
        total_element = soup.find(
            "div", class_="busca-info__resultado busca-info__resultado--informado"
        )
        if total_element is None:
            return None
        try:
            return int(total_element.get_text(" ", strip=True).split()[-1])
        except TypeError, ValueError, IndexError:
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
        soup = (await self.http.request(url)).soup()
        if not soup:
            logger.error(f"Could not fetch listing page: {url}")
            raise ValueError("Could not fetch listing page")
        documents_html_links_info = self._parse_listing_page_documents(soup)
        if not documents_html_links_info:
            logger.debug(f"No documents found for url: {url}")
        return documents_html_links_info

    async def _get_document_text_link(
        self, doc: dict, year: int, norm_type: str
    ) -> dict | None:
        """Resolve the actual text URL from a document's metadata page.

        Priority: "texto - republicação" > "texto - publicação original"
                  > "texto - emenda" > other "texto - *" > bare "texto"
        Within each bucket, HTML links are preferred over PDFs.
        """
        title = doc.get("title", "")
        summary = doc.get("summary", "")
        situation = doc.get("situation", "") or "Não Informado"
        metadata_url = doc.get("metadata_url", "")
        soup = (await self.http.request(metadata_url)).soup()
        if not soup:
            reason = soup.reason
            status = soup.status
            detail = f"HTTP {status} — {reason}" if status else reason
            logger.error(
                f"Could not fetch metadata page: {title} | {detail} | {metadata_url}"
            )
            raise ValueError(f"Could not fetch page HTML: {detail}")
        not_found = any(
            (
                heading.get_text(" ", strip=True) == "Not Found"
                for heading in soup.find_all("h1")
            )
        )
        if not_found:
            logger.warning(f"Document not found: {title}")
            raise ValueError("Document text not found (404)")
        sessao_divs = soup.find_all("div", class_="sessao")
        if not sessao_divs:
            logger.error(f"Could not find text link for document: {title}")
            raise ValueError("Could not find text link div.sessao in page")
        original_links = []
        repub_links = []
        emenda_links = []
        dash_links = []
        bare_links = []
        for sessao_div in sessao_divs:
            for link in sessao_div.find_all("a", href=True):
                link_text = link.get_text(" ", strip=True).lower()
                if "texto - publicação original" in link_text:
                    original_links.append(link)
                elif "texto - republicação" in link_text:
                    repub_links.append(link)
                elif "texto - emenda" in link_text:
                    emenda_links.append(link)
                elif "texto -" in link_text:
                    dash_links.append(link)
                elif "texto" in link_text:
                    bare_links.append(link)

        def _prefer_html(links: list) -> str | None:
            html = [
                lnk["href"] for lnk in links if lnk["href"].lower().endswith(".html")
            ]
            fallback = [lnk["href"] for lnk in links]
            candidates = html or fallback
            return candidates[-1] if candidates else None

        chosen = None
        if repub_links:
            chosen = _prefer_html(repub_links)
        elif original_links:
            chosen = _prefer_html(original_links)
        elif emenda_links:
            chosen = _prefer_html(emenda_links)
        elif dash_links:
            chosen = _prefer_html(dash_links)
        elif bare_links:
            chosen = _prefer_html(bare_links)
        if chosen is None:
            logger.error(f"Could not find text link for document: {title}")
            raise ValueError("No text link found in page anchors")
        text_url = urljoin(metadata_url, chosen)
        return {
            "title": title,
            "_norm_type": norm_type,
            "summary": summary,
            "situation": situation,
            "metadata_url": metadata_url,
            "document_url": text_url,
        }

    async def fetch(self, doc: dict) -> Document | None:
        year = doc["year"]
        norm_type = doc["type"]
        doc = {**doc, **await self._get_document_text_link(doc, year, norm_type)}
        title = doc.get("title", "")
        summary = doc.get("summary", "")
        metadata_url = doc.get("metadata_url", "")
        document_text_link = doc.get("document_url", "")
        situation = doc.get("situation", "") or "Não Informado"
        doc_type = norm_type or self._resolve_norm_type(
            title, metadata_url=metadata_url, document_url=document_text_link
        )
        try:
            resp_or_failed = await self.http.request(document_text_link)
            html_bytes = resp_or_failed.content
            if html_bytes.startswith(b"%PDF"):
                return await self.fetch_pdf({**doc, "year": year, "type": doc_type})
            html_text = resp_or_failed.text
            soup = BeautifulSoup(html_text, "lxml")
            content_div = (
                soup.find("div", id="content")
                or soup.find("div", class_="textoNorma")
                or soup
            )
            strip_html_chrome(
                content_div,
                extra_selectors=[
                    {"class_": "vejaTambem"},
                    {"class_": "documentFirstHeading"},
                    {"class_": "rodapeTexto"},
                    {"class_": "publicacoesTI"},
                ],
            )
            clean_norm_soup(content_div)
            for h1 in content_div.find_all("h1"):
                h1.decompose()
            ementa_nodes = content_div.find_all("p", class_="ementa")
            html_with_ementa = str(content_div)
            for ementa in ementa_nodes:
                ementa.decompose()
            html_string = str(content_div)
            text_markdown = await self.markdown(html=html_string)
            if not text_markdown or not text_markdown.strip():
                if ementa_nodes:
                    fallback_markdown = await self.markdown(html=html_with_ementa)
                    if fallback_markdown and fallback_markdown.strip():
                        text_markdown = fallback_markdown
                        html_string = html_with_ementa
                if not text_markdown or not text_markdown.strip():
                    logger.warning(f"Document text is empty after conversion: {title}")
                    raise ValueError("Document text is empty after conversion")
            from src.core.document import Document

            return Document(
                year=year,
                title=title,
                type=doc_type,
                summary=summary,
                situation=situation,
                metadata_url=metadata_url,
                text_markdown=text_markdown.strip(),
                document_url=document_text_link,
                raw=html_bytes,
                raw_ext=".html",
            )
        except Exception as e:
            logger.error(f"Error converting document to markdown: {title} - {e}")
            raise ValueError(str(e))

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
        self, title: str, *, metadata_url: str = "", document_url: str = ""
    ) -> str:
        """Resolve the most reliable norm type from URL slug first, title second."""
        slug = self._extract_type_slug(metadata_url, document_url)
        if slug:
            mapped = _FEDERAL_TYPE_SLUG_MAP.get(slug)
            if mapped:
                return mapped
        return self._infer_norm_type(title)

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        url = self._format_search_url(str(year), "")
        soup = (await self.http.request(url)).soup()
        total = self._extract_total_results(soup)
        if total is None:
            raise ValueError("Could not parse federal legislation result count")

        async def fetch_page(page: int) -> list[dict]:
            return (
                self._parse_listing_page_documents(soup)
                if page == 1
                else await self._get_docs_links(url + f"&pagina={page}")
            )

        async for ref in self.paginate(fetch_page, calc_pages(total, 20)):
            norm_type = self._resolve_norm_type(
                ref["title"], metadata_url=ref["metadata_url"]
            )
            yield {
                **ref,
                "year": year,
                "type": norm_type,
                "document_url": ref["metadata_url"],
                "situation": ref.get("situation") or DEFAULT_VALID_SITUATION,
            }
