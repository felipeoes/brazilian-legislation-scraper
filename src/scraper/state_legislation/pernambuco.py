from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument
import re
from dataclasses import dataclass
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.scraper.base.converter import valid_markdown, wrap_html
from src.scraper.base.scraper import (
    DEFAULT_INVALID_SITUATION,
    DEFAULT_VALID_SITUATION,
    StateScraper,
)
from src.services.request.service import FailedRequest


TYPES = {
    "Ato Administrativo Normativo": 0,
    "Ato Administrativo Parlamentar": 1,
    "Constituição Estadual": 2,
    "Decreto do Executivo": 3,
    "Decreto Legislativo": 4,
    "Decreto-Lei": 5,
    "Emenda Constitucional": 6,
    "Lei Complementar": 7,
    "Lei Delegada": 8,
    "Lei Ordinária": 9,
    "Lei Provincial": 10,
    "Portaria Administrativa da Alepe": 11,
    "Resolução Conjunta": 12,
    "Resolução da Alepe": 13,
    "Resolução do Poder Judiciário": 14,
}

# ALEPE does not expose an explicit validity filter on the search form.
SITUATIONS: dict[str, str] = {}

_SEARCH_PATH = "/Paginas/pesquisaAvancada.aspx"
_TEXT_VERSION_PRIORITY = ("TEXTOATUALIZADO", "TEXTOORIGINAL", "TEXTOANOTADO", "")
_TYPE_ALIASES = {
    "ato administrativo normativo": "Ato Administrativo Normativo",
    "ato administrativo parlamentar": "Ato Administrativo Parlamentar",
    "constituição estadual": "Constituição Estadual",
    "constituição do estado": "Constituição Estadual",
    "decreto do executivo": "Decreto do Executivo",
    "decreto legislativo": "Decreto Legislativo",
    "decreto-lei": "Decreto-Lei",
    "emenda constitucional": "Emenda Constitucional",
    "lei complementar": "Lei Complementar",
    "lei delegada": "Lei Delegada",
    "lei ordinária": "Lei Ordinária",
    "lei provincial": "Lei Provincial",
    "portaria administrativa da alepe": "Portaria Administrativa da Alepe",
    "resolução conjunta": "Resolução Conjunta",
    "resolução da alepe": "Resolução da Alepe",
    "resolução do poder judiciário": "Resolução do Poder Judiciário",
}


@dataclass(slots=True)
class PagerSlot:
    slot_id: str
    page_number: int
    is_active: bool = False


class PernambucoAlepeScraper(StateScraper):
    """Scraper for Assembleia Legislativa de Pernambuco (ALEPE).

    Year start (earliest on source): 1835

    The search form is an ASP.NET postback page. The fastest reliable flow is to
    search by year only, then infer the norm type from the result row title.
    ALEPE also exposes multiple text versions per document; this scraper prefers
    ``TEXTOATUALIZADO`` and falls back to ``TEXTOORIGINAL`` when needed.
    """

    def __init__(
        self,
        base_url: str = "https://legis.alepe.pe.gov.br",
        **kwargs,
    ):
        super().__init__(
            base_url,
            types=TYPES,
            situations=SITUATIONS,
            name="PERNAMBUCO",
            **kwargs,
        )
        self.search_url = urljoin(self.base_url, _SEARCH_PATH)
        self.params = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": "",
            "__VIEWSTATEGENERATOR": "",
            "__EVENTVALIDATION": "",
            "ctl00$hfUrl": self.search_url,
            "ctl00$tbxLogin": "",
            "ctl00$tbxSenha": "",
            "ctl00$conteudo$tbxNumero": "",
            "ctl00$conteudo$tbxAno": "",
            "ctl00$conteudo$tbxTextoPesquisa": "",
            "ctl00$conteudo$tbxTextoPesquisaNeg": "",
            "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_0": "CONTTXTORIGINAL",
            "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_1": "EMENTA",
            "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_2": "APELIDO",
            "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_3": "NOME",
            "ctl00$conteudo$tbxThesaurus": "",
            "ctl00$conteudo$rbOpThesaurus": "Todos",
            "ctl00$conteudo$tbxAssuntoGeral": "",
            "ctl00$conteudo$rbOpAssuntoGeral": "Todos",
            "ctl00$conteudo$tbxDataInicialNorma": "",
            "ctl00$conteudo$tbxDataFinalNorma": "",
            "ctl00$conteudo$ddlPublicacao": "",
            "ctl00$conteudo$tbxDataInicialPublicacao": "",
            "ctl00$conteudo$tbxDataFinalPublicacao": "",
            "ctl00$conteudo$tbxIniciativa": "",
            "ctl00$conteudo$tbxNumeroProjeto": "",
            "ctl00$conteudo$tbxAnoProjeto": "",
            "ctl00$conteudo$btnPesquisar": "Pesquisar",
            "ctl00$tbxNomeErro": "",
            "ctl00$tbxEmailErro": "",
            "ctl00$tbxMensagemErro": "",
            "ctl00$tbxLoginMob": "",
            "ctl00$tbxSenhaMob": "",
        }
        self._known_types = tuple(sorted(TYPES.keys(), key=len, reverse=True))

    @staticmethod
    def _attr_str(tag: Tag, attr_name: str) -> str:
        value = tag.get(attr_name, "")
        return value if isinstance(value, str) else ""

    def _get_form_state(self, soup: BeautifulSoup) -> dict[str, str]:
        """Extract the ASP.NET form state from a page."""
        state: dict[str, str] = {}
        for field_name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
            input_tag = soup.find("input", {"name": field_name})
            if isinstance(input_tag, Tag):
                state[field_name] = self._attr_str(input_tag, "value")
        return state

    def _build_search_payload(
        self,
        year: int,
        form_state: dict[str, str],
    ) -> dict[str, str]:
        payload = self.params.copy()
        payload.update(form_state)
        payload["ctl00$conteudo$tbxAno"] = str(year)
        return payload

    def _build_postback_payload(
        self,
        base_payload: dict[str, str],
        source_soup: BeautifulSoup,
        control_id: str,
    ) -> dict[str, str]:
        payload = base_payload.copy()
        payload.update(self._get_form_state(source_soup))
        payload["__EVENTTARGET"] = (
            control_id
            if control_id.startswith("ctl00$")
            else f"ctl00$conteudo${control_id}"
        )
        payload["__EVENTARGUMENT"] = ""
        payload["__LASTFOCUS"] = ""
        payload["ctl00$conteudo$hfPage"] = "0"
        payload.pop("ctl00$conteudo$btnPesquisar", None)
        return payload

    async def _post_results_page(self, payload: dict[str, str]) -> BeautifulSoup | None:
        response = await self.request_service.make_request(
            self.search_url,
            method="POST",
            payload=payload,
            timeout=60,
        )
        if not response:
            logger.warning(
                f"PERNAMBUCO | Search POST failed: {getattr(response, 'reason', 'unknown error')}"
            )
            return None
        client_response = response
        assert isinstance(client_response, aiohttp.ClientResponse)
        html = await client_response.text(errors="replace")
        return BeautifulSoup(html, "html.parser")

    @staticmethod
    def _extract_text_version(url: str) -> str:
        upper_url = url.upper()
        for version in ("TEXTOATUALIZADO", "TEXTOORIGINAL", "TEXTOANOTADO"):
            if f"TIPO={version}" in upper_url:
                return version
        return ""

    def _get_candidate_document_urls(self, row: Tag) -> list[str]:
        urls_by_version: dict[str, str] = {}

        title_link = row.select_one("span.nome-norma a[href]")
        if isinstance(title_link, Tag):
            href = self._attr_str(title_link, "href")
            if href:
                urls_by_version[""] = urljoin(self.base_url, href)

        for link in row.select("td.textos a[href], td#textos a[href]"):
            if not isinstance(link, Tag):
                continue
            href = self._attr_str(link, "href")
            if not href:
                continue
            abs_url = urljoin(self.base_url, href)
            version = self._extract_text_version(abs_url)
            urls_by_version[version] = abs_url

        ordered_urls: list[str] = []
        for version in _TEXT_VERSION_PRIORITY:
            candidate_url = urls_by_version.get(version)
            if candidate_url and candidate_url not in ordered_urls:
                ordered_urls.append(candidate_url)
        return ordered_urls

    def _extract_norm_type(self, title: str) -> str:
        normalized_title = " ".join(title.split())
        lowered = normalized_title.casefold()

        for alias, canonical in sorted(
            _TYPE_ALIASES.items(), key=lambda item: len(item[0]), reverse=True
        ):
            if lowered.startswith(alias):
                return canonical

        for norm_type in self._known_types:
            if normalized_title.startswith(norm_type):
                return norm_type

        match = re.match(r"(.+?)\s+n[°ºo]", normalized_title, re.IGNORECASE)
        if match:
            fallback = " ".join(match.group(1).split())
            logger.warning(f"PERNAMBUCO | Unknown norm type fallback: {fallback}")
            return fallback

        match = re.match(r"(.+?)\s+de\s+\d{4}$", normalized_title, re.IGNORECASE)
        if match:
            fallback = " ".join(match.group(1).split())
            logger.warning(f"PERNAMBUCO | Unknown year-title fallback: {fallback}")
            return fallback

        logger.warning(f"PERNAMBUCO | Could not infer norm type from title: {title}")
        return normalized_title

    def _extract_documents(self, soup: BeautifulSoup, year: int) -> list[dict]:
        """Extract document metadata from a result page."""
        div_resultado = soup.find("div", id="divResultado")
        if not isinstance(div_resultado, Tag):
            return []

        tbody = div_resultado.find("tbody")
        if not isinstance(tbody, Tag):
            return []

        documents: list[dict] = []
        for row in tbody.find_all("tr"):
            if not isinstance(row, Tag):
                continue

            title_link = row.select_one("span.nome-norma a[href]")
            if not isinstance(title_link, Tag):
                continue

            title = title_link.get_text(" ", strip=True)
            if not title:
                continue

            candidate_urls = self._get_candidate_document_urls(row)
            if not candidate_urls:
                continue

            summary_div = row.select_one("td.ementa-norma .fLeft")
            additional_data_link = row.select_one(
                "td.ementa-norma a[href*='dadosReferenciais.aspx']"
            )
            publication_span = row.select_one("span.publicacao")

            documents.append(
                {
                    "year": year,
                    "type": self._extract_norm_type(title),
                    "title": title,
                    "summary": (
                        summary_div.get_text(" ", strip=True)
                        if isinstance(summary_div, Tag)
                        else ""
                    ),
                    "publication": (
                        publication_span.get_text(" ", strip=True)
                        if isinstance(publication_span, Tag)
                        else ""
                    ),
                    "document_url": candidate_urls[0],
                    "additional_data_url": (
                        urljoin(
                            self.base_url, self._attr_str(additional_data_link, "href")
                        )
                        if isinstance(additional_data_link, Tag)
                        else ""
                    ),
                    "_candidate_document_urls": candidate_urls,
                }
            )

        return documents

    @staticmethod
    def _parse_pager_slots(soup: BeautifulSoup) -> list[PagerSlot]:
        slots: list[PagerSlot] = []
        for tag in soup.select("a[id^='lbtn'], span[id^='lbtn']"):
            if not isinstance(tag, Tag):
                continue
            slot_id = PernambucoAlepeScraper._attr_str(tag, "id")
            label = tag.get_text(" ", strip=True)
            if not slot_id or not label.isdigit():
                continue
            slots.append(
                PagerSlot(
                    slot_id=slot_id,
                    page_number=int(label),
                    is_active="active" in (tag.get("class") or []),
                )
            )
        return slots

    async def _get_docs_links(self, year: int) -> list[dict]:
        """Fetch all search-result rows for a year-only ALEPE search."""
        initial_soup = await self.request_service.get_soup(self.search_url)
        if not initial_soup:
            logger.warning(
                f"PERNAMBUCO | Failed to retrieve initial search page: {getattr(initial_soup, 'reason', 'unknown error')}"
            )
            return []
        assert isinstance(initial_soup, BeautifulSoup)

        form_state = self._get_form_state(initial_soup)
        if not form_state:
            logger.warning("PERNAMBUCO | Failed to extract search form state")
            return []

        base_payload = self._build_search_payload(year, form_state)
        current_page_soup = await self._post_results_page(base_payload)
        if current_page_soup is None:
            return []

        documents: list[dict] = []
        seen_pages: set[int] = set()

        while True:
            current_slots = self._parse_pager_slots(current_page_soup)
            current_page_number = next(
                (slot.page_number for slot in current_slots if slot.is_active),
                1,
            )

            if current_page_number not in seen_pages:
                page_docs = self._extract_documents(current_page_soup, year)
                documents.extend(page_docs)
                seen_pages.add(current_page_number)
                logger.debug(
                    f"PERNAMBUCO | Year {year} | Page {current_page_number} | Found {len(page_docs)} docs"
                )
            else:
                logger.debug(
                    f"PERNAMBUCO | Year {year} | Skipping duplicate page {current_page_number}"
                )

            for slot in current_slots:
                if (
                    slot.page_number <= current_page_number
                    or slot.page_number in seen_pages
                ):
                    continue

                page_soup = await self._post_results_page(
                    self._build_postback_payload(
                        base_payload,
                        current_page_soup,
                        slot.slot_id,
                    )
                )
                if page_soup is None:
                    continue

                page_docs = self._extract_documents(page_soup, year)
                documents.extend(page_docs)
                seen_pages.add(slot.page_number)
                logger.debug(
                    f"PERNAMBUCO | Year {year} | Page {slot.page_number} | Found {len(page_docs)} docs"
                )

            if current_page_soup.find("a", id="lbtnProx") is None:
                break

            next_block_soup = await self._post_results_page(
                self._build_postback_payload(
                    base_payload,
                    current_page_soup,
                    "lbtnProx",
                )
            )
            if next_block_soup is None:
                break
            current_page_soup = next_block_soup

        return documents

    @staticmethod
    def _extract_reference_value(soup: BeautifulSoup, label: str) -> str:
        for header in soup.find_all("th"):
            if not isinstance(header, Tag):
                continue
            if header.get_text(" ", strip=True) != label:
                continue
            value_cell = header.find_next_sibling("td")
            if isinstance(value_cell, Tag):
                return value_cell.get_text(" ", strip=True)
        return ""

    @staticmethod
    def _infer_situation(
        text_soup: BeautifulSoup | None,
        reference_soup: BeautifulSoup | None,
    ) -> str:
        for soup in (text_soup, reference_soup):
            if isinstance(soup, BeautifulSoup) and soup.find("div", id="divRevogada"):
                return DEFAULT_INVALID_SITUATION
        return DEFAULT_VALID_SITUATION

    def _get_additional_data(
        self,
        reference_soup: BeautifulSoup | None,
        *,
        text_soup: BeautifulSoup | None = None,
    ) -> dict[str, str]:
        data = {
            "situation": self._infer_situation(text_soup, reference_soup),
            "date": "",
            "initiative": "",
            "publication": "",
            "subject": "",
            "updates": "",
            "indexation": "",
        }
        if not isinstance(reference_soup, BeautifulSoup):
            return data

        data.update(
            {
                "date": self._extract_reference_value(reference_soup, "Data"),
                "initiative": self._extract_reference_value(
                    reference_soup, "Iniciativa"
                ),
                "publication": self._extract_reference_value(
                    reference_soup, "Publicação"
                ),
                "subject": self._extract_reference_value(
                    reference_soup, "Assunto Geral"
                ),
                "updates": self._extract_reference_value(
                    reference_soup, "Atualizações"
                ),
                "indexation": self._extract_reference_value(
                    reference_soup, "Indexação"
                ),
            }
        )

        summary = self._extract_reference_value(reference_soup, "Ementa")
        if summary:
            data["summary"] = summary
        return data

    async def _fetch_reference_page(
        self,
        url: str,
        *,
        title: str,
    ) -> BeautifulSoup | None:
        last_failure: FailedRequest | None = None
        for _ in range(2):
            result = await self.request_service.fetch_bytes(url, timeout=60)
            if not result:
                if isinstance(result, FailedRequest):
                    last_failure = result
                continue
            assert not isinstance(result, FailedRequest)
            body, _ = result
            return BeautifulSoup(body, "html.parser")

        if last_failure is not None:
            logger.warning(
                f"PERNAMBUCO | Failed to retrieve additional data for {title}: {last_failure.reason}"
            )
        return None

    async def _get_doc_data(self, doc_info: dict, year: int) -> ScrapedDocument | None:
        """Fetch one ALEPE norm page, prefer updated text, and save metadata."""
        title = doc_info.get("title", "")
        norm_type = doc_info.get("type", "")
        candidate_urls = list(
            dict.fromkeys(
                doc_info.get("_candidate_document_urls")
                or [doc_info.get("document_url", "")]
            )
        )
        candidate_urls = [url for url in candidate_urls if url]

        if any(self._is_already_scraped(url, title) for url in candidate_urls):
            return None

        additional_data_url = doc_info.get("additional_data_url", "")

        selected_url = ""
        selected_soup: BeautifulSoup | None = None
        text_markdown = ""
        last_failure_reason = "Failed to retrieve document page"

        for candidate_url in candidate_urls:
            fetch_result = await self.request_service.fetch_bytes(
                candidate_url, timeout=60
            )
            if not fetch_result:
                last_failure_reason = getattr(
                    fetch_result, "reason", last_failure_reason
                )
                continue

            assert not isinstance(fetch_result, FailedRequest)
            body, _ = fetch_result
            soup = BeautifulSoup(body, "html.parser")
            content_div = soup.find("div", class_="WordSection1")
            if not isinstance(content_div, Tag):
                last_failure_reason = "Could not find WordSection1 in document page"
                continue

            self._clean_norm_soup(
                content_div,
                remove_disclaimers=True,
                unwrap_links=True,
                remove_images=True,
                remove_empty_tags=True,
                strip_styles=True,
            )
            full_html = wrap_html(str(content_div))
            candidate_markdown = await self._get_markdown(html_content=full_html)
            valid, reason = valid_markdown(candidate_markdown)
            if not valid:
                last_failure_reason = f"Invalid markdown: {reason}"
                continue

            selected_url = candidate_url
            selected_soup = soup
            text_markdown = candidate_markdown
            break

        if selected_soup is None or not selected_url:
            await self._save_doc_error(
                title=title,
                year=year,
                norm_type=norm_type,
                html_link=doc_info.get(
                    "document_url", candidate_urls[0] if candidate_urls else ""
                ),
                error_message=last_failure_reason,
            )
            return None

        reference_page: BeautifulSoup | None = None
        if additional_data_url:
            reference_page = await self._fetch_reference_page(
                additional_data_url,
                title=title,
            )

        try:
            mhtml = await self._capture_mhtml(selected_url)
        except Exception as exc:
            logger.warning(f"MHTML capture failed for {selected_url}: {exc}")
            await self._save_doc_error(
                title=title,
                year=year,
                norm_type=norm_type,
                html_link=selected_url,
                error_message=f"MHTML capture failed: {exc}",
            )
            return None

        result = {
            **doc_info,
            "year": year,
            "type": norm_type,
            "document_url": selected_url,
            "text_version": self._extract_text_version(selected_url) or "DEFAULT",
            "text_markdown": text_markdown,
            "raw_content": mhtml,
            "content_extension": ".mhtml",
        }
        result.pop("_candidate_document_urls", None)

        additional_data = self._get_additional_data(
            reference_page, text_soup=selected_soup
        )
        for key, value in additional_data.items():
            if value or key == "situation":
                result[key] = value

        if not result.get("situation"):
            result["situation"] = DEFAULT_VALID_SITUATION

        from src.scraper.base.schemas import ScrapedDocument

        return ScrapedDocument(**result)

    async def _scrape_year(self, year: int) -> list[dict]:
        documents = await self._get_docs_links(year)
        if not documents:
            return []

        return await self._process_documents(
            documents,
            year=year,
            norm_type="NA",
            situation="NA",
            desc=f"{self.name} | {year}",
            doc_data_kwargs={"year": year},
        )

    async def _scrape_type(
        self, norm_type: str, norm_type_id: int, year: int
    ) -> list[dict]:
        """Unused: ALEPE is scraped year-by-year, not year-by-type."""
        return []
