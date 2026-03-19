from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument

import json
import re
from io import BytesIO
from typing import cast
from urllib.parse import urljoin

import aiohttp
import fitz
from bs4 import BeautifulSoup, NavigableString, Tag
from loguru import logger

from src.scraper.base.converter import (
    detect_extension,
    is_pdf,
    valid_markdown,
    wrap_html,
)
from src.scraper.base.scraper import StateScraper

TYPES = {
    "Ato da Mesa Diretora": 17000000,
    "Ato Declaratório": 18000000,
    "Ato Declaratório Interpretativo": "7c5da8af85dd43b8973acaf39043a3d2",
    "Ato do Presidente": "18e34c5d799c445ab47df54cf6f1d2b9",
    "Ato Regimental": 20000000,
    "Decisão": 23000000,
    "Decreto": 27000000,
    "Decreto Executivo": 28000000,
    "Decreto Legislativo": 29000000,
    "Deliberação": "c870f54826864e6889ec08c7f3d9d8c2",
    "Despacho": 31000000,
    "Determinação": "b67f52a2c5a5471299f5ea2cc6c2aad5",
    "Emenda Regimental": 38000000,
    "Estatuto": 39000000,
    "Instrução": 41000000,
    "Instrução de Serviço": 43000000,
    "Instrução Normativa": 45000000,
    "Lei": 46000000,
    "Lei Complementar": 47000000,
    "Norma Técnica": 52000000,
    "Ordem de Serviço": 53000000,
    "Ordem de Serviço Conjunta": 54000000,
    "Parecer Normativo": 57000000,
    "Parecer Referencial": "877d20147e02451e929fcfa80ae76de3",
    "Plano": 58000000,
    "Portaria": 59000000,
    "Portaria Conjunta": 60000000,
    "Portaria Normativa": 61000000,
    "Recomendação": 65000000,
    "Regimento": 66000000,
    "Regimento Interno": 67000000,
    "Regulamento": 68000000,
    "Resolução": 71000000,
    "Resolução Administrativa": 72000000,
    "Resolução Normativa": 75000000,
    "Resolução Ordinária": "037f6f0fc7a04d69834cf60007bba07d",
    "Súmula": 76000000,
    "Súmula Administrativa": "d74996b4f496432fa09fea831f4f72be",
}

VALID_SITUATIONS = {
    "Sem Revogação Expressa": "semrevogacaoexpressa",
    "Ajuizado": "ajuizado",
    "Alterado": "alterado",
    "Julgado Procedente": "julgadoprocedente",
    "Não conhecida": "naoconhecida",
}

INVALID_SITUATIONS = {
    "Anulado": "anulado",
    "Cancelado": "cancelado",
    "Cessar os efeitos": "cessarosefeitos",
    "Extinta": "extinta",
    "Inconstitucional": "inconstitucional",
    "Prejudicada": "prejudicada",
    "Revogado": "revogado",
    "Suspenso": "suspenso",
    "Sustado(a)": "sustado",
    "Tornado sem efeito": "tornadosemefeito",
}  # kept as reference for site taxonomy documentation

SITUATIONS = VALID_SITUATIONS | INVALID_SITUATIONS


def _build_stop_next_heading_re(
    types: dict, type_aliases: dict[str, tuple[str, ...]]
) -> re.Pattern[str]:
    """Precompute the stop-marker regex for multi-norm PDF pages."""
    all_type_names = sorted(
        {
            *types.keys(),
            *type_aliases.keys(),
            *(alias for aliases in type_aliases.values() for alias in aliases),
        },
        key=len,
        reverse=True,
    )
    alternation = "|".join(re.escape(t) for t in all_type_names)
    return re.compile(
        rf"\n(?:{alternation})\b\s*(?:N(?:[º°o]|o)?\.?\s*)?\d",
        re.IGNORECASE,
    )


class DFSinjScraper(StateScraper):
    """Webscraper for Distrito Federal state legislation website (https://www.sinj.df.gov.br/sinj/)

    Year start (earliest on source): 1922

    The SINJ datatable endpoint supports broad year-only searches. This scraper
    therefore fetches a whole year at a time and accepts every type/situation
    returned by the source, keeping ``TYPES`` and ``SITUATIONS`` only as source
    taxonomy documentation.
    """

    _iterate_situations = False
    _TEXT_ENDPOINT = "TextoArquivoNorma.aspx?id_file={id_file}"
    _RAW_ENDPOINT = "Norma/{ch_norma}/arquivo"
    _DETAILS_ENDPOINT = "DetalhesDeNorma.aspx?id_norma={ch_norma}"
    _DIARY_ENDPOINT = "BaixarArquivoDiario.aspx?id_file={id_file}"
    _SITE_CHROME_PATTERNS = (
        "Texto Compilado",
        "Visitar o SINJ-DF",
        "!print",
    )
    _TEXT_HEADER_RE = re.compile(
        r"^(?:Sistema Integrado de Normas Jurídicas do Distrito Federal\s*[\-–­]?\s*SINJ-DF|Legislação correlata\s*-\s*[^\n]+)\s*",
        re.IGNORECASE,
    )
    _DISCLAIMER_RE = re.compile(
        r"(?:Est[ea]|Ess[ea])\s+texto\s+n[aã]o\s+substitui",
        re.IGNORECASE,
    )
    _MAINTENANCE_RE = re.compile(
        r"TEXTO\s+EM\s+MANUTEN(?:Ç|C)(?:Ã|A)O",
        re.IGNORECASE,
    )
    _MAINTENANCE_DETAIL_RE = re.compile(
        r"TEXTO\s+DA\s+NORMA\s+EST(?:Á|A)\s+SENDO\s+(?:REVISAD|ATUALIZAD)|"
        r"INFORMA(?:Ç|C)(?:Õ|O)ES\s+IMPRECISAS",
        re.IGNORECASE,
    )
    _TYPE_ALIASES: dict[str, tuple[str, ...]] = {
        "ADI": ("Ação Direta de Inconstitucionalidade",),
    }

    # Precomputed stop-marker patterns for _clean_pdf_fallback_text
    _STOP_NEXT_HEADING_RE: re.Pattern[str]  # set after class body
    _STOP_ATOS_RE = re.compile(r"\nATOS?\s+DA\s+[A-ZÁÉÍÓÚÂÊÔÃÕÇ\s-]{3,}", re.IGNORECASE)
    _STOP_DIGITAL_SIGN_RE = re.compile(
        r"\nDocumento\s+assinado\s+digitalmente", re.IGNORECASE
    )

    def __init__(
        self,
        base_url: str = "https://www.sinj.df.gov.br/sinj",
        **kwargs,
    ):
        super().__init__(
            base_url,
            name="DISTRITO_FEDERAL",
            types=TYPES,
            situations=SITUATIONS,
            **kwargs,
        )
        self.search_url = (
            f"{self.base_url}/ashx/Datatable/ResultadoDePesquisaNormaDatatable.ashx"
        )
        self._display_length = 5000

    @staticmethod
    def _infer_norm_type(title: str) -> str:
        """Infer a norm type from the rendered title when the API omits it."""
        normalized_title = " ".join(title.split())
        for type_name in sorted(TYPES, key=len, reverse=True):
            if normalized_title.casefold().startswith(type_name.casefold()):
                return type_name
        return ""

    def _build_payload(
        self,
        year: int,
        *,
        offset: int = 0,
        limit: int | None = None,
    ) -> list[tuple[str, object]]:
        """Build the minimal datatable payload needed for a year-wide search."""
        display_length = self._display_length if limit is None else limit
        return [
            ("bbusca", "sinj_norma"),
            ("tipo_pesquisa", "avancada"),
            (
                "argumento",
                f"number#ano_assinatura#Ano de Assinatura#igual#igual a#{year}#{year}#E",
            ),
            ("iDisplayStart", offset),
            ("iDisplayLength", display_length),
        ]

    @staticmethod
    def _pick_file_info(item_info: dict) -> dict:
        current_file = item_info.get("ar_atualizado") or {}
        if current_file.get("id_file"):
            return current_file

        for source in item_info.get("fontes") or []:
            file_info = (source or {}).get("ar_fonte") or {}
            if file_info.get("id_file"):
                return file_info

        return {}

    @classmethod
    def _build_text_url(cls, base_url: str, file_id: str) -> str:
        return f"{base_url}/{cls._TEXT_ENDPOINT.format(id_file=file_id)}"

    @classmethod
    def _build_raw_url(cls, base_url: str, ch_norma: str) -> str:
        return f"{base_url}/{cls._RAW_ENDPOINT.format(ch_norma=ch_norma)}"

    @staticmethod
    def _number_pattern(number: str) -> str:
        normalized = re.sub(r"\W+", "", number or "")
        if not normalized:
            return ""
        return r"\W*".join(re.escape(ch) for ch in normalized)

    def _iter_title_patterns(self, doc_info: dict) -> list[str]:
        norm_type = (doc_info.get("type") or "").strip()
        number_pattern = self._number_pattern(str(doc_info.get("number") or ""))
        type_variants = [norm_type]
        type_variants.extend(self._TYPE_ALIASES.get(norm_type, ()))

        patterns = []
        for type_variant in type_variants:
            if not type_variant:
                continue
            if number_pattern:
                patterns.append(
                    rf"\b{re.escape(type_variant)}\b\s*(?:N(?:[º°o]|o)?\.?\s*)?{number_pattern}"
                )
            if not (
                number_pattern
                and type_variant.isupper()
                and len(type_variant.replace(" ", "")) <= 4
            ):
                patterns.append(rf"\b{re.escape(type_variant)}\b")

        return patterns

    def _trim_to_title(
        self,
        text: str,
        doc_info: dict,
        *,
        max_start: int | None = 400,
    ) -> str:
        for pattern in self._iter_title_patterns(doc_info):
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match and (max_start is None or match.start() <= max_start):
                return text[match.start() :]

        return text

    @staticmethod
    def _normalize_whitespace(text: str) -> str:
        """Collapse runs of spaces/tabs and excess blank lines."""
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n[ \t]+", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _normalize_linebreaks(text: str) -> str:
        """Normalize line endings and remove soft hyphens."""
        text = text.replace("\r", "\n").replace("\x0c", "\n")
        return text.replace("\u00ad", "")

    def _clean_extracted_text(self, text: str, doc_info: dict) -> str:
        cleaned = self._normalize_linebreaks(text)
        cleaned = self._trim_to_title(cleaned, doc_info)
        cleaned = self._TEXT_HEADER_RE.sub("", cleaned, count=1).strip()

        disclaimer_match = self._DISCLAIMER_RE.search(cleaned)
        if disclaimer_match:
            cleaned = cleaned[: disclaimer_match.start()].rstrip()

        return self._normalize_whitespace(cleaned)

    _SUMMARY_NORMALIZE_RE = re.compile(r"[^\w]", re.UNICODE)

    @classmethod
    def _normalize_for_compare(cls, text: str) -> str:
        return cls._SUMMARY_NORMALIZE_RE.sub("", text).lower()

    @classmethod
    def _remove_summary_element(cls, content_root: Tag, summary: str) -> None:
        """Remove the summary/ementa from *content_root*.

        First tries to decompose a child *tag* (``<p>``, ``<span>``, etc.)
        whose text matches the summary.  If no suitable tag is found (common on
        SINJ pages where ``div_texto`` contains only raw text nodes), falls back
        to replacing the matching ``NavigableString`` text node directly.
        """
        if not summary:
            return

        normalized_summary = cls._normalize_for_compare(summary)
        if not normalized_summary:
            return

        # --- Strategy 1: decompose a child tag containing the summary ---
        candidates: list[tuple[int, Tag]] = []
        for tag in content_root.find_all(["p", "span", "font", "div", "td", "table"]):
            tag_text = tag.get_text(" ", strip=True)
            normalized_tag = cls._normalize_for_compare(tag_text)
            if not normalized_tag:
                continue
            if normalized_summary not in normalized_tag:
                continue
            lower_text = tag_text.lower()
            if "art." in lower_text or "o governador" in lower_text:
                continue
            candidates.append((len(normalized_tag), tag))

        if candidates:
            _, best_tag = min(candidates, key=lambda item: item[0])
            best_tag.decompose()
            return

        # --- Strategy 2: strip summary from raw NavigableString text nodes ---
        # SINJ pages often have div_texto with a single text node containing
        # the entire document.  Find and excise just the summary line.
        for node in list(content_root.children):
            if not isinstance(node, NavigableString):
                continue
            text = str(node)
            normalized_text = cls._normalize_for_compare(text)
            if normalized_summary not in normalized_text:
                continue
            # Walk lines to find the one matching the summary
            lines = text.split("\n")
            filtered = []
            removed = False
            for line in lines:
                if (
                    not removed
                    and cls._normalize_for_compare(line) == normalized_summary
                ):
                    removed = True
                    continue
                filtered.append(line)
            if removed:
                node.replace_with(NavigableString("\n".join(filtered)))
                return

    @classmethod
    def _strip_summary_text(cls, text: str, summary: str) -> str:
        """Remove the summary/ementa from the beginning of plain text (PDF path)."""
        if not summary:
            return text
        normalized_summary = cls._normalize_for_compare(summary)
        if not normalized_summary:
            return text
        # Find the first line break after the title line
        first_nl = text.find("\n")
        if first_nl < 0:
            return text
        # Search for the summary within a reasonable window after the title
        search_region = text[first_nl : first_nl + len(summary) * 3]
        normalized_region = cls._normalize_for_compare(search_region)
        pos = normalized_region.find(normalized_summary)
        if pos < 0:
            return text
        # Map normalized position back to original text position
        consumed_norm = 0
        orig_start = 0
        for i, ch in enumerate(search_region):
            if consumed_norm >= pos:
                orig_start = i
                break
            if cls._SUMMARY_NORMALIZE_RE.match(ch) is None:
                consumed_norm += 1
        consumed_norm = 0
        orig_end = len(search_region)
        for i, ch in enumerate(search_region[orig_start:], start=orig_start):
            if consumed_norm >= len(normalized_summary):
                orig_end = i
                break
            if cls._SUMMARY_NORMALIZE_RE.match(ch) is None:
                consumed_norm += 1
        abs_start = first_nl + orig_start
        abs_end = first_nl + orig_end
        return (text[:abs_start].rstrip() + "\n" + text[abs_end:].lstrip()).strip()

    async def _extract_html_markdown(self, soup: BeautifulSoup, doc_info: dict) -> str:
        """Extract markdown from a pre-parsed BeautifulSoup object."""
        norm_text_tag = soup.find("div", id="div_texto")
        if not norm_text_tag:
            return ""

        self._remove_summary_element(norm_text_tag, doc_info.get("summary", ""))

        text_markdown = self._clean_extracted_text(
            norm_text_tag.get_text("\n", strip=False),
            doc_info,
        )
        if valid_markdown(text_markdown)[0]:
            return text_markdown

        cleaned_tag = self._clean_norm_soup(
            norm_text_tag,
            remove_disclaimers=True,
            unwrap_links=True,
            remove_empty_tags=True,
            strip_styles=True,
            remove_style_tags=True,
            remove_script_tags=True,
        )
        html_string = wrap_html(str(cleaned_tag))
        text_markdown = await self._get_markdown(html_content=html_string)
        return self._clean_extracted_text(text_markdown, doc_info)

    def _clean_pdf_fallback_text(self, text: str, doc_info: dict) -> str:
        cleaned = self._normalize_linebreaks(text)
        cleaned = self._trim_to_title(cleaned, doc_info, max_start=None)
        cleaned = self._strip_summary_text(cleaned, doc_info.get("summary", ""))

        cut_positions = []
        for pattern in (
            self._STOP_NEXT_HEADING_RE,
            self._STOP_ATOS_RE,
            self._STOP_DIGITAL_SIGN_RE,
        ):
            match = pattern.search(cleaned[1:])
            if match:
                cut_positions.append(match.start() + 1)
        if cut_positions:
            cleaned = cleaned[: min(cut_positions)].rstrip()

        disclaimer_match = self._DISCLAIMER_RE.search(cleaned)
        if disclaimer_match:
            cleaned = cleaned[: disclaimer_match.start()].rstrip()

        return self._normalize_whitespace(cleaned)

    @classmethod
    def _is_maintenance_soup(cls, soup: BeautifulSoup) -> bool:
        """Return True if *soup* contains a SINJ maintenance placeholder."""
        text = soup.get_text(" ", strip=True)
        if not text:
            return False
        return bool(cls._MAINTENANCE_RE.search(text)) and bool(
            cls._MAINTENANCE_DETAIL_RE.search(text)
        )

    @classmethod
    def _looks_like_site_chrome(cls, text_markdown: str) -> bool:
        return any(pattern in text_markdown for pattern in cls._SITE_CHROME_PATTERNS)

    @classmethod
    def _build_details_url(cls, base_url: str, ch_norma: str) -> str:
        return f"{base_url}/{cls._DETAILS_ENDPOINT.format(ch_norma=ch_norma)}"

    @classmethod
    def _build_diary_url(cls, base_url: str, file_id: str) -> str:
        return f"{base_url}/{cls._DIARY_ENDPOINT.format(id_file=file_id)}"

    async def _fetch_details_json(self, ch_norma: str) -> dict:
        details_url = self._build_details_url(self.base_url, ch_norma)
        response = await self.request_service.make_request(details_url)
        if not response:
            return {}

        client_response = cast(aiohttp.ClientResponse, response)
        html = await client_response.text()
        match = re.search(
            r"json_norma\s*=\s*(\{.*?\});\s*var\s+highlight",
            html,
            flags=re.DOTALL,
        )
        if not match:
            return {}

        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            return {}

    @staticmethod
    def _parse_diary_pages(page_text: str) -> list[int]:
        page_numbers = [int(number) for number in re.findall(r"\d+", page_text or "")]
        if not page_numbers:
            return []

        lower = (page_text or "").lower()
        if " a " in lower or " até " in lower or "-" in lower:
            start, end = page_numbers[0], page_numbers[-1]
            if start <= end:
                return list(range(start, end + 1))

        return sorted(set(page_numbers))

    @staticmethod
    def _extract_pdf_pages(pdf_bytes: bytes, pages: list[int]) -> bytes:
        if not pages:
            return pdf_bytes

        source = fitz.open(stream=pdf_bytes, filetype="pdf")
        extracted = fitz.open()
        try:
            for page_number in pages:
                page_index = page_number - 1
                if 0 <= page_index < source.page_count:
                    extracted.insert_pdf(
                        source, from_page=page_index, to_page=page_index
                    )
            if extracted.page_count == 0:
                return pdf_bytes
            return extracted.tobytes(garbage=4, deflate=True)
        finally:
            extracted.close()
            source.close()

    async def _fetch_diary_pdf_fallback(
        self, doc: dict
    ) -> tuple[str, bytes, str, str] | None:
        ch_norma = str(doc.get("ch_norma") or "")
        if not ch_norma:
            return None

        details = await self._fetch_details_json(ch_norma)
        for source in details.get("fontes") or []:
            diary_file = (source or {}).get("ar_diario") or {}
            diary_file_id = str(diary_file.get("id_file") or "").strip()
            if not diary_file_id:
                continue

            diary_url = self._build_diary_url(self.base_url, diary_file_id)
            response = await self.request_service.make_request(diary_url)
            if not response:
                continue

            client_response = cast(aiohttp.ClientResponse, response)
            pdf_bytes = await client_response.read()
            page_numbers = self._parse_diary_pages(
                str((source or {}).get("nr_pagina") or "")
            )
            pdf_slice = self._extract_pdf_pages(pdf_bytes, page_numbers)
            text_markdown = await self._get_markdown(
                stream=BytesIO(pdf_slice),
                filename=diary_file.get("filename") or "diario.pdf",
            )
            text_markdown = self._clean_pdf_fallback_text(text_markdown, doc)
            valid, _ = valid_markdown(text_markdown)
            if valid:
                return text_markdown, pdf_slice, ".pdf", diary_url

        return None

    async def _fetch_search_page(
        self,
        payload: list[tuple[str, object]],
    ) -> tuple[list[dict], int]:
        response = await self.request_service.make_request(
            self.search_url,
            method="POST",
            payload=payload,
        )
        if not response:
            return [], 0

        client_response = cast(aiohttp.ClientResponse, response)
        data = await client_response.json()
        docs = []
        for item in data.get("aaData") or []:
            item_info = item.get("_source") or {}
            ch_norma = str(item_info.get("ch_norma") or "").strip()
            if not ch_norma:
                continue

            file_info = self._pick_file_info(item_info)
            fallback_url = self._build_raw_url(self.base_url, ch_norma)
            title = (
                f"{item_info.get('nm_tipo_norma', 'Norma')} "
                f"{item_info.get('nr_norma', '')} de {item_info.get('dt_assinatura', '')}"
            ).strip()
            norm_type = (
                item_info.get("nm_tipo_norma") or ""
            ).strip() or self._infer_norm_type(title)

            docs.append(
                {
                    "title": title,
                    "summary": item_info.get("ds_ementa", ""),
                    "date": item_info.get("dt_assinatura", ""),
                    "number": item_info.get("nr_norma", ""),
                    "type": norm_type,
                    "situation": item_info.get("nm_situacao") or "NA",
                    "document_url": fallback_url,
                    "file_id": file_info.get("id_file") or "",
                    "file_name": file_info.get("filename") or "",
                    "file_mimetype": file_info.get("mimetype") or "",
                    "ch_norma": ch_norma,
                }
            )

        total = int(data.get("iTotalDisplayRecords") or len(docs) or 0)
        return docs, total

    async def _get_docs_links(
        self, url: str, payload: list[tuple[str, object]]
    ) -> list[dict]:
        """Template-method implementation — wraps ``_fetch_search_page`` and discards the total.

        The internal scraping flow (``_scrape_year``) calls ``_fetch_search_page``
        directly to retain the total count for pagination.  This method exists for
        interface compliance with ``BaseScraper`` and for external/test callers that
        only need the document list.
        """
        docs, _ = await self._fetch_search_page(payload)
        return docs

    async def _handle_maintenance(self, soup: BeautifulSoup, doc: dict) -> bool:
        """Log a maintenance error and return True if *soup* is a maintenance page."""
        if not self._is_maintenance_soup(soup):
            return False
        fallback_url = doc.get("document_url", "")
        await self._save_doc_error(
            title=doc.get("title", ""),
            year=doc.get("year", ""),
            situation=doc.get("situation", ""),
            norm_type=doc.get("type", ""),
            html_link=fallback_url,
            error_message="Norm text is in maintenance on SINJ",
        )
        return True

    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        """Fetch a single norm, preferring the HTML text endpoint over raw PDFs."""
        doc = dict(doc_info)
        document_url = doc.get("document_url", "")
        title = doc.get("title", "Unknown")
        file_id = str(doc.get("file_id") or "").strip()
        file_name = doc.get("file_name") or ""
        ch_norma = str(doc.get("ch_norma") or "")
        fallback_url = document_url or self._build_raw_url(self.base_url, ch_norma)
        text_url = self._build_text_url(self.base_url, file_id) if file_id else ""
        download_url = (
            f"{self.base_url}/Norma/{ch_norma}/{file_name}"
            if file_name and ch_norma
            else ""
        )
        pdf_file = file_name.lower().endswith(".pdf")
        if download_url and not pdf_file:
            primary_url = download_url
        else:
            primary_url = text_url or fallback_url

        if self._is_already_scraped(primary_url, title):
            return None

        try:
            # --- Attempt 1: compiled HTML text endpoint ---
            if file_id:
                response = await self.request_service.make_request(text_url)
                if response:
                    client_response = cast(aiohttp.ClientResponse, response)
                    body = await client_response.read()
                    soup = BeautifulSoup(body, "html.parser")
                    if await self._handle_maintenance(soup, doc):
                        return None
                    text_markdown = await self._extract_html_markdown(soup, doc)
                    valid, _ = valid_markdown(text_markdown)
                    if valid and not self._looks_like_site_chrome(text_markdown):
                        dl_link = soup.find("a", class_="baixarArquivo")
                        if dl_link and dl_link.get("href"):
                            doc_url = urljoin(text_url, str(dl_link["href"]))
                        else:
                            doc_url = download_url or text_url
                        # PDF download URLs trigger Playwright downloads instead of
                        # loading a page — use the HTML text endpoint for MHTML capture.
                        if doc_url.lower().endswith(".pdf"):
                            doc_url = text_url
                        doc["text_markdown"] = text_markdown
                        doc["document_url"] = doc_url
                        try:
                            doc["_raw_content"] = await self._capture_mhtml(doc_url)
                            doc["_content_extension"] = ".mhtml"
                        except Exception as exc:
                            logger.warning(f"MHTML capture failed for {doc_url}: {exc}")
                            await self._save_doc_error(
                                title=title,
                                year=doc.get("year", ""),
                                situation=doc.get("situation", ""),
                                norm_type=doc.get("type", ""),
                                html_link=doc_url,
                                error_message=f"MHTML capture failed: {exc}",
                            )
                            return None
                        return doc

            # --- Attempt 2: raw /arquivo endpoint ---
            response = await self.request_service.make_request(fallback_url)
            if not response:
                raise RuntimeError(f"No response for {fallback_url}")

            client_response = cast(aiohttp.ClientResponse, response)
            body = await client_response.read()
            filename, content_type = self.request_service.detect_content_info(
                client_response
            )

            if is_pdf(body, content_type):
                # --- Attempt 2b: direct PDF conversion (bytes already in memory) ---
                text_markdown = await self._get_markdown(
                    stream=BytesIO(body),
                    filename=filename or doc.get("file_name") or "document.pdf",
                )
                valid, reason = valid_markdown(text_markdown)
                if valid and not self._looks_like_site_chrome(text_markdown):
                    doc["text_markdown"] = text_markdown
                    doc["document_url"] = fallback_url
                    doc["_raw_content"] = body
                    doc["_content_extension"] = detect_extension(content_type, filename)
                    return doc

                # --- Attempt 3 (PDF): diary PDF slice — only for image/scanned PDFs ---
                diary_fallback = await self._fetch_diary_pdf_fallback(doc)
                if diary_fallback is not None:
                    text_markdown, raw_content, content_ext, diary_url = diary_fallback
                    doc["text_markdown"] = text_markdown
                    doc["document_url"] = diary_url
                    doc["_raw_content"] = raw_content
                    doc["_content_extension"] = content_ext
                    return doc

                await self._save_doc_error(
                    title=title,
                    year=doc.get("year", ""),
                    situation=doc.get("situation", ""),
                    norm_type=doc.get("type", ""),
                    html_link=fallback_url,
                    error_message=f"Could not extract valid markdown from PDF: {reason}",
                )
                return None

            # HTML path
            soup = BeautifulSoup(body, "html.parser")
            if await self._handle_maintenance(soup, doc):
                return None

            text_markdown = await self._extract_html_markdown(soup, doc)
            valid, reason = valid_markdown(text_markdown)
            if valid and not self._looks_like_site_chrome(text_markdown):
                doc["text_markdown"] = text_markdown
                doc["document_url"] = fallback_url
                try:
                    doc["_raw_content"] = await self._capture_mhtml(fallback_url)
                    doc["_content_extension"] = ".mhtml"
                except Exception as exc:
                    logger.warning(f"MHTML capture failed for {fallback_url}: {exc}")
                    await self._save_doc_error(
                        title=title,
                        year=doc.get("year", ""),
                        situation=doc.get("situation", ""),
                        norm_type=doc.get("type", ""),
                        html_link=fallback_url,
                        error_message=f"MHTML capture failed: {exc}",
                    )
                    return None
                return doc

            # --- Attempt 3 (HTML): diary PDF slice ---
            diary_fallback = await self._fetch_diary_pdf_fallback(doc)
            if diary_fallback is not None:
                text_markdown, raw_content, content_ext, diary_url = diary_fallback
                doc["text_markdown"] = text_markdown
                doc["document_url"] = diary_url
                doc["_raw_content"] = raw_content
                doc["_content_extension"] = content_ext
                return doc

            # --- Attempt 4: generic markdown from full HTML page ---
            text_markdown = await self._get_markdown(
                stream=BytesIO(body),
                filename=filename or doc.get("file_name") or "document.html",
            )
            valid, reason = valid_markdown(text_markdown)
            if not valid or self._looks_like_site_chrome(text_markdown):
                await self._save_doc_error(
                    title=title,
                    year=doc.get("year", ""),
                    situation=doc.get("situation", ""),
                    norm_type=doc.get("type", ""),
                    html_link=fallback_url,
                    error_message=f"Could not extract valid markdown: {reason}",
                )
                return None

            doc["text_markdown"] = text_markdown
            doc["document_url"] = fallback_url
            doc["_raw_content"] = body
            doc["_content_extension"] = detect_extension(content_type, filename)
            return doc
        except Exception as exc:
            logger.exception(f"Error getting document data for {fallback_url}: {exc}")
            await self._save_doc_error(
                title=title,
                year=doc.get("year", ""),
                situation=doc.get("situation", ""),
                norm_type=doc.get("type", ""),
                html_link=fallback_url,
                error_message=str(exc),
            )
            return None

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape a whole SINJ year via a broad datatable query."""
        first_page_docs, total = await self._fetch_search_page(
            self._build_payload(year)
        )
        if total == 0:
            return []

        documents = list(first_page_docs)
        if total > len(first_page_docs):
            offsets = range(len(first_page_docs), total, self._display_length)
            page_results = await self._gather_results(
                [
                    self._fetch_search_page(self._build_payload(year, offset=o))
                    for o in offsets
                ],
                context={"year": year, "type": "NA", "situation": "NA"},
                desc=f"DISTRITO FEDERAL | {year} | get_docs_links",
            )
            for page_docs, _ in page_results:
                if page_docs:
                    documents.extend(page_docs)

        if not documents:
            return []

        for doc in documents:
            doc["year"] = year

        return await self._process_documents(
            documents,
            year=year,
            norm_type="NA",
            situation="NA",
            desc=f"DISTRITO FEDERAL | {year} | docs",
        )


# Precompute the stop-next-heading regex at class definition time.
# Done outside the class body so it can reference both TYPES and _TYPE_ALIASES.
DFSinjScraper._STOP_NEXT_HEADING_RE = _build_stop_next_heading_re(
    TYPES, DFSinjScraper._TYPE_ALIASES
)
