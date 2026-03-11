"""Base scraper for SAPL (Sistema de Apoio ao Processo Legislativo) API sites.

Many Brazilian state legislatures use SAPL with identical REST API structures.
This base class provides the shared logic for:
  - Paginated norm search via ``/api/norma/normajuridica/``
  - Subject (assunto) fetching via ``/api/norma/assuntonorma/``
  - PDF processing with markitdown → OCR fallback
"""

from __future__ import annotations

import re

from collections import defaultdict
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

from loguru import logger
from src.scraper.base.converter import valid_markdown
from src.scraper.base.scraper import (
    DEFAULT_INVALID_SITUATION,
    DEFAULT_VALID_SITUATION,
    StateScraper,
    flatten_results,
)


def normalize_title_text(text: str) -> str:
    """Normalize a title-ish line for loose comparisons."""
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", " ", text.casefold())).strip()


def norm_line(raw: str) -> tuple[str, str]:
    """Return ``(compact, lower)`` for *raw*, where compact collapses whitespace."""
    compact = " ".join(raw.split())
    return compact, compact.lower()


class SAPLBaseScraper(StateScraper):
    """Base scraper for SAPL API-based state legislation sites.

    Subclasses must set:
        - ``TYPES``: dict mapping norm type names to API IDs
        - ``base_url`` (via ``__init__`` default)
        - ``name`` (via ``__init__`` default)

    Subclasses may override:
        - ``_process_pdf`` for state-specific PDF handling strategies
    """

    _SEI_NOISE_PATTERNS: tuple[str, ...] = (
        "(assinado eletronicamente)",
        "documento assinado eletronicamente",
        "horário oficial de brasília",
        "horario oficial de brasilia",
        "fundamento no cap. iii, art. 14 do decreto estadual",
        "a autenticidade deste documento pode ser conferida no site",
        "acao=documento_conferir",
        "controlador_externo.php",
        "id_orgao_acesso_externo",
        "código verificador",
        "codigo verificador",
        "código crc",
        "codigo crc",
        "referência: caso responda este documento",
        "referencia: caso responda este documento",
    )
    _SAPL_TITLE_RE = re.compile(
        r"^(CONSTITUIÇÃO ESTADUAL|DECRETO LEGISLATIVO|DECRETO|EMENDA CONSTITUCIONAL|LEI COMPLEMENTAR|LEI DELEGADA|LEI|RESOLU[ÇC][AÃ]O)\b",
        re.IGNORECASE,
    )
    _EXTRA_DOC_START_PATTERNS: tuple[str, ...] = (
        "governo do estado do piauí",
        "secretaria de governo do estado do piauí",
        "setor de protocolo alepi",
        "expediente ",
        "ofício pres. sgm",
        "oficio pres. sgm",
        "proposição ",
        "proposicao ",
    )
    _FOOTER_BLOCK_PATTERNS: tuple[str, ...] = (
        "a autenticidade do documento pode ser conferida",
        "a autenticidade deste documento pode ser conferida",
        "informando o código verificador",
        "informando o codigo verificador",
        "documento.imprimir",
        "controlador.php",
        "infrasistem",
        "assinatura eletr",
    )
    _PROCESS_NUMBER_RE = re.compile(r"^\d{5}\.\d{6,7}/\d{4}[-.]\d{2}$")
    _SEI_PAGE_COUNTER_RE = re.compile(r"^\d+\s*/\s*\d+$")
    _SEI_TIMESTAMP_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4},\s*\d{1,2}:\d{2}$")
    _SEI_VERSION_RE = re.compile(r"^\d[\dv\s]{6,}$", re.IGNORECASE)

    def _title_match_score(self, line: str, expected_title: str) -> int:
        """Score how well a line matches the expected document title."""
        normalized_line = normalize_title_text(line)
        normalized_expected = normalize_title_text(expected_title)
        line_tokens = {
            token for token in re.findall(r"\w+", normalized_line) if len(token) > 1
        }
        expected_tokens = [
            token for token in re.findall(r"\w+", normalized_expected) if len(token) > 1
        ]
        score = 0
        for token in expected_tokens:
            if token in line_tokens:
                score += 2 if token.isdigit() else 1
        return score

    def _build_pdf_fetch_urls(self, pdf_link: str) -> list[str]:
        """Build ordered candidate URLs for fetching a SAPL attachment."""
        resolved = urljoin(self.base_url, pdf_link)
        candidates: list[str] = []

        def _add(url: str) -> None:
            if url and url not in candidates:
                candidates.append(url)

        base_parts = urlsplit(self.base_url)
        resolved_parts = urlsplit(resolved)
        if (
            base_parts.scheme == "https"
            and resolved_parts.scheme == "http"
            and resolved_parts.netloc == base_parts.netloc
        ):
            _add(
                urlunsplit(
                    (
                        "https",
                        resolved_parts.netloc,
                        resolved_parts.path,
                        resolved_parts.query,
                        resolved_parts.fragment,
                    )
                )
            )
        _add(resolved)
        return candidates

    def _is_norm_content_line(self, compact: str, lower: str) -> bool:
        """Return whether a line looks like substantive legal text."""
        if not compact:
            return False
        if self._SAPL_TITLE_RE.match(compact):
            return True
        return bool(
            re.match(
                r"^(art\.|§|\$|par[aá]grafo|cap[ií]tulo|t[ií]tulo|se[çc][aã]o|anexo\b|(?:[ivxlcdm]+|\d+)\s*-)",
                lower,
            )
            or compact.upper().startswith("PALÁCIO")
        )

    def _is_footer_block_line(self, compact: str, lower: str) -> bool:
        """Return whether a line belongs to a SEI/authenticity footer block."""
        if not compact:
            return False
        if any(pattern in lower for pattern in self._FOOTER_BLOCK_PATTERNS):
            return True
        if lower.startswith("(assinatura") or lower.startswith("assinatura"):
            return True
        if lower.startswith("sei/"):
            return True
        if lower.startswith("http") and "sei" in lower:
            return True
        if self._PROCESS_NUMBER_RE.match(compact):
            return True
        if self._SEI_PAGE_COUNTER_RE.match(compact):
            return True
        if self._SEI_TIMESTAMP_RE.match(compact):
            return True
        if self._SEI_VERSION_RE.match(compact) and "v" in lower:
            return True
        if lower.startswith("roraima, em "):
            return True
        return False

    def __init__(
        self,
        base_url: str,
        name: str,
        types: dict,
        page_size: int = 100,
        **kwargs: Any,
    ):
        super().__init__(base_url, types=types, situations={}, name=name, **kwargs)
        self.subjects: dict[int, str] = {}
        self._id_to_type: dict[int, str] = {v: k for k, v in types.items()}
        self._page_size = max(1, min(page_size, 100))

    # ------------------------------------------------------------------
    # URL formatting
    # ------------------------------------------------------------------

    def _format_search_url(
        self,
        norm_type_id: str | int,
        year: int,
        page: int = 1,
    ) -> str:
        """Format URL for SAPL norm search API (single type)."""
        return (
            f"{self.base_url}/api/norma/normajuridica/"
            f"?tipo={norm_type_id}&page={page}&ano={year}&page_size={self._page_size}"
        )

    def _format_year_url(self, year: int, page: int = 1) -> str:
        """Format URL for all-types SAPL API query for a given year."""
        return (
            f"{self.base_url}/api/norma/normajuridica/"
            f"?page={page}&ano={year}&page_size={self._page_size}"
        )

    def _format_subjects_url(self, page: int = 1) -> str:
        """Format URL for SAPL subject listing."""
        return (
            f"{self.base_url}/api/norma/assuntonorma/"
            f"?page={page}&page_size={self._page_size}"
        )

    def _infer_situation(self, item: dict[str, Any]) -> str:
        """Infer document situation from SAPL payload fields."""
        return (
            DEFAULT_INVALID_SITUATION
            if item.get("data_vigencia")
            else DEFAULT_VALID_SITUATION
        )

    def _resolve_type_name(self, tipo_id: int | None) -> str:
        """Resolve SAPL type id to human-readable label."""
        return self._id_to_type.get(tipo_id, "Outros") if tipo_id else "Outros"

    async def _report_missing_texto_integral(self, item: dict[str, Any]) -> None:
        """Record a norm skipped because the SAPL API exposes no attachment URL."""
        title = item.get("__str__") or f"Norma {item.get('numero', '')}"
        tipo_id = item.get("tipo")
        type_name = self._resolve_type_name(tipo_id)
        detail_url = urljoin(self.base_url, item.get("link_detail_backend", ""))
        year = item.get("ano") or item.get("data", "")
        situation = self._infer_situation(item)

        logger.warning(
            f"{self.name} | Missing texto_integral | year={year} | "
            f"type={type_name} | title={title}"
        )
        await self._save_doc_error(
            title=title,
            year=year,
            situation=situation,
            norm_type=type_name,
            html_link=detail_url,
            error_message="SAPL API returned no texto_integral attachment",
            norma_id=item.get("id"),
            materia_id=item.get("materia"),
            api_detail_url=(
                f"{self.base_url}/api/norma/normajuridica/{item['id']}"
                if item.get("id")
                else ""
            ),
        )

    def _strip_sei_noise(self, lines: list[str]) -> list[str]:
        """Remove SEI verification, signature, and page-counter lines."""
        out: list[str] = []
        for raw_line in lines:
            compact, lower = norm_line(raw_line)
            if not compact:
                out.append("")
                continue
            if any(pattern in lower for pattern in self._SEI_NOISE_PATTERNS):
                continue
            if lower.startswith(("sei nº", "sei n°", "sei no ")):
                continue
            if "sei" in lower and re.search(r"\bpg\.\s*\d+\b", lower):
                continue
            out.append(raw_line)
        return out

    def _find_content_start(self, lines: list[str], expected_title: str | None) -> int:
        """Return the index of the first line of the actual norm content.

        Searches for the title line using exact match, fuzzy token-score, then
        falls back to the last numbered SAPL-title-RE candidate.  Returns 0 if
        no reliable start can be found.
        """
        normalized_expected = (
            normalize_title_text(expected_title) if expected_title else ""
        )
        # Precompute compact form once for all lines (used by multiple branches below)
        compact_lines = [" ".join(line.split()) for line in lines]

        title_candidates = [
            idx
            for idx, compact in enumerate(compact_lines)
            if self._SAPL_TITLE_RE.match(compact)
        ]

        if normalized_expected:
            for idx, compact in enumerate(compact_lines):
                norm_line = normalize_title_text(compact)
                if norm_line and (
                    norm_line == normalized_expected
                    or norm_line.startswith(normalized_expected)
                ):
                    return idx

        if normalized_expected and title_candidates:
            scored = [
                (self._title_match_score(lines[idx], expected_title), idx)
                for idx in title_candidates
            ]
            best_score, best_idx = max(scored, default=(0, None))
            if best_idx is not None and best_score >= 3:
                return best_idx

        if title_candidates:
            first_norm_content_idx = next(
                (
                    idx
                    for idx, compact in enumerate(compact_lines)
                    if re.match(
                        r"^(art\.|cap[ií]tulo|t[ií]tulo|se[çc][aã]o|anexo\b)",
                        compact.lower(),
                    )
                    or compact.upper().startswith("PALÁCIO")
                ),
                None,
            )
            if first_norm_content_idx is not None:
                title_candidates = [
                    idx for idx in title_candidates if idx <= first_norm_content_idx
                ] or title_candidates
            numbered = [
                idx
                for idx in title_candidates
                if re.search(r"\b\d{1,4}\b", compact_lines[idx])
            ]
            return numbered[-1] if numbered else title_candidates[-1]

        return 0

    def _filter_footer_blocks(
        self, line_tuples: list[tuple[str, str, str]]
    ) -> list[tuple[str, str, str]]:
        """Remove footer-block lines (authentication blocks, stamps, etc.).

        Accepts (raw, compact, lower) tuples; returns tuples for kept lines.
        """
        out: list[tuple[str, str, str]] = []
        in_footer_block = False
        for raw_line, compact, lower in line_tuples:
            if in_footer_block:
                if self._is_norm_content_line(compact, lower):
                    in_footer_block = False
                else:
                    continue
            if self._is_footer_block_line(compact, lower):
                in_footer_block = True
                continue
            out.append((raw_line, compact, lower))
        return out

    def _truncate_extra_documents(
        self, line_tuples: list[tuple[str, str, str]]
    ) -> list[str]:
        """Stop at the first line that looks like the start of a new document.

        Accepts (raw, compact, lower) tuples; returns original raw lines.
        """
        out: list[str] = []
        seen_norm_content = False
        for raw_line, compact, lower in line_tuples:
            if self._is_footer_block_line(compact, lower):
                continue
            if self._is_norm_content_line(compact, lower):
                seen_norm_content = True
            if seen_norm_content and any(
                lower.startswith(pattern) for pattern in self._EXTRA_DOC_START_PATTERNS
            ):
                break
            out.append(raw_line)
        return out

    def _clean_sapl_pdf_markdown(
        self,
        text_markdown: str,
        expected_title: str | None = None,
    ) -> str:
        """Strip common SAPL / SEI verification footer noise from extracted PDFs."""
        normalized = (
            text_markdown.replace("\r\n", "\n")
            .replace("\r", "\n")
            .replace("\f", "\n")
            .replace("\u200b", "")
        )

        lines = self._strip_sei_noise(normalized.split("\n"))
        start = self._find_content_start(lines, expected_title)
        if start:
            lines = lines[start:]
        line_tuples = [(line, *norm_line(line)) for line in lines]
        filtered_tuples = self._filter_footer_blocks(line_tuples)
        lines = self._truncate_extra_documents(filtered_tuples)

        result = re.sub(r"\n{3,}", "\n\n", "\n".join(lines))
        return result.strip()

    # ------------------------------------------------------------------
    # Document link extraction
    # ------------------------------------------------------------------

    async def _get_docs_links(
        self, url: str, data: dict[str, Any] | None = None
    ) -> list:
        """Parse document list from a single SAPL API page."""
        if data is None:
            response = await self.request_service.make_request(url)
            if not response:
                return []

            data = await response.json()

        items = data.get("results", [])
        docs = []

        for item in items:
            if not item.get("texto_integral"):
                await self._report_missing_texto_integral(item)
                continue

            # TODO: Verify SAPL `data_vigencia` semantics — it may mean
            # "end of validity date" (revoked) or simply "effective since date".
            # Using it as a revocation indicator until confirmed with API docs.
            situation = self._infer_situation(item)

            doc = {
                "id": item["id"],
                "norm_number": item["numero"],
                "title": item["__str__"],
                "situation": situation,
                "summary": item["ementa"],
                "subject": [self.subjects.get(s, "") for s in item.get("assuntos", [])],
                "date": item["data"],
                "origin": item.get("esfera_federacao"),
                "publication": item.get("veiculo_publicacao"),
                "tipo_id": item.get("tipo"),
                "pdf_link": item["texto_integral"],
            }
            docs.append(doc)

        return docs

    # ------------------------------------------------------------------
    # PDF processing (override in subclasses for custom strategies)
    # ------------------------------------------------------------------

    async def _process_pdf(
        self,
        pdf_link: str,
        _year: int,
        title: str | None = None,
    ) -> dict | None:
        """Download and convert a PDF to markdown.

        Default: tries markitdown, then falls back to LLM OCR.
        Override in subclasses for year-based or threshold-based strategies.
        """
        for fetch_url in self._build_pdf_fetch_urls(pdf_link):
            text_markdown, raw_bytes, ext = await self._download_and_convert(fetch_url)
            text_markdown = self._clean_sapl_pdf_markdown(
                text_markdown,
                expected_title=title,
            )
            valid, _ = valid_markdown(text_markdown)
            if not valid:
                continue
            return {
                "text_markdown": text_markdown.strip(),
                "document_url": pdf_link,
                "_raw_content": raw_bytes,
                "_content_extension": ext,
            }
        return None

    async def _get_doc_data(self, doc_info: dict, year: int = 0) -> dict | None:
        """Get full document data by processing the PDF attachment."""
        pdf_link = doc_info.pop("pdf_link")
        doc_info.pop("tipo_id", None)
        title = doc_info.get("title", "")

        if self._is_already_scraped(pdf_link, title):
            return None

        processed = await self._process_pdf(pdf_link, year, title=title)
        if processed is None:
            await self._save_doc_error(
                title=title,
                year=year or doc_info.get("date", ""),
                situation=doc_info.get("situation", ""),
                norm_type=doc_info.get("type", ""),
                html_link=pdf_link,
                error_message="PDF processing failed (no text extracted)",
            )
            return None
        doc_info["year"] = year
        doc_info.update(processed)

        return doc_info

    # ------------------------------------------------------------------
    # Subject fetching
    # ------------------------------------------------------------------

    async def _fetch_subjects(self) -> None:
        """Fetch all subjects (assuntos) from the SAPL API concurrently."""
        if self.subjects:
            return

        subjects_url = self._format_subjects_url()
        response = await self.request_service.make_request(subjects_url)
        if not response:
            return

        data = await response.json()
        total_pages = data.get("pagination", {}).get("total_pages", 1)

        subjects = {item["id"]: item["assunto"] for item in data.get("results", [])}

        # Fetch remaining pages concurrently
        if total_pages > 1:
            tasks = [
                self.request_service.make_request(self._format_subjects_url(page=page))
                for page in range(2, total_pages + 1)
            ]
            results = await self._gather_results(
                tasks,
                desc=f"{self.name} | Fetching subjects",
            )
            for resp in results:
                if resp:
                    page_data = await resp.json()
                    subjects.update(
                        {item["id"]: item["assunto"] for item in page_data["results"]}
                    )

        self.subjects = subjects

    # ------------------------------------------------------------------
    # Year-level scraping (all types in one paginated stream)
    # ------------------------------------------------------------------

    async def _scrape_year(self, year: int) -> list[dict]:
        """Fetch all norm types for *year* in a single paginated API stream."""
        url = self._format_year_url(year)
        response = await self.request_service.make_request(url)

        if not response or response.status != 200:
            status = response.status if response else "No response"
            logger.error(f"Error fetching data for Year: {year} | Status: {status}")
            return []

        data = await response.json()
        if not data.get("results"):
            return []

        total_pages = data.get("pagination", {}).get("total_pages", 1)

        link_tasks = [self._get_docs_links(url, data=data)]
        link_tasks.extend(
            self._get_docs_links(self._format_year_url(year, page=p))
            for p in range(2, total_pages + 1)
        )
        link_results = await self._gather_results(
            link_tasks,
            context={"year": year, "type": "NA", "situation": "NA"},
            desc=f"{self.name} | {year} | listing",
        )
        by_type: dict[str, list] = defaultdict(list)
        for result in link_results:
            for doc in result or []:
                if self._is_already_scraped(
                    doc.get("pdf_link", ""), doc.get("title", "")
                ):
                    continue
                tipo_id = doc.pop("tipo_id", None)
                type_name = self._resolve_type_name(tipo_id)
                by_type[type_name].append(doc)

        tasks = [
            self._process_documents(
                type_docs,
                year=year,
                norm_type=nt,
                situation="NA",
                doc_data_kwargs={"year": year},
            )
            for nt, type_docs in by_type.items()
        ]
        valid = await self._gather_results(
            tasks,
            context={"year": year, "type": "NA", "situation": "NA"},
            desc=f"{self.name} | Year {year}",
        )
        return flatten_results(valid)

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    async def _before_scrape(self) -> None:
        """Fetch subjects once before year iteration begins."""
        await self._fetch_subjects()
