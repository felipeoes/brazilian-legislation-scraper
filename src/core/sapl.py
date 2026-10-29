"""Base scraper for SAPL (Sistema de Apoio ao Processo Legislativo) API sites.

Many Brazilian state legislatures use SAPL with identical REST API structures.
This base class provides the shared logic for:
  - Paginated norm search via ``/api/norma/normajuridica/``
  - Subject (assunto) fetching via ``/api/norma/assuntonorma/``
  - PDF processing with pymupdf4llm → OCR fallback
"""

import re
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

from src.core.document import Document
from src.core.http import HttpError
from src.core.scraper import (
    DEFAULT_INVALID_SITUATION,
    DEFAULT_VALID_SITUATION,
    BaseScraper,
)


def normalize_title_text(text: str) -> str:
    """Normalize a title-ish line for loose comparisons."""
    return re.sub("\\s+", " ", re.sub("[^\\w\\s]", " ", text.casefold())).strip()


def norm_line(raw: str) -> tuple[str, str]:
    """Return ``(compact, lower)`` for *raw*, where compact collapses whitespace."""
    compact = " ".join(raw.split())
    return (compact, compact.lower())


class SAPLBaseScraper(BaseScraper):
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
        "^(CONSTITUIÇÃO ESTADUAL|DECRETO LEGISLATIVO|DECRETO|EMENDA CONSTITUCIONAL|LEI COMPLEMENTAR|LEI DELEGADA|LEI|RESOLU[ÇC][AÃ]O)\\b",
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
    _PROCESS_NUMBER_RE = re.compile("^\\d{5}\\.\\d{6,7}/\\d{4}[-.]\\d{2}$")
    _SEI_PAGE_COUNTER_RE = re.compile("^\\d+\\s*/\\s*\\d+$")
    _SEI_TIMESTAMP_RE = re.compile("^\\d{1,2}/\\d{1,2}/\\d{4},\\s*\\d{1,2}:\\d{2}$")
    _SEI_VERSION_RE = re.compile("^\\d[\\dv\\s]{6,}$", re.IGNORECASE)

    def _title_match_score(self, line: str, expected_title: str) -> int:
        """Score how well a line matches the expected document title."""
        normalized_line = normalize_title_text(line)
        normalized_expected = normalize_title_text(expected_title)
        line_tokens = {
            token for token in re.findall("\\w+", normalized_line) if len(token) > 1
        }
        expected_tokens = [
            token for token in re.findall("\\w+", normalized_expected) if len(token) > 1
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
            and (resolved_parts.netloc == base_parts.netloc)
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
                "^(art\\.|§|\\$|par[aá]grafo|cap[ií]tulo|t[ií]tulo|se[çc][aã]o|anexo\\b|(?:[ivxlcdm]+|\\d+)\\s*-)",
                lower,
            )
            or compact.upper().startswith("PALÁCIO")
        )

    def _is_footer_block_line(self, compact: str, lower: str) -> bool:
        """Return whether a line belongs to a SEI/authenticity footer block."""
        if not compact:
            return False
        if any((pattern in lower for pattern in self._FOOTER_BLOCK_PATTERNS)):
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

    def _format_search_url(
        self, norm_type_id: str | int, year: int, page: int = 1
    ) -> str:
        """Format URL for SAPL norm search API (single type)."""
        return f"{self.base_url}/api/norma/normajuridica/?tipo={norm_type_id}&page={page}&ano={year}&page_size={self._page_size}"

    def _format_year_url(self, year: int, page: int = 1) -> str:
        """Format URL for all-types SAPL API query for a given year."""
        return f"{self.base_url}/api/norma/normajuridica/?page={page}&ano={year}&page_size={self._page_size}"

    def _format_subjects_url(self, page: int = 1) -> str:
        """Format URL for SAPL subject listing."""
        return f"{self.base_url}/api/norma/assuntonorma/?page={page}&page_size={self._page_size}"

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

    def _strip_sei_noise(self, lines: list[str]) -> list[str]:
        """Remove SEI verification, signature, and page-counter lines."""
        out: list[str] = []
        for raw_line in lines:
            compact, lower = norm_line(raw_line)
            if not compact:
                out.append("")
                continue
            if any((pattern in lower for pattern in self._SEI_NOISE_PATTERNS)):
                continue
            if lower.startswith(("sei nº", "sei n°", "sei no ")):
                continue
            if "sei" in lower and re.search("\\bpg\\.\\s*\\d+\\b", lower):
                continue
            out.append(raw_line)
        return out

    def _find_exact_title_match(
        self, compact_lines: list[str], normalized_expected: str
    ) -> int | None:
        """Return the index of the first line that exactly matches (or starts with) the expected title.

        Returns ``None`` when no match is found.
        """
        for idx, compact in enumerate(compact_lines):
            norm = normalize_title_text(compact)
            if norm and (
                norm == normalized_expected or norm.startswith(normalized_expected)
            ):
                return idx
        return None

    def _find_fuzzy_title_match(
        self, lines: list[str], expected_title: str, title_candidates: list[int]
    ) -> int | None:
        """Score each title candidate against *expected_title* and return the best if score ≥ 3.

        Returns ``None`` when no candidate scores high enough.
        """
        scored = [
            (self._title_match_score(lines[idx], expected_title), idx)
            for idx in title_candidates
        ]
        best_score, best_idx = max(scored, default=(0, None))
        if best_idx is not None and best_score >= 3:
            return best_idx
        return None

    def _find_first_norm_content_index(self, compact_lines: list[str]) -> int | None:
        """Return the index of the first line that looks like norm body content.

        Matches ``Art.``, ``Capítulo``, ``Título``, ``Seção``, ``Anexo``, or
        ``PALÁCIO`` patterns.  Returns ``None`` when nothing matches.
        """
        return next(
            (
                idx
                for idx, compact in enumerate(compact_lines)
                if re.match(
                    "^(art\\.|cap[ií]tulo|t[ií]tulo|se[çc][aã]o|anexo\\b)",
                    compact.lower(),
                )
                or compact.upper().startswith("PALÁCIO")
            ),
            None,
        )

    def _select_best_title_candidate(
        self, compact_lines: list[str], title_candidates: list[int]
    ) -> int:
        """Pick the best SAPL-title candidate, preferring numbered ones before the first article.

        Candidates appearing after the first norm-body line are deprioritised.
        Among the remaining candidates, numbered ones (containing digits) are
        preferred over unnumbered ones.
        """
        first_norm = self._find_first_norm_content_index(compact_lines)
        if first_norm is not None:
            before = [idx for idx in title_candidates if idx <= first_norm]
            title_candidates = before or title_candidates
        numbered = [
            idx
            for idx in title_candidates
            if re.search("\\b\\d{1,4}\\b", compact_lines[idx])
        ]
        return numbered[-1] if numbered else title_candidates[-1]

    def _collect_title_candidates(self, compact_lines: list[str]) -> list[int]:
        """Return indices of lines matching ``_SAPL_TITLE_RE``."""
        return [
            idx
            for idx, compact in enumerate(compact_lines)
            if self._SAPL_TITLE_RE.match(compact)
        ]

    def _find_content_start(self, lines: list[str], expected_title: str | None) -> int:
        """Return the index of the first line of the actual norm content.

        Searches for the title line using exact match, fuzzy token-score, then
        falls back to the last numbered SAPL-title-RE candidate.  Returns 0 if
        no reliable start can be found.
        """
        normalized_expected = (
            normalize_title_text(expected_title) if expected_title else ""
        )
        compact_lines = [" ".join(line.split()) for line in lines]
        title_candidates = self._collect_title_candidates(compact_lines)
        if normalized_expected:
            exact = self._find_exact_title_match(compact_lines, normalized_expected)
            if exact is not None:
                return exact
            if title_candidates:
                fuzzy = self._find_fuzzy_title_match(
                    lines, expected_title, title_candidates
                )
                if fuzzy is not None:
                    return fuzzy
        if title_candidates:
            return self._select_best_title_candidate(compact_lines, title_candidates)
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
                (
                    lower.startswith(pattern)
                    for pattern in self._EXTRA_DOC_START_PATTERNS
                )
            ):
                break
            out.append(raw_line)
        return out

    def _clean_sapl_pdf_markdown(
        self, text_markdown: str, expected_title: str | None = None
    ) -> str:
        """Strip common SAPL / SEI verification footer noise from extracted PDFs."""
        normalized = (
            text_markdown.replace("\r\n", "\n")
            .replace("\r", "\n")
            .replace("\x0c", "\n")
            .replace("\u200b", "")
        )
        lines = self._strip_sei_noise(normalized.split("\n"))
        start = self._find_content_start(lines, expected_title)
        if start:
            lines = lines[start:]
        line_tuples = [(line, *norm_line(line)) for line in lines]
        filtered_tuples = self._filter_footer_blocks(line_tuples)
        lines = self._truncate_extra_documents(filtered_tuples)
        result = re.sub("\\n{3,}", "\n\n", "\n".join(lines))
        return result.strip()

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.subjects: dict[int, str] = {}
        self._id_to_type = {value: name for name, value in self.types.items()}
        self._page_size = 100

    async def setup(self) -> None:
        first = (await self.http.request(self._format_subjects_url())).json()
        pages = first.get("pagination", {}).get("total_pages", 1)
        for page in range(1, pages + 1):
            data = (
                first
                if page == 1
                else (await self.http.request(self._format_subjects_url(page))).json()
            )
            self.subjects.update(
                {item["id"]: item["assunto"] for item in data["results"]}
            )

    async def _get_docs_links(self, url: str, data: dict | None = None) -> list[dict]:
        if data is None:
            data = (await self.http.request(url)).json()
        refs = []
        for item in data["results"]:
            url = item.get("texto_integral") or urljoin(
                self.base_url, item.get("link_detail_backend", "")
            )
            ref = {
                "id": item["id"],
                "norm_number": item["numero"],
                "title": item["__str__"],
                "situation": self._infer_situation(item),
                "summary": item.get("ementa", ""),
                "subject": [self.subjects.get(s, "") for s in item.get("assuntos", [])],
                "date": item["data"],
                "origin": item.get("esfera_federacao"),
                "publication": item.get("veiculo_publicacao"),
                "type": self._resolve_type_name(item.get("tipo")),
                "year": int(item["ano"]),
                "document_url": url,
                "pdf_link": item.get("texto_integral"),
            }
            refs.append(ref)
        return refs

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        url = self._format_year_url(year)
        first = (await self.http.request(url)).json()

        async def fetch_page(page: int) -> list[dict]:
            return (
                await self._get_docs_links(url, first)
                if page == 1
                else await self._get_docs_links(self._format_year_url(year, page))
            )

        async for ref in self.paginate(
            fetch_page, first.get("pagination", {}).get("total_pages", 1)
        ):
            yield ref

    async def fetch(self, ref: dict) -> Document:
        if not ref.get("pdf_link"):
            raise ValueError("SAPL API returned no texto_integral attachment")
        errors = []
        for url in self._build_pdf_fetch_urls(ref["pdf_link"]):
            try:
                markdown, raw, ext = await self.download(url)
                markdown = self._clean_sapl_pdf_markdown(
                    markdown, expected_title=ref["title"]
                )
                return Document(
                    **{**ref, "text_markdown": markdown, "raw": raw, "raw_ext": ext}
                )
            except (HttpError, ValueError) as exc:
                errors.append(exc)
        raise ExceptionGroup("No SAPL attachment variant succeeded", errors)
