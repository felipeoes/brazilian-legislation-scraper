from __future__ import annotations
from typing import TYPE_CHECKING
import re
import urllib.parse
from io import BytesIO

from bs4 import BeautifulSoup
from loguru import logger

from src.scraper.base.converter import strip_html_chrome, valid_markdown
from src.scraper.base.scraper import (
    DEFAULT_INVALID_SITUATION,
    DEFAULT_VALID_SITUATION,
    BaseScraper,
    flatten_results,
)
from src.scraper.base.schemas import ScrapedDocument

if TYPE_CHECKING:
    pass

# Kept for documentation / downstream reference — not used to filter API requests.
# The unfiltered API already returns id_tipo_ato + nomeato on every row.
TYPES = {
    "Resolução": 1,
    "Moção": 2,
    "Recomendação": 3,
    "Proposição": 4,
    "Decisão": 5,
    "Portaria": 6,
}


class ConamaScraper(BaseScraper):
    """Webscraper for Conama (Conselho Nacional do Meio Ambiente) website (https://conama.mma.gov.br/atos-normativos-sistema)

    Year start (earliest on source): 1984

    The API returns all norm types in a single unfiltered request per year —
    each row already carries ``id_tipo_ato`` and ``nomeato`` — so no per-type
    iteration is needed.

    Example search request (no tipo filter):
    https://conama.mma.gov.br/?option=com_sisconama&order=asc&offset=0&limit=100&task=atosnormativos.getList&ano=2000

    Observation: Conama does not have a situation field; invalid norms will
    have an indication in the document text (status field).
    """

    def __init__(
        self,
        base_url: str = "https://conama.mma.gov.br/",
        **kwargs,
    ):
        # Pass types for display/logging only; situations unused by this scraper.
        super().__init__(
            base_url,
            name="CONAMA",
            types=TYPES,
            situations={},
            **kwargs,
        )
        self.params = {
            "option": "com_sisconama",
            "order": "asc",
            "limit": 100,
        }
        self._situation_regex = re.compile(r"Revogad|Revogação", re.IGNORECASE)

    def _format_search_url(self, offset: int = 0, year: str | None = None) -> str:
        """Format URL for the unfiltered norm-list endpoint."""
        return (
            f"{self.base_url}?option={self.params['option']}"
            f"&order={self.params['order']}"
            f"&offset={offset}&limit={self.params['limit']}"
            f"&task=atosnormativos.getList&ano={year}"
        )

    _DOU_GARBAGE_STRINGS = [
        "DOU - Imprensa Nacional",
        "DIÁRIO OFICIAL DA UNIÃO",
        "Imprensa Nacional",
    ]

    _DISCLAIMER_PATTERNS = [
        re.compile(r"Est[ea] conte.do não substitui", re.IGNORECASE),
        re.compile(r"Est[ea] texto não substitui", re.IGNORECASE),
        re.compile(r"Ess[ea] texto não substitui", re.IGNORECASE),
        re.compile(r"\*?Obs:?\*?\*?\s*não há registro no sítio", re.IGNORECASE),
    ]

    _PDF_CLEANUP_PATTERNS: list[tuple[str, str, int]] = [
        (r"\x0c", "", 0),
        # SEI artefacts
        (r"Ato \d+ \(\d+\)\s+SEI \S+ / pg\. \d+", "", 0),
        (r"Documento assinado eletronicamente por\s+.+?de 2020\s*\.?", "", re.DOTALL),
        (
            r"A autenticidade deste documento pode ser conferida no site\s+.+?código CRC \w+\.?",
            "",
            re.DOTALL,
        ),
        (r"Refer.ncia: Processo n. \S+\s*\n*SEI n. \d+", "", 0),
        # DOU artefacts (DOU web pages saved as PDF)
        (r"\d{2}/\d{2}/\d{4},\s*\d{2}:\d{2}\s*\n*", "", 0),
        (r"^.+?- DOU - Imprensa Nacional\s*\n*", "", re.MULTILINE),
        (r"DI.RIO OFICIAL DA UNI.O\s*\n*", "", 0),
        (r"Publicado em:.*?P.gina:.*?\n*", "", re.DOTALL),
        (r".rg.o:.*?Meio Ambiente.*?\n+", "", 0),
        (r"https?://www\.in\.gov\.br/\S+", "", 0),
        (r"\n\d+/\d+\s*$", "\n", re.MULTILINE),
        # Leading page number left by PDF extraction — must run last so
        # earlier patterns (e.g. DOU date stamp) can expose it first.
        (r"^\s*\d{1,4}\s*\n+", "", 0),
    ]

    def _clean_dou_html(self, soup: BeautifulSoup) -> BeautifulSoup:
        """Remove DOU website chrome from HTML before markdown conversion."""
        strip_html_chrome(soup)

        # Remove <title> tag (contains duplicated title + "DOU - Imprensa Nacional")
        for el in soup.find_all("title"):
            el.decompose()

        # Single pass: garbage-string check + pattern matching combined.
        # Only inspect leaf-like elements (no child tags) to avoid decomposing
        # ancestor nodes (e.g. <body>) whose aggregate text happens to be short.
        for el in soup.find_all(True):
            if el.decomposed:
                continue
            if el.find(True):  # has child tag elements — skip container nodes
                continue
            text = el.get_text(strip=True)

            # Garbage-string check
            for garbage in self._DOU_GARBAGE_STRINGS:
                if garbage in text and len(text) < 300:
                    el.decompose()
                    break
            else:
                # Pattern matching (only if not already decomposed above)
                if re.match(r"^Publicado em:\s*\d", text):
                    el.decompose()
                    continue
                if re.match(r"^Órgão:\s*", text):
                    el.decompose()
                    continue
                if re.match(r"^\d{2}/\d{2}/\d{4},\s*\d{2}:\d{2}$", text):
                    el.decompose()
                    continue
                if re.match(r"^\d+/\d+$", text):
                    el.decompose()
                    continue
                for pat in self._DISCLAIMER_PATTERNS:
                    if pat.search(text) and len(text) < 200:
                        el.decompose()
                        break

        for el in soup.find_all("a"):
            if el.decomposed:
                continue
            href = str(el.get("href") or "")
            if "in.gov.br" in href:
                el.decompose()
            else:
                el.unwrap()

        return soup

    def _clean_pdf_markdown(self, text: str) -> str:
        """Clean SEI/PDF-specific artefacts from extracted text.

        Does NOT call ``_clean_markdown`` at the end — ``_get_markdown``
        already applies it on the way out, so calling it again here would
        be a redundant second pass.
        """
        for pattern, replacement, flags in self._PDF_CLEANUP_PATTERNS:
            text = re.sub(pattern, replacement, text, flags=flags)
        for pat in self._DISCLAIMER_PATTERNS:
            text = pat.sub("", text)
        return text.strip()

    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        """Fetch and convert a single CONAMA norm.

        Download URL pattern:
        https://conama.mma.gov.br/?option=com_sisconama&task=arquivo.download&id={id}
        """
        doc_id = doc_info.get("aid")
        doc_number = doc_info.get("numero", "")
        doc_type = doc_info.get("nomeato", "")
        doc_year = doc_info.get("ano", "")

        title = f"{doc_type} CONAMA Nº {doc_number}/{doc_year}"

        # Early-exit for missing ID before any URL construction or resume check
        if doc_id is None:
            logger.info(
                f"Skipping {title} as it has no document ID attached (aid is null)."
            )
            return None

        doc_url = urllib.parse.urljoin(
            self.base_url,
            f"?option=com_sisconama&task=arquivo.download&id={doc_id}",
        )

        if self._is_already_scraped(doc_url, title):
            return None

        doc_description = doc_info.get("descricao", "")
        doc_status = doc_info.get("status")
        doc_keyword = doc_info.get("palavra_chave", "")
        doc_origin = doc_info.get("porigem", "")

        # Fetch the document once; detect content-type to pick HTML vs PDF path
        resp = await self.request_service.make_request(doc_url)
        if not resp:
            logger.warning(
                f"Failed to fetch document for {doc_type} CONAMA Nº {doc_number}/{doc_year}"
            )
            await self._save_doc_error(
                title=title,
                year=doc_year,
                norm_type=doc_type,
                html_link=doc_url,
                error_message="Failed to fetch document URL",
            )
            return None

        content_type = (resp.content_type or "").lower()
        is_html = "html" in content_type

        if is_html:
            try:
                soup, mhtml = await self._fetch_soup_and_mhtml(doc_url)
            except Exception as exc:
                logger.warning(
                    f"Browser fetch failed for {doc_type} CONAMA Nº {doc_number}/{doc_year}: {exc}"
                )
                await self._save_doc_error(
                    title=title,
                    year=doc_year,
                    norm_type=doc_type,
                    html_link=doc_url,
                    error_message=f"Browser fetch failed: {exc}",
                )
                return None
            soup = self._clean_dou_html(soup)
            # str(soup) is compact and avoids the prettify() indentation overhead.
            # _get_markdown already applies _clean_markdown internally — no second call.
            text_markdown = await self._get_markdown(html_content=str(soup))
            raw_content = mhtml
            content_ext = ".mhtml"
        else:
            body = await resp.read()
            # _get_markdown applies _clean_markdown internally; _clean_pdf_markdown
            # applies only the PDF-specific regex passes (no redundant second clean).
            text_markdown = await self._get_markdown(stream=BytesIO(body))
            text_markdown = self._clean_pdf_markdown(text_markdown)
            raw_content = body
            content_ext = ".pdf"

        text_markdown = text_markdown.strip() if text_markdown else text_markdown

        if text_markdown is None or not text_markdown.strip():
            logger.warning(
                f"Empty markdown for {doc_type} CONAMA Nº {doc_number}/{doc_year}"
            )
            await self._save_doc_error(
                title=title,
                year=doc_year,
                norm_type=doc_type,
                html_link=doc_url,
                error_message="Empty markdown after conversion",
            )
            return None

        situation = DEFAULT_VALID_SITUATION
        if doc_status and self._situation_regex.search(doc_status):
            situation = DEFAULT_INVALID_SITUATION

        # _valid_markdown also catches server-error strings (via _SERVER_ERROR_PATTERNS)
        # so no separate PHP-error check is needed here.
        is_valid, reason = valid_markdown(text_markdown, 200)
        if not is_valid:
            logger.warning(
                f"Markdown text for {doc_type} CONAMA Nº {doc_number}/{doc_year} "
                f"is invalid: {reason}. Length: {len(text_markdown) if text_markdown else 0} chars."
            )
            await self._save_doc_error(
                title=title,
                year=doc_year,
                situation=situation,
                norm_type=doc_type,
                html_link=doc_url,
                error_message="Markdown text very short after cleaning, may indicate conversion issues",
            )
            return None

        return ScrapedDocument(
            year=doc_year,
            title=title,
            type=doc_type,  # from nomeato — each doc carries its own type
            id=doc_id,
            number=doc_number,
            summary=doc_description,
            situation=situation,
            keyword=doc_keyword,
            origin=doc_origin,
            text_markdown=text_markdown,
            document_url=doc_url,
            raw_content=raw_content,
            content_extension=content_ext,
        )

    async def _fetch_page_norms(self, offset: int, year_str: str) -> list[dict]:
        """Fetch norms from a single pagination page."""
        url = self._format_search_url(offset=offset, year=year_str)
        response = await self.request_service.make_request(url)
        if not response:
            return []
        try:
            json_response = await response.json(content_type=None)
            return json_response["data"]["rows"]
        except (KeyError, ValueError, TypeError) as e:
            logger.error(
                f"Failed to parse pagination JSON for year {year_str} offset {offset}: {e}"
            )
            return []

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape all norms for a given year in a single unfiltered API request.

        The CONAMA API returns every norm type for the requested year in one
        shot; ``nomeato`` on each row carries the type name.  No per-type
        iteration is needed.
        """
        year_str = str(year)
        url = self._format_search_url(offset=0, year=year_str)

        response = await self.request_service.make_request(url)
        if not response:
            return []

        try:
            json_response = await response.json(content_type=None)
            data = json_response["data"]
            total_norms = data["total"]
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Failed to parse JSON for year {year_str}: {e}")
            return []

        norms = list(data["rows"])
        limit = self.params["limit"]

        # Fetch any additional pages in parallel (guard for future years)
        if total_norms > limit:
            offsets = list(range(limit, total_norms, limit))
            page_tasks = [
                self._fetch_page_norms(offset, year_str) for offset in offsets
            ]
            page_results = await self._gather_results(
                page_tasks,
                context={"year": year_str, "type": "NA", "situation": "NA"},
                desc=f"CONAMA | {year_str} | pagination",
            )
            norms.extend(flatten_results(page_results))

        return await self._process_documents(
            norms,
            year=year,
            norm_type="NA",  # each doc sets its own "type" from nomeato
            situation="Não consta",
            desc=f"CONAMA | {year_str}",
        )
