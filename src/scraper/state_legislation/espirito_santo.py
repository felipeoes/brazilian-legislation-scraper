from __future__ import annotations

import math
import re
from io import BytesIO
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from loguru import logger
from tqdm.asyncio import tqdm

from src.scraper.base.schemas import ScrapedDocument
from src.scraper.base.scraper import StateScraper

TYPES = {
    "Acórdão do Colegiado da Procuradoria": 18,
    "Ato Administrativo": 10,
    "Constituição Estadual": 1,
    "Decreto Executivo": 12,
    "Decreto Legislativo": 5,
    "Decreto Normativo": 6,
    "Decreto Regulamentar": 9,
    "Decreto Suplementar": 11,
    "Emenda à Constituição Estadual": 2,
    "Lei Complementar": 4,
    "Lei Delegada": 8,
    "Lei Ordinária": 3,
    "Resolução": 7,
}

VALID_SITUATIONS = {
    "Em Vigor": 2,
}

INVALID_SITUATIONS = {
    "Declarada Inconstitucional": 4,
    "Declarada Insubisistente": 5,
    "Eficácia Suspensa": 7,
    "Não Emitida. Falha de Sequência.": 9,
    "Revogada": 3,
    "Tornado Sem Efeito": 10,
}  # norms with these situations are invalid norms (no longer have legal effect)

SITUATIONS = {**VALID_SITUATIONS, **INVALID_SITUATIONS}

# Matches the digital-signature manifest block that always appears at the end of
# ES Assembly PDFs.  Everything from this marker onward is stripped.
_MANIFESTO_RE = re.compile(r"MANIFESTO\s+DE\s+ASSINATURAS", re.IGNORECASE)

# Matches the authentication footer emitted by the ES Assembly viewer. This block
# may wrap across multiple lines and, in some PDFs, straddle a page boundary.
_AUTH_FOOTER_RE = re.compile(
    r"(?:^\s*P[áa]gina\s+\d+(?:\s+de\s+\d+)?\s*$\s*)?"
    r"^\s*Autenticar documento em https://www3\.al\.es\.gov\.br/autenticidade\s*$"
    r"[\s\S]{0,120}?com o identificador\s+[0-9A-F]+\s*,?\s*"
    r"[\s\S]{0,220}?ICP-Brasil\.?"
    r"(?:\s*^\s*P[áa]gina\s+\d+(?:\s+de\s+\d+)?\s*$)?",
    re.IGNORECASE | re.MULTILINE,
)

# Matches standalone page-number lines left by PDF extraction.
_PAGE_NUMBER_RE = re.compile(r"(?im)^\s*P[áa]gina\s+\d+(?:\s+de\s+\d+)?\s*$")

# Matches editorial disclaimer lines that appear in ES HTML exports.
_DISCLAIMER_RE = re.compile(
    r"(?:(?<=\n)|^)\s*(?:Est[ea]|Ess[ea])\s+texto\s+n[aã]o\s+substitui\s+"
    r"o\s+publicado\s+no\s+(?:D\.?\s*P\.?\s*L\.?|D\.?\s*O\.?(?:\s*E\.?)?)"
    r"\s+de\s+[^\n.]+\.?",
    re.IGNORECASE,
)

# Matches the "PÁGINA X / Y PARA VERIFICAR A AUTENTICIDADE …" watermark that
# appears on some ES Assembly PDF pages.
_WATERMARK_RE = re.compile(
    r"P[ÁáAa]GINA\s+\d+\s*/\s*\d+\s+PARA\s+VERIFICAR\s+A\s+AUTENTICIDADE"
    r"\s+DESTE\s+DOCUMENTO.*?(?:https?://\S+)",
    re.IGNORECASE | re.DOTALL,
)


def _collapse_blank_lines(text: str) -> str:
    """Collapse excessive blank lines introduced by artifact removal."""
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _strip_summary(text_markdown: str, summary: str) -> str:
    """Remove the listing-page summary from the beginning of *text_markdown* if present.

    The summary scraped from the search-results listing is often the opening
    sentence of the document.  Stripping it avoids storing it twice.
    """
    if not summary or not text_markdown:
        return text_markdown
    # Escape each word individually and join with \s+ so the pattern is flexible
    # about whitespace (line-breaks, multiple spaces, etc.).
    words = summary.strip().split()
    if not words:
        return text_markdown
    pattern = r"\s+".join(re.escape(w) for w in words)
    match = re.search(pattern, text_markdown, re.IGNORECASE)
    if not match:
        return text_markdown

    prefix = text_markdown[: match.start()]
    prefix_lines = [line.strip() for line in prefix.splitlines() if line.strip()]
    prefix_substantive = re.sub(r"[\W_]+", "", prefix, flags=re.UNICODE)

    # Be conservative: ES Ales listings sometimes use a long excerpt from the
    # first operative paragraph as the "summary". Strip only when the match is
    # genuinely at the beginning of the document and the preceding text is
    # limited to a short title/header block.
    if (
        match.start() < 300
        and len(prefix_lines) <= 3
        and len(prefix_substantive) <= 120
    ):
        return text_markdown[match.end() :].lstrip()
    return text_markdown


def _clean_markdown(text_markdown: str, summary: str = "") -> str:
    """Remove ES-specific PDF/HTML artefacts and strip the listing summary.

    pymupdf4llm already strips page-number footers, authentication watermarks,
    and sidebar text that the old PyMuPDF plain-text extractor used to include.
    The only ES-specific artefacts that survive are:

    * The "MANIFESTO DE ASSINATURAS" digital-signature block at the end of PDFs.
    * Authentication footer / page-number / disclaimer lines that may appear in
      HTML→markdown output from the ES Assembly website.
    """
    match = _MANIFESTO_RE.search(text_markdown)
    if match:
        text_markdown = text_markdown[: match.start()].strip()

    text = _AUTH_FOOTER_RE.sub("", text_markdown)
    text = _WATERMARK_RE.sub("", text)
    text = _PAGE_NUMBER_RE.sub("", text)
    text = _DISCLAIMER_RE.sub("", text)
    text = _collapse_blank_lines(text)
    return _strip_summary(text, summary)


class ESAlesScraper(StateScraper):
    """Webscraper for Espirito Santo state legislation website (https://www3.al.es.gov.br/legislacao)

    Year start (earliest on source): 1958

    Example search request: https://www3.al.es.gov.br/legislacao/consulta-legislacao.aspx?ano=2000&interno=1

    Document types:
      - "TEXTO COMPLETO" links (.html) — static Word HTML exports served at
        /Arquivo/Documents/legislacao/html/. All content is in the initial HTTP
        response (zero AJAX), so these are fetched via plain aiohttp (get_soup),
        not Playwright.
      - "NORMA ORIGINAL" links (.pdf) — scanned PDFs at
        /Arquivo/Documents/legislacao/image/. Downloaded and converted via
        pymupdf4llm with LLM OCR fallback.
      - "Digital.aspx" links — document viewer URLs; the embedded PDF path is
        extracted from the ``arquivo`` query param and fetched as a regular PDF.

    Skipped document types:
      - URLs containing ``/DiariosPDF/`` are skipped entirely. These are Official
        Gazette (Diário Oficial) edition PDFs that bundle many norms into a single
        file; they do not represent an individual legislative document and would
        pollute the dataset with unrelated content.
    """

    def __init__(
        self,
        base_url: str = "https://www3.al.es.gov.br",
        **kwargs,
    ):
        super().__init__(
            base_url,
            name="ESPIRITO_SANTO",
            types=TYPES,
            situations={},
            **kwargs,
        )

    def _format_search_url(self, year: int) -> str:
        """Format year-only search URL (returns all types and situations combined)."""
        return (
            f"{self.base_url}/legislacao/consulta-legislacao.aspx?ano={year}&interno=1"
        )

    @staticmethod
    def _infer_norm_type(title: str) -> str:
        """Infer a norm type from the listing title when the first line is missing."""
        normalized_title = " ".join(title.split())
        for type_name in sorted(TYPES, key=len, reverse=True):
            if normalized_title.casefold().startswith(type_name.casefold()):
                return type_name
        return ""

    def _parse_docs_from_soup(self, soup: BeautifulSoup) -> list[dict]:
        """Parse document entries from a listing page soup."""
        docs = []
        container = soup.find("div", class_="kt-portlet__body")
        if not container:
            return docs

        items = container.find_all("div", class_="kt-widget5__item")
        for item in items:
            title_tag = item.find("a", class_="kt-widget5__title")
            if not title_tag:
                continue
            title_raw = title_tag.text
            # norm_type is first non-empty line of title (handles \r\n line endings)
            norm_type = next(
                (line.strip() for line in title_raw.splitlines() if line.strip()), ""
            )

            title = re.sub(r"\r\n +", " ", title_raw.strip())
            if not norm_type:
                norm_type = self._infer_norm_type(title)

            summary_tag = item.find("a", class_="kt-widget5__desc")
            summary = summary_tag.text.strip() if summary_tag else ""

            # date and situation are the two kt-font-info spans in the first info div
            info_divs = item.find_all("div", class_="kt-widget5__info")
            info_spans = (
                info_divs[0].find_all("span", class_="kt-font-info")
                if info_divs
                else []
            )
            date = info_spans[0].text.strip() if info_spans else ""
            situation = info_spans[1].text.strip() if len(info_spans) > 1 else ""

            # authors from the second kt-widget5__info div
            authors = ""
            if len(info_divs) > 1:
                author_span = info_divs[1].find("span", class_="kt-font-info")
                if author_span:
                    authors = author_span.text.strip()

            doc_link_tags = item.find_all("a", class_="btn-label-info")
            if not doc_link_tags:
                continue
            doc_link = doc_link_tags[0]["href"]

            if "processo.aspx?" in doc_link:
                logger.info(
                    f"Skipping '{title_raw.strip()}' since it is a process link"
                )
                continue

            if doc_link.endswith(".docx"):
                logger.info(
                    f"Skipping '{title_raw.strip()}' since it is a docx document"
                )
                continue

            docs.append(
                {
                    "title": title,
                    "norm_type": norm_type,
                    "situation": situation,
                    "summary": summary,
                    "date": date,
                    "authors": authors,
                    "doc_link": doc_link,
                }
            )

        return docs

    def _has_next_page(self, soup: BeautifulSoup) -> bool:
        """Return True if a functional lbNext button exists on the page."""
        btn = soup.find(id="ContentPlaceHolder1_lbNext")
        return bool(btn) and "aspNetDisabled" not in (btn.get("class") or [])

    async def _fetch_state_page(
        self, url: str, post_data: dict | None = None
    ) -> tuple[bytes | None, str | None, str | None]:
        """Fetch a page (GET or POST) and return (content, viewstate, eventvalidation)."""
        resp = await self.request_service.make_request(
            url,
            method="POST" if post_data is not None else "GET",
            payload=post_data,
        )
        if not resp:
            return None, None, None
        try:
            content = await resp.read()
        except Exception:
            logger.exception(f"Failed to read response body for {url}")
            return None, None, None
        soup = BeautifulSoup(content, "html.parser")
        vs = soup.find(id="__VIEWSTATE")
        ev = soup.find(id="__EVENTVALIDATION")
        return content, vs["value"] if vs else None, ev["value"] if ev else None

    async def _fetch_first_page(
        self, url: str
    ) -> tuple[bytes | None, str | None, str | None]:
        """GET the first page and return (content, viewstate, eventvalidation)."""
        return await self._fetch_state_page(url)

    async def _fetch_next_page(
        self, url: str, viewstate: str, eventvalidation: str
    ) -> tuple[bytes | None, str | None, str | None]:
        """POST the lbNext button to advance one page."""
        return await self._fetch_postback(
            url,
            viewstate,
            eventvalidation,
            "ctl00$ContentPlaceHolder1$lbNext",
        )

    async def _fetch_postback(
        self,
        url: str,
        viewstate: str,
        eventvalidation: str,
        target: str,
        arg: str = "",
        items_per_page: str = "100",
    ) -> tuple[bytes | None, str | None, str | None]:
        """POST a generic target and return (content, viewstate, eventvalidation)."""
        return await self._fetch_state_page(
            url,
            {
                "__EVENTTARGET": target,
                "__EVENTARGUMENT": arg,
                "__VIEWSTATE": viewstate,
                "__EVENTVALIDATION": eventvalidation,
                "ctl00$ContentPlaceHolder1$ddl_ItensExibidos": items_per_page,
            },
        )

    def _extract_total_count(self, soup: BeautifulSoup) -> int | None:
        """Extract total result count from the listing page."""
        tag = soup.find(id="ContentPlaceHolder1_contagem")
        if tag:
            m = re.search(r"(\d+)", tag.text.replace(".", ""))
            if m:
                return int(m.group(1))
        return None

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape all norms for a year using sequential Next-button pagination."""
        url = self._format_search_url(year)

        content, vs, ev = await self._fetch_first_page(url)
        if not content or not vs or not ev:
            return []

        # Switch to 100 items per page to minimize requests
        content, vs, ev = await self._fetch_postback(
            url,
            vs,
            ev,
            "ctl00$ContentPlaceHolder1$ddl_ItensExibidos",
            items_per_page="100",
        )
        if not content or not vs or not ev:
            return []

        soup = BeautifulSoup(content, "html.parser")
        total_count = self._extract_total_count(soup)
        total_pages = math.ceil(total_count / 100) if total_count else 1
        logger.info(
            f"ESPIRITO_SANTO | {year} | {total_count or '?'} results, ~{total_pages} pages"
        )

        all_docs = self._parse_docs_from_soup(soup)

        # Sequential pagination with progress bar
        page_progress = tqdm(
            total=total_pages,
            initial=1,
            desc=f"ESPIRITO_SANTO | {year} | Pages",
        )
        try:
            while self._has_next_page(soup):
                content, vs, ev = await self._fetch_next_page(url, vs, ev)
                if not content or not vs or not ev:
                    break
                soup = BeautifulSoup(content, "html.parser")
                all_docs.extend(self._parse_docs_from_soup(soup))
                page_progress.update()
        finally:
            page_progress.close()

        for doc in all_docs:
            doc["year"] = year

        return await self._process_documents(
            all_docs,
            year=year,
            norm_type="all",
            situation="all",
            desc=f"ESPIRITO_SANTO | {year}",
        )

    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        """Get document data from document link"""
        doc_info = dict(doc_info)
        doc_link = doc_info.pop("doc_link")
        # Use the actual norm type from the listing (not the generic "all" context value).
        inferred_type = doc_info.pop("norm_type", "") or self._infer_norm_type(
            doc_info.get("title", "")
        )
        if inferred_type:
            doc_info["type"] = inferred_type

        # Grab summary before discarding it; used to strip it from text_markdown.
        summary = doc_info.pop("summary", "")
        url = urljoin(self.base_url, doc_link)

        # some urls are malformed, e.g., they ends with .pd instead of .pdf
        if url.endswith(".pd"):
            url = url + "f"

        # Resolve Processo2/Digital.aspx document-viewer URLs to the embedded PDF path.
        # Pattern: .../Processo2/Digital.aspx?id=NNN&arquivo=Arquivo/Documents/.../x.pdf&...
        if "Processo2/Digital.aspx" in url:
            params = parse_qs(urlparse(url).query)
            arquivo = params.get("arquivo", [""])[0]
            if arquivo.lower().endswith(".pdf"):
                url = urljoin(self.base_url, "/" + arquivo.lstrip("/"))
            else:
                logger.info(f"Skipping non-PDF Digital.aspx URL: {url}")
                return None

        # Skip Official Gazette edition PDFs — they bundle many norms into one file.
        if "/DiariosPDF/" in url:
            logger.info(f"Skipping diary PDF: {url}")
            return None

        if self._is_already_scraped(url, doc_info.get("title", "")):
            return None

        # if url ends with .pdf, download, clean, and fall back to LLM OCR if too short
        if url.endswith(".pdf"):
            text_markdown, raw_content, content_ext = await self._download_and_convert(
                url
            )
            cleaned = _clean_markdown(text_markdown, summary)

            if len(cleaned.strip()) < 200:
                ocr_md = await self._get_markdown(
                    stream=BytesIO(raw_content), filename=url
                )
                cleaned = _clean_markdown(ocr_md, summary)

                if len(cleaned.strip()) < 100:
                    await self._save_doc_error(
                        title=doc_info.get("title", ""),
                        year=doc_info.get("year", ""),
                        html_link=url,
                        error_message="PDF content too short after OCR fallback",
                        content=cleaned,
                    )
                    return None

            doc_info["text_markdown"] = cleaned
            if not doc_info["text_markdown"] or not doc_info["text_markdown"].strip():
                await self._save_doc_error(
                    title=doc_info.get("title", ""),
                    year=doc_info.get("year", ""),
                    html_link=url,
                    error_message="Markdown empty after ES-specific cleanup",
                )
                return None
            doc_info["document_url"] = url
            doc_info["raw_content"] = raw_content
            doc_info["content_extension"] = content_ext

            return ScrapedDocument(**doc_info)

        soup = await self.request_service.get_soup(url)
        if not soup:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=url,
                error_message="Failed to fetch HTML document",
            )
            return None

        # Remove images — alt text from decorative logos pollutes the markdown
        for img in soup.find_all("img"):
            img.decompose()
        html_string = str(soup)

        text_markdown = await self._get_markdown(html_content=html_string)
        result = await self._process_doc(
            doc_info,
            url,
            text_markdown,
            html_string.encode("utf-8"),
            ".html",
            error_prefix="Invalid markdown",
        )
        if result:
            cleaned = _clean_markdown(result.text_markdown, summary)
            if not cleaned or not cleaned.strip():
                await self._save_doc_error(
                    title=doc_info.get("title", ""),
                    year=doc_info.get("year", ""),
                    html_link=url,
                    error_message="Markdown empty after ES-specific cleanup",
                )
                return None
            result.text_markdown = cleaned
        return result
