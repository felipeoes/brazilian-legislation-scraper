import re
from collections.abc import AsyncIterator
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from loguru import logger

from src.core.convert import infer_type_by_prefix
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

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
VALID_SITUATIONS = {"Em Vigor": 2}
INVALID_SITUATIONS = {
    "Declarada Inconstitucional": 4,
    "Declarada Insubisistente": 5,
    "Eficácia Suspensa": 7,
    "Não Emitida. Falha de Sequência.": 9,
    "Revogada": 3,
    "Tornado Sem Efeito": 10,
}
SITUATIONS = {**VALID_SITUATIONS, **INVALID_SITUATIONS}
_MANIFESTO_RE = re.compile("MANIFESTO\\s+DE\\s+ASSINATURAS", re.IGNORECASE)
_AUTH_FOOTER_RE = re.compile(
    "(?:^\\s*P[áa]gina\\s+\\d+(?:\\s+de\\s+\\d+)?\\s*$\\s*)?^\\s*Autenticar documento em https://www3\\.al\\.es\\.gov\\.br/autenticidade\\s*$[\\s\\S]{0,120}?com o identificador\\s+[0-9A-F]+\\s*,?\\s*[\\s\\S]{0,220}?ICP-Brasil\\.?(?:\\s*^\\s*P[áa]gina\\s+\\d+(?:\\s+de\\s+\\d+)?\\s*$)?",
    re.IGNORECASE | re.MULTILINE,
)
_PAGE_NUMBER_RE = re.compile("(?im)^\\s*P[áa]gina\\s+\\d+(?:\\s+de\\s+\\d+)?\\s*$")
_DISCLAIMER_RE = re.compile(
    "(?:(?<=\\n)|^)\\s*(?:Est[ea]|Ess[ea])\\s+texto\\s+n[aã]o\\s+substitui\\s+o\\s+publicado\\s+no\\s+(?:D\\.?\\s*P\\.?\\s*L\\.?|D\\.?\\s*O\\.?(?:\\s*E\\.?)?)\\s+de\\s+[^\\n.]+\\.?",
    re.IGNORECASE,
)
_WATERMARK_RE = re.compile(
    "P[ÁáAa]GINA\\s+\\d+\\s*/\\s*\\d+\\s+PARA\\s+VERIFICAR\\s+A\\s+AUTENTICIDADE\\s+DESTE\\s+DOCUMENTO.*?(?:https?://\\S+)",
    re.IGNORECASE | re.DOTALL,
)


def _collapse_blank_lines(text: str) -> str:
    """Collapse excessive blank lines introduced by artifact removal."""
    return re.sub("\\n{3,}", "\n\n", text).strip()


def _strip_summary(text_markdown: str, summary: str) -> str:
    """Remove the listing-page summary from the beginning of *text_markdown* if present.

    The summary scraped from the search-results listing is often the opening
    sentence of the document.  Stripping it avoids storing it twice.
    """
    if not summary or not text_markdown:
        return text_markdown
    words = summary.strip().split()
    if not words:
        return text_markdown
    pattern = "\\s+".join((re.escape(w) for w in words))
    match = re.search(pattern, text_markdown, re.IGNORECASE)
    if not match:
        return text_markdown
    prefix = text_markdown[: match.start()]
    prefix_lines = [line.strip() for line in prefix.splitlines() if line.strip()]
    prefix_substantive = re.sub("[\\W_]+", "", prefix, flags=re.UNICODE)
    if (
        match.start() < 300
        and len(prefix_lines) <= 3
        and (len(prefix_substantive) <= 120)
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


class ESAlesScraper(BaseScraper):
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

    name = "ESPIRITO_SANTO"
    types = TYPES
    situations = {}
    base_url = "https://www3.al.es.gov.br"

    def _format_search_url(self, year: int) -> str:
        """Format year-only search URL (returns all types and situations combined)."""
        return (
            f"{self.base_url}/legislacao/consulta-legislacao.aspx?ano={year}&interno=1"
        )

    @staticmethod
    def _infer_norm_type(title: str) -> str:
        return infer_type_by_prefix(title, list(TYPES))

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
            norm_type = next(
                (line.strip() for line in title_raw.splitlines() if line.strip()), ""
            )
            title = re.sub("\\r\\n +", " ", title_raw.strip())
            if not norm_type:
                norm_type = self._infer_norm_type(title)
            summary_tag = item.find("a", class_="kt-widget5__desc")
            summary = summary_tag.text.strip() if summary_tag else ""
            info_divs = item.find_all("div", class_="kt-widget5__info")
            info_spans = (
                info_divs[0].find_all("span", class_="kt-font-info")
                if info_divs
                else []
            )
            date = info_spans[0].text.strip() if info_spans else ""
            situation = info_spans[1].text.strip() if len(info_spans) > 1 else ""
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
        resp = await self.http.request(
            url, method="POST" if post_data is not None else "GET", data=post_data
        )
        try:
            content = resp.content
        except Exception:
            logger.exception(f"Failed to read response body for {url}")
            return (None, None, None)
        soup = BeautifulSoup(content, "lxml")
        vs = soup.find(id="__VIEWSTATE")
        ev = soup.find(id="__EVENTVALIDATION")
        return (content, vs["value"] if vs else None, ev["value"] if ev else None)

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
            url, viewstate, eventvalidation, "ctl00$ContentPlaceHolder1$lbNext"
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
            m = re.search("(\\d+)", tag.text.replace(".", ""))
            if m:
                return int(m.group(1))
        return None

    async def fetch(self, doc_info: dict) -> Document | None:
        """Get document data from document link"""
        doc_info = dict(doc_info)
        doc_link = doc_info.pop("doc_link")
        inferred_type = doc_info.pop("norm_type", "") or self._infer_norm_type(
            doc_info.get("title", "")
        )
        if inferred_type:
            doc_info["type"] = inferred_type
        summary = doc_info.get("summary", "")
        url = urljoin(self.base_url, doc_link)
        if url.endswith(".pd"):
            url = url + "f"
        if "Processo2/Digital.aspx" in url:
            params = parse_qs(urlparse(url).query)
            arquivo = params.get("arquivo", [""])[0]
            if arquivo.lower().endswith(".pdf"):
                url = urljoin(self.base_url, "/" + arquivo.lstrip("/"))
            else:
                logger.info(f"Skipping non-PDF Digital.aspx URL: {url}")
                return None
        if "/DiariosPDF/" in url:
            logger.info(f"Skipping diary PDF: {url}")
            return None
        if url.endswith(".pdf"):
            text_markdown, raw, content_ext = await self.download(url)
            cleaned = _clean_markdown(text_markdown, summary)
            doc_info["text_markdown"] = cleaned
            if not doc_info["text_markdown"] or not doc_info["text_markdown"].strip():
                raise ValueError("Markdown empty after ES-specific cleanup")
            doc_info["document_url"] = url
            doc_info["raw"] = raw
            doc_info["raw_ext"] = content_ext
            return Document(**doc_info)
        soup = (await self.http.request(url)).soup()
        if not soup:
            raise ValueError("Failed to fetch HTML document")
        html_string = str(soup)
        text_markdown = await self.markdown(html=html_string)
        result = Document(
            **{
                **doc_info,
                "document_url": url,
                "text_markdown": text_markdown,
                "raw": html_string.encode("utf-8"),
                "raw_ext": ".html",
            }
        )
        if result:
            cleaned = _clean_markdown(result.text_markdown, summary)
            if not cleaned or not cleaned.strip():
                raise ValueError("Markdown empty after ES-specific cleanup")
            result.text_markdown = cleaned
        return result

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        url = self._format_search_url(year)
        content, vs, ev = await self._fetch_first_page(url)
        if not content or not vs or (not ev):
            raise ValueError("Missing Espírito Santo form state")
        content, vs, ev = await self._fetch_postback(
            url,
            vs,
            ev,
            "ctl00$ContentPlaceHolder1$ddl_ItensExibidos",
            items_per_page="100",
        )
        while True:
            if not content or not vs or (not ev):
                raise ValueError("Missing Espírito Santo result state")
            soup = BeautifulSoup(content, "lxml")
            for ref in self._parse_docs_from_soup(soup):
                yield {
                    "year": year,
                    **ref,
                    "situation": ref.get("situation") or DEFAULT_VALID_SITUATION,
                    "document_url": urljoin(self.base_url, ref["doc_link"]),
                }
            if not self._has_next_page(soup):
                break
            content, vs, ev = await self._fetch_next_page(url, vs, ev)
