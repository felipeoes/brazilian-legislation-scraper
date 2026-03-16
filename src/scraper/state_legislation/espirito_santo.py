from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument

from io import BytesIO
from urllib.parse import parse_qs, urljoin, urlparse

import re
from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.converter import valid_markdown
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

# Matches runs of 15+ consecutive lines with ≤2 characters — the digital-authentication
# sidebar watermark that PyMuPDF extracts as single-character lines when text is rotated.
# Threshold is set high (15) to avoid false-positive removal of legitimate short lines.
_VERTICAL_WATERMARK_RE = re.compile(r"(?:\n[^\n]{0,2}){15,}")


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
    # Only strip when the match is close to the start of the document.
    if match and match.start() < 300:
        return text_markdown[match.end() :].lstrip()
    return text_markdown


def _clean_markdown(text_markdown: str, summary: str = "") -> str:
    """Remove watermark garbage and strip the listing summary from *text_markdown*."""
    text = _VERTICAL_WATERMARK_RE.sub("", text_markdown).strip()
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
        markitdown with LLM OCR fallback.
      - "Digital.aspx" links — document viewer URLs; the embedded PDF path is
        extracted from the ``arquivo`` query param and fetched as a regular PDF.
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

    async def _fetch_page_content(
        self, url: str, vs: str, ev: str, target: str, page_num: int
    ) -> tuple[int, bytes | None]:
        """Wrapper to fetch a postback and return the (page_num, content)."""
        content, _, _ = await self._fetch_postback(url, vs, ev, target, "", "100")
        return page_num, content

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape all norms for a year.

        Pagination uses ASP.NET WebForms. By switching to 100 items per page,
        we can read the repeater controls and concurrently fetch all pages
        listed in the pager (up to 10 at a time). We take the __VIEWSTATE from
        the highest fetched page to get the links for the next batch, repeating
        until all pages are exhausted.
        """
        url = self._format_search_url(year)
        all_docs: list[dict] = []

        content, vs, ev = await self._fetch_first_page(url)
        if not content or not vs or not ev:
            return []

        # Switch to 100 items per page to minimize requests
        content, vs, ev = await self._fetch_postback(
            url,
            vs,
            ev,
            "ctl00$ContentPlaceHolder1$ddl_ItensExibidos",
            arg="",
            items_per_page="100",
        )
        if not content:
            return []

        soup = BeautifulSoup(content, "html.parser")
        all_docs.extend(self._parse_docs_from_soup(soup))

        visited_pages = {1}
        vs_val = vs
        ev_val = ev

        while True:
            if not vs_val or not ev_val:
                break

            pager_container = soup.find(id="ContentPlaceHolder1_rptPaging")
            if not pager_container:
                break

            tasks = []
            for a in pager_container.find_all("a"):
                href = a.get("href", "")
                if "javascript:__doPostBack" in href:
                    target = href.split("'")[1]
                    page_num = int(a.text.strip())
                    if page_num not in visited_pages:
                        visited_pages.add(page_num)
                        tasks.append(
                            self._fetch_page_content(
                                url, vs_val, ev_val, target, page_num
                            )
                        )

            if not tasks:
                break

            desc = f"ESPIRITO_SANTO | {year} | Pagers"
            results = await self._gather_results(tasks, desc=desc)

            highest_page_num = 0
            highest_page_soup = None

            for page_num, content_bytes in results:
                if not content_bytes:
                    continue
                s = BeautifulSoup(content_bytes, "html.parser")
                all_docs.extend(self._parse_docs_from_soup(s))

                if page_num > highest_page_num:
                    highest_page_num = page_num
                    highest_page_soup = s

            if highest_page_soup is None:
                break

            soup = highest_page_soup
            vs_tag = soup.find(id="__VIEWSTATE")
            ev_tag = soup.find(id="__EVENTVALIDATION")
            vs_val = vs_tag["value"] if vs_tag else None
            ev_val = ev_tag["value"] if ev_tag else None

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

        if self._is_already_scraped(url, doc_info.get("title", "")):
            return None

        # if url ends with .pdf, get only text_markdown
        if url.endswith(".pdf"):
            text_markdown, raw_content, content_ext = await self._download_and_convert(
                url
            )

            valid, _reason = valid_markdown(text_markdown)
            if not valid:
                # raw_content already holds the PDF bytes from _download_and_convert;
                # reuse them for the LLM OCR fallback instead of re-downloading.
                text_markdown = await self._get_markdown(stream=BytesIO(raw_content))

                valid, reason = valid_markdown(text_markdown)
                if not valid:
                    await self._save_doc_error(
                        title=doc_info.get("title", ""),
                        year=doc_info.get("year", ""),
                        html_link=url,
                        error_message=f"Invalid markdown from PDF: {reason}",
                    )
                    return None

            doc_info["text_markdown"] = _clean_markdown(text_markdown, summary)
            doc_info["document_url"] = url
            doc_info["raw_content"] = raw_content
            doc_info["content_extension"] = content_ext
            from src.scraper.base.schemas import ScrapedDocument

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
        html_string = soup.prettify()

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
            result.text_markdown = _clean_markdown(result.text_markdown, summary)
        return result
