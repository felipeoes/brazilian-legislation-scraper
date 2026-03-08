from io import BytesIO
from urllib.parse import urljoin

import re
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
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


class ESAlesScraper(StateScraper):
    """Webscraper for Espirito Santo state legislation website (https://www3.al.es.gov.br/legislacao)

    Year start (earliest on source): 1958

    Example search request: https://www3.al.es.gov.br/legislacao/consulta-legislacao.aspx?ano=2000&interno=1
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

            summary_tag = item.find("a", class_="kt-widget5__desc")
            summary = summary_tag.text.strip() if summary_tag else ""

            # date and situation are the two kt-font-info spans in the first info div
            info_div = item.find("div", class_="kt-widget5__info")
            info_spans = (
                info_div.find_all("span", class_="kt-font-info") if info_div else []
            )
            date = info_spans[0].text.strip() if info_spans else ""
            situation = info_spans[1].text.strip() if len(info_spans) > 1 else ""

            # authors from the second kt-widget5__info div
            info_divs = item.find_all("div", class_="kt-widget5__info")
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
                    "title": re.sub(r"\r\n +", " ", title_raw.strip()),
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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=15),
        reraise=True,
    )
    async def _fetch_first_page(
        self, url: str
    ) -> tuple[bytes | None, str | None, str | None]:
        """GET the first page and return (content, viewstate, eventvalidation)."""
        resp = await self.request_service.make_request(url)
        if not resp:
            return None, None, None
        content = await resp.read()
        soup = BeautifulSoup(content, "html.parser")
        vs = soup.find(id="__VIEWSTATE")
        ev = soup.find(id="__EVENTVALIDATION")
        return content, vs["value"] if vs else None, ev["value"] if ev else None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=15),
        reraise=True,
    )
    async def _fetch_next_page(
        self, url: str, viewstate: str, eventvalidation: str
    ) -> tuple[bytes | None, str | None, str | None]:
        """POST lbNext and return (content, viewstate, eventvalidation) for the next page."""
        post_data = {
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$lbNext",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": viewstate,
            "__EVENTVALIDATION": eventvalidation,
        }
        resp = await self.request_service.make_request(
            url, method="POST", payload=post_data
        )
        if not resp:
            return None, None, None
        content = await resp.read()
        soup = BeautifulSoup(content, "html.parser")
        vs = soup.find(id="__VIEWSTATE")
        ev = soup.find(id="__EVENTVALIDATION")
        return content, vs["value"] if vs else None, ev["value"] if ev else None

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape all norms for a year by following lbNext sequentially."""
        url = self._format_search_url(year)
        all_docs: list[dict] = []

        content, vs, ev = await self._fetch_first_page(url)
        if content is None:
            return []

        soup = BeautifulSoup(content, "html.parser")
        all_docs.extend(self._parse_docs_from_soup(soup))

        while self._has_next_page(soup):
            content, vs, ev = await self._fetch_next_page(url, vs, ev)
            if content is None or vs is None:
                break
            soup = BeautifulSoup(content, "html.parser")
            all_docs.extend(self._parse_docs_from_soup(soup))

        for doc in all_docs:
            doc["year"] = year

        return await self._process_documents(
            all_docs,
            year=year,
            norm_type="all",
            situation="all",
            desc=f"ESPIRITO_SANTO | {year}",
        )

    async def _get_doc_data(self, doc_info: dict) -> dict | None:
        """Get document data from document link"""
        doc_info = dict(doc_info)
        doc_link = doc_info.pop("doc_link")
        url = urljoin(self.base_url, doc_link)

        # some urls are malformed, e.g., they ends with .pd instead of .pdf
        if url.endswith(".pd"):
            url = url + "f"

        if self._is_already_scraped(url, doc_info.get("title", "")):
            return None

        # if url ends with .pdf, get only text_markdown
        if url.endswith(".pdf"):
            text_markdown, raw_content, content_ext = await self._download_and_convert(
                url
            )

            valid, _reason = self._valid_markdown(text_markdown)
            if not valid:
                # pdf may be an image — try LLM OCR path
                response = await self.request_service.make_request(url)
                if not response:
                    logger.error(f"Failed to download PDF from URL: {url}")
                    await self._save_doc_error(
                        title=doc_info.get("title", ""),
                        year=doc_info.get("year", ""),
                        html_link=url,
                        error_message="Failed to download PDF",
                    )
                    return None

                pdf_content = await response.read()
                text_markdown = await self._get_markdown(stream=BytesIO(pdf_content))
                raw_content = pdf_content
                content_ext = ".pdf"

                valid, reason = self._valid_markdown(text_markdown)
                if not valid:
                    await self._save_doc_error(
                        title=doc_info.get("title", ""),
                        year=doc_info.get("year", ""),
                        html_link=url,
                        error_message=f"Invalid markdown from PDF: {reason}",
                    )
                    return None

            doc_info["text_markdown"] = text_markdown
            doc_info["document_url"] = url
            doc_info["_raw_content"] = raw_content
            doc_info["_content_extension"] = content_ext
            return doc_info

        resp = await self.request_service.make_request(url)
        if not resp:
            logger.error(f"Failed to fetch HTML for URL: {url}")
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=url,
                error_message="Failed to get document page",
            )
            return None

        content = await resp.read()
        # Parse raw bytes so BeautifulSoup detects the actual encoding
        # (these pages declare utf-8 but are actually windows-1252)
        soup = BeautifulSoup(content, "html.parser")
        # Remove images — alt text from decorative logos pollutes the markdown
        for img in soup.find_all("img"):
            img.decompose()
        html_string = soup.prettify()

        return await self._process_html_doc(doc_info, html_string, url)
