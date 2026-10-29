import re
from collections.abc import AsyncIterator
from datetime import datetime
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup, Tag

from src.core.convert import (
    calc_pages,
    clean_norm_soup,
    normalize_type,
    remove_summary_element,
    valid_markdown,
    wrap_html,
)
from src.core.document import Document
from src.core.http import HttpError
from src.core.scraper import BaseScraper

TYPES = {
    "Decreto Legislativo": 14,
    "Lei Complementar": 12,
    "Lei Ordinária": 13,
    "Resolução": 15,
    "Emenda Constitucional": 11,
}
SITUATIONS = {"Não consta": "Não consta"}
_RESULTS_PER_PAGE = 20
_WHITESPACE_RE = re.compile("\\s+")
_TOTAL_RESULTS_RE = re.compile("Encontramos\\s*(\\d+)\\s*res", re.IGNORECASE)
_TITLE_TOTAL_RE = re.compile("BUSCAR\\s+LEGISLA..ES\\s*\\((\\d+)\\)", re.IGNORECASE)
_TITLE_DATE_RE = re.compile("\\b(\\d{2}/\\d{2}/\\d{2,4})\\b")
_CONSOLIDATED_PATH = "ver_texto_consolidado.php"
_TEXT_PATH = "ver_texto_lei.php"


class AmapaAlapScraper(BaseScraper):
    """Webscraper for Amapa state legislation website (https://al.ap.leg.br).

    Year start (earliest on source): 1991

    Example search request: https://al.ap.leg.br/pagina.php?pg=buscar_legislacao&aba=legislacao&submenu=listar_legislacao&especie_documento=&ano=2025&pesquisa=&n_doeB=&n_leiB=&data_inicial=&data_final=&orgaoB=&autor=&legislaturaB=&pagina=2
    """

    name = "AMAPA"
    yearless = True
    types = TYPES
    situations = {}
    base_url = "https://al.ap.leg.br"

    @staticmethod
    def _clean_text(value: str) -> str:
        return _WHITESPACE_RE.sub(" ", value or "").strip()

    @classmethod
    def _canonical_doc_link(cls, link: str | None) -> str | None:
        if not link:
            return None
        return link.replace(_CONSOLIDATED_PATH, _TEXT_PATH, 1)

    def _normalize_type(self, raw_type: str) -> str:
        return normalize_type(raw_type, fallback="Legislação", known_types=self.types)

    def _format_search_url(self, year: int, page: int) -> str:
        params = {
            "pg": "buscar_legislacao",
            "aba": "legislacao",
            "submenu": "listar_legislacao",
            "especie_documento": "",
            "ano": year,
            "pesquisa": "",
            "n_doeB": "",
            "n_leiB": "",
            "data_inicial": "",
            "data_final": "",
            "orgaoB": "",
            "autor": "",
            "legislaturaB": "",
            "pagina": page,
        }
        return f"{self.base_url}/pagina.php?{urlencode(params)}"

    def _parse_total_results(self, soup: BeautifulSoup) -> int:
        page_text = self._clean_text(soup.get_text(" ", strip=True))
        for regex in (_TOTAL_RESULTS_RE, _TITLE_TOTAL_RE):
            match = regex.search(page_text)
            if match:
                return int(match.group(1))
        raise ValueError("Could not determine total results from AL/AP listing page")

    @staticmethod
    def _parse_publication_year(date_text: str) -> int | None:
        cleaned = date_text.replace(".", "/").strip()
        for fmt in ("%d/%m/%Y", "%d/%m/%y"):
            try:
                return datetime.strptime(cleaned, fmt).year
            except ValueError:
                continue
        return None

    @classmethod
    def _parse_title_year(cls, title_text: str) -> int | None:
        match = _TITLE_DATE_RE.search(title_text or "")
        if not match:
            return None
        return cls._parse_publication_year(match.group(1))

    def _parse_listing_documents(
        self, soup: BeautifulSoup, query_year: int
    ) -> list[dict]:
        tbody = soup.find("tbody")
        if tbody is None:
            if self._parse_total_results(soup) == 0:
                return []
            raise ValueError("Could not find legislation results table body")
        docs: list[dict] = []
        current_doc: dict | None = None
        for row in tbody.find_all("tr", recursive=False):
            tds = row.find_all("td", recursive=False)
            if len(tds) == 6:
                type_link = tds[0].find("a")
                doc_type = self._normalize_type(
                    (type_link.get("title") if type_link else "")
                    or (type_link.get_text(" ", strip=True) if type_link else "")
                    or tds[0].get_text(" ", strip=True)
                )
                title_text = self._clean_text(tds[0].get_text(" ", strip=True))
                publication_date = self._clean_text(tds[3].get_text(" ", strip=True))
                publication_year = (
                    self._parse_publication_year(publication_date)
                    or self._parse_title_year(title_text)
                    or query_year
                )
                view_link = tds[5].find("a", href=True)
                current_doc = {
                    "title": title_text,
                    "summary": self._clean_text(tds[1].get_text(" ", strip=True)),
                    "doe_number": self._clean_text(tds[2].get_text(" ", strip=True)),
                    "date": publication_date,
                    "publication_date": publication_date,
                    "year": publication_year,
                    "query_year": query_year,
                    "proposition_number": self._clean_text(
                        tds[4].get_text(" ", strip=True)
                    ),
                    "type": doc_type,
                    "situation": "Não consta",
                    "html_link": view_link["href"] if view_link else None,
                }
                docs.append(current_doc)
                continue
            if current_doc is None:
                continue
            consolidated_link = None
            for link in row.find_all("a", href=True):
                href = link.get("href")
                if isinstance(href, str) and _CONSOLIDATED_PATH in href:
                    consolidated_link = link
                    break
            if consolidated_link:
                href = str(consolidated_link.get("href", ""))
                current_doc["consolidated_link"] = href
                if not current_doc.get("html_link"):
                    current_doc["html_link"] = self._canonical_doc_link(href)
            row_text = self._clean_text(row.get_text(" ", strip=True))
            lower_text = row_text.casefold()
            if lower_text.startswith("alterações:"):
                current_doc["alteracoes"] = row_text.partition(":")[2].strip()
            elif lower_text.startswith("observações:"):
                current_doc["observacoes"] = row_text.partition(":")[2].strip()
        return docs

    async def _get_docs_links(self, url: str, *, query_year: int) -> list[dict]:
        soup = (await self.http.request(url)).soup()
        if not soup or not isinstance(soup, BeautifulSoup):
            raise ValueError(f"Failed to get soup for URL: {url}")
        return self._parse_listing_documents(soup, query_year=query_year)

    def _extract_document_container(
        self, soup: BeautifulSoup
    ) -> Tag | BeautifulSoup | None:
        for a_tag in soup.find_all("a", class_="texto_noticia3"):
            table = a_tag.find_parent("table")
            if table:
                table.decompose()
        for img in soup.find_all("img"):
            src = img.get("src")
            if isinstance(src, str) and "brasao" in src.lower():
                table = img.find_parent("table")
                if table:
                    table.decompose()
        remaining_table = soup.find("table")
        if remaining_table:
            inner_table = remaining_table.find("table")
            content_td = (
                inner_table.find("td") if inner_table else remaining_table.find("td")
            )
            return content_td or remaining_table
        return soup.body or soup

    def _paragraphs_to_markdown(self, container: Tag | BeautifulSoup) -> str:
        lines: list[str] = []
        for paragraph in container.find_all("p"):
            text = paragraph.get_text(" ", strip=True)
            if not text:
                continue
            strong = paragraph.find("strong")
            if strong and len(paragraph.find_all("strong")) == 1:
                inner = strong.get_text(" ", strip=True)
                if inner == text:
                    text = f"**{text}**"
            lines.append(text)
        return "\n\n".join(lines)

    async def _extract_source_content(
        self, source_url: str, summary: str = ""
    ) -> dict | None:
        response = await self.http.request(source_url)
        response_obj = response
        soup = BeautifulSoup(response_obj.content, "lxml")
        container = self._extract_document_container(soup)
        if container is None:
            return None
        clean_norm_soup(container, unwrap_fonts=True, strip_styles=True)
        remove_summary_element(container, summary)
        container_text = self._clean_text(container.get_text(" ", strip=True))
        if not container_text:
            return None
        html_string = wrap_html(container.decode_contents())
        text_markdown = await self.markdown(html=html_string)
        if len((text_markdown or "").strip()) < 100:
            fallback_markdown = self._paragraphs_to_markdown(container)
            if fallback_markdown:
                text_markdown = fallback_markdown
        valid, reason = valid_markdown(text_markdown)
        return {
            "source_url": source_url,
            "html_string": html_string,
            "text_markdown": text_markdown,
            "valid": valid,
            "reason": reason,
        }

    async def fetch(self, doc_info: dict) -> Document | None:
        doc_data = dict(doc_info)
        html_link = doc_data.get("html_link")
        consolidated_link = doc_data.get("consolidated_link")
        canonical_link = html_link or self._canonical_doc_link(consolidated_link)
        canonical_url = (
            urljoin(self.base_url, canonical_link) if canonical_link else None
        )
        if not canonical_url:
            raise ValueError("No text link found for document")
        source_links = []
        if consolidated_link:
            source_links.append(urljoin(self.base_url, consolidated_link))
        if canonical_url:
            source_links.append(canonical_url)
        source_links = list(dict.fromkeys(source_links))
        invalid_sources: list[str] = []
        selected_content: dict | None = None
        for source_url in source_links:
            try:
                extracted = await self._extract_source_content(
                    source_url, summary=doc_data.get("summary", "")
                )
            except HttpError as exc:
                if exc.status != 404:
                    raise
                invalid_sources.append(f"{source_url}: not found")
                continue
            if extracted is None:
                invalid_sources.append(f"{source_url}: empty or inaccessible content")
                continue
            if extracted["valid"]:
                selected_content = extracted
                break
            invalid_sources.append(f"{source_url}: {extracted['reason']}")
        if selected_content is None:
            raise ValueError(
                "; ".join(invalid_sources)
                or "Failed to fetch or validate document content"
            )
        document_url = canonical_url
        doc_data["text_markdown"] = selected_content["text_markdown"]
        doc_data["document_url"] = document_url
        capture_url = selected_content["source_url"]
        if capture_url != document_url:
            doc_data["content_source_url"] = capture_url
        try:
            mhtml = (await self.browser.snapshot(capture_url))[1]
        except Exception as exc:
            raise ValueError(f"MHTML capture failed: {exc}")
        from src.core.document import Document

        return Document(**doc_data, raw=mhtml, raw_ext=".mhtml")

    async def list_documents(self, year: int | None) -> AsyncIterator[dict]:
        url = self._format_search_url("", page=1)
        soup = (await self.http.request(url)).soup()

        async def fetch_page(page: int) -> list[dict]:
            return (
                self._parse_listing_documents(soup, query_year=None)
                if page == 1
                else await self._get_docs_links(
                    self._format_search_url("", page), query_year=None
                )
            )

        async for ref in self.paginate(
            fetch_page, calc_pages(self._parse_total_results(soup), _RESULTS_PER_PAGE)
        ):
            yield {
                **ref,
                "document_url": urljoin(
                    self.base_url,
                    ref.get("html_link")
                    or self._canonical_doc_link(ref.get("consolidated_link"))
                    or "",
                ),
            }
