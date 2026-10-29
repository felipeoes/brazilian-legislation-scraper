import re
from collections import defaultdict
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup
from loguru import logger

from src.core.convert import (
    clean_norm_soup,
    pdf_to_markdown,
    remove_summary_element,
    valid_markdown,
)
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

TYPES = {
    "Ato Deliberativo": "ato-deliberativo",
    "Ato Normativo": "ato-normativo",
    "Decreto Legislativo": "decleg/decleg.htm",
    "Emenda Constitucional": "legislacao5/const_e/ement.htm",
    "Lei Complementar": "ementario/lc.htm",
    "Lei Ordinária": "lei_ordinaria.htm",
    "Resolução": "resolucao",
}
SITUATIONS = {"Não consta": "Não consta"}


class CearaAleceScraper(BaseScraper):
    """Webscraper for Ceara state legislation website (https://www.al.ce.gov.br/)

    Year start (earliest on source): 1968

    Example search request: https://www.al.ce.gov.br/legislativo/leis-e-normativos-internos?categoria=ato-normativo&page=1
    """

    name = "CEARA"
    types = TYPES
    situations = SITUATIONS
    base_url = "https://www.al.ce.gov.br/legislativo"
    PAGINATED_TYPES = {"Ato Deliberativo", "Ato Normativo", "Resolução"}
    STATIC_INDEX_TYPES = {
        "Emenda Constitucional",
        "Lei Complementar",
        "Decreto Legislativo",
    }

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.params = {"categoria": "", "page": 1}
        self._prefetched_docs: dict[int, dict[str, list]] = defaultdict(
            lambda: defaultdict(list)
        )

    def _format_search_url(self, norm_type_id: str, page: int) -> str:
        """Format url for search request"""
        params = {**self.params, "categoria": norm_type_id, "page": page}
        return f"{self.base_url}/leis-e-normativos-internos?{urlencode(params)}"

    async def _get_docs_links(
        self, norm_type: str, url: str, soup: BeautifulSoup | None = None
    ) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'year', 'norm_number', 'summary', 'document_url'
        """
        if soup is None:
            soup = (await self.http.request(url, timeout=60)).soup()
        if not soup:
            return []
        docs = []
        empty_tag = soup.find("p", class_="mt-5")
        if empty_tag and "nenhum dado localizado" in empty_tag.text.lower():
            return []
        tables = soup.find_all("table")
        if len(tables) < 2:
            table = tables[0]
        else:
            table = tables[1]
        items = table.find_all("tr")
        for item in items:
            tds = item.find_all("td")
            if len(tds) != 6:
                continue
            norm_number = tds[0].text.strip()
            year = norm_number.split("/")[1]
            title = norm_number
            summary = tds[2].text.strip()
            document_url = tds[5].find("a")["href"]
            docs.append(
                {
                    "title": f"{norm_type} {title}",
                    "year": year,
                    "norm_number": norm_number,
                    "summary": summary,
                    "document_url": document_url,
                }
            )
        return docs

    async def fetch(self, doc_info: dict) -> Document | None:
        """Get document data from given document dict"""
        if doc_info["type"] not in self.PAGINATED_TYPES:
            return await self._get_laws_constitution_amendments_doc_data(
                doc_info, doc_info["type"], doc_info["year"]
            )
        visualizar_url = doc_info["document_url"].rstrip("/") + "/visualizar"
        doc_info["document_url"] = visualizar_url
        try:
            soup, mhtml = await self.browser.snapshot(visualizar_url)
        except Exception as exc:
            raise ValueError(f"Failed to fetch document page: {exc}")
        content = soup.find("div", class_="card-body") or soup.find("main") or soup
        clean_norm_soup(content, unwrap_links=False)
        remove_summary_element(content, doc_info.get("summary", ""))
        html_string = str(content)
        text_markdown = await self.markdown(html=html_string)
        valid, reason = valid_markdown(text_markdown)
        if not valid:
            logger.error(f"Invalid markdown for {visualizar_url}: {reason}")
            raise ValueError(reason)
        doc_info["text_markdown"] = text_markdown
        doc_info["raw"] = mhtml
        doc_info["raw_ext"] = ".mhtml"
        return Document(**doc_info)

    async def _fetch_lei_ordinaria_years(self) -> list[int]:
        """Return sorted list of available years for Lei Ordinária."""
        norm_type_id = self.types["Lei Ordinária"]
        url = f"https://www2.al.ce.gov.br/legislativo/{norm_type_id}"
        soup = (await self.http.request(url)).soup()
        if not soup:
            return []
        table = soup.find("table", {"class": "MsoNormalTable"})
        rows = table.find_all("tr")
        years = []
        for row in rows[1:]:
            for td in row.find_all("td"):
                a = td.find("a")
                if a:
                    years.append(int(a.text))
        years.sort()
        return years

    async def _get_laws_constitution_amendments_docs_links(
        self, url: str, norm_type: str
    ) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'year', 'summary', 'html_link'
        """
        soup = (await self.http.request(url)).soup()
        if not soup:
            return []
        docs = []
        table = soup.find_all("table")
        if len(table) > 1:
            table = table[1]
        else:
            table = table[0]
        items = table.find_all("tr")
        for item in items[1:]:
            tds = item.find_all("td")
            if norm_type == "Lei Ordinária" and len(tds) > 3:
                continue
            title = tds[0].text.strip()
            try:
                summary = tds[1].text.strip()
                html_link = tds[0].find("a")["href"]
                html_link = html_link.replace("../", "")
            except Exception:
                logger.debug(f"No link found for document '{title}' — skipping")
                continue
            year = None
            if norm_type != "Lei Ordinária":
                year_match = re.search(
                    "[\\s,]\\d{1,2}°?[./]\\d{1,2}[./](\\d{2}|\\d{4})\\b", title
                )
                if not year_match:
                    year_match = re.search("\\b((?:19|20)\\d{2})\\b", title)
                if not year_match:
                    logger.warning(
                        f"Could not extract year from title '{title[:80]}' — skipping"
                    )
                    continue
                year_text = year_match.group(1)
                if len(year_text) == 2:
                    year_now = datetime.now().year
                    year_text = f"20{year_text}"
                    if int(year_text) > year_now:
                        year_text = f"19{year_text[2:]}"
                year = int(year_text)
            docs.append(
                {
                    "title": title,
                    "year": year,
                    "summary": summary,
                    "html_link": html_link,
                }
            )
        return docs

    def construct_url(self, norm_type: str, html_link: str, year: int | None) -> str:
        """Construct the full url for the document page"""
        if "http://" in html_link or "https://" in html_link:
            return html_link.replace("http://", "https://", 1)
        if "file://" in html_link:
            html_link = html_link.split("legislacao5")[-1].replace("\\", "/")
            if html_link.startswith("/"):
                html_link = html_link[1:]
            base_url = "https://www2.al.ce.gov.br/legislativo/legislacao5/"
            return urljoin(base_url, html_link)
        if norm_type == "Lei Complementar":
            base_url = "https://www2.al.ce.gov.br/legislativo/"
            if "ementario" not in html_link:
                html_link = f"ementario/{html_link}"
        elif norm_type == "Decreto Legislativo":
            base_url = "https://www2.al.ce.gov.br/legislativo/decleg/"
        else:
            base_url = "https://www2.al.ce.gov.br/legislativo/legislacao5/"
            if norm_type == "Lei Ordinária":
                if "leis" not in html_link:
                    year = year if year >= 2000 else str(year)[2:]
                    html_link = f"leis{year}/{html_link}"
                elif (
                    "legislacao5" in html_link
                    or "legislativo/legislacao5/" in html_link
                    or "/legislativo/legislacao5/" in html_link
                ):
                    html_link = (
                        html_link.replace("/legislativo/legislacao5/", "")
                        .replace("legislativo/legislacao5/", "")
                        .replace("legislacao5/", "")
                    )
        return urljoin(base_url, html_link)

    async def _get_laws_constitution_amendments_doc_data(
        self, doc_info: dict, norm_type: str, year: int | None = None
    ) -> dict | None:
        """Get document data from given document dict"""
        doc_info = dict(doc_info)
        html_link = doc_info.pop("html_link")
        url = self.construct_url(norm_type, html_link, year)
        try:
            soup, mhtml = await self.browser.snapshot(url)
        except Exception as exc:
            logger.error(
                f"Error fetching document page: {url}. Year: {year}. HTML link: {html_link}"
            )
            raise ValueError(f"Failed to fetch document page: {exc}")
        for a in soup.find_all("a"):
            if a.get_text(strip=True).upper() == "VOLTAR":
                a.decompose()
        for node in soup.find_all(
            string=re.compile("O texto desta Lei", re.IGNORECASE)
        ):
            node.parent.decompose()
        sections = soup.find_all("div", class_=re.compile("^Section\\d+$"))
        if sections:
            container = BeautifulSoup("<div></div>", "lxml").div
            for sec in sections:
                for child in list(sec.children):
                    container.append(child.extract())
            content = container
        else:
            content = soup
        remove_summary_element(content, doc_info.get("summary", ""))
        html_string = str(content).strip()
        if "NÄO EXISTE LEI COM ESTE NÚMERO".lower() in html_string.lower():
            raise ValueError("Invalid document (NÄO EXISTE LEI COM ESTE NÚMERO)")
        text_markdown = await self.markdown(html=html_string)
        valid, reason = valid_markdown(text_markdown)
        if not valid:
            pdf_md = await self._browser_pdf_to_markdown(url)
            if pdf_md:
                text_markdown = pdf_md
                valid, reason = valid_markdown(text_markdown)
        if not valid:
            logger.error(f"Invalid markdown for {url} (year={year}): {reason}")
            raise ValueError(reason)
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url
        doc_info["raw"] = mhtml
        doc_info["raw_ext"] = ".mhtml"
        return Document(**doc_info)

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        for norm_type, refs in self._prefetched_docs.get(year, {}).items():
            for ref in refs:
                url = ref.get("document_url") or self.construct_url(
                    norm_type, ref["html_link"], year
                )
                yield {
                    **ref,
                    "year": year,
                    "type": norm_type,
                    "document_url": url,
                    "situation": DEFAULT_VALID_SITUATION,
                }
        if year in self._lei_ordinaria_years:
            suffix = str(year) if year >= 2000 else str(year)[2:]
            filename = f"LEIS{year}" if year >= 2021 else f"e{suffix}"
            url = f"https://www2.al.ce.gov.br/legislativo/legislacao5/leis{suffix}/{filename}.htm"
            for ref in await self._get_laws_constitution_amendments_docs_links(
                url, "Lei Ordinária"
            ):
                yield {
                    **ref,
                    "year": year,
                    "type": "Lei Ordinária",
                    "document_url": self.construct_url(
                        "Lei Ordinária", ref["html_link"], year
                    ),
                    "situation": DEFAULT_VALID_SITUATION,
                }

    async def setup(self) -> None:
        self._lei_ordinaria_years = set(await self._fetch_lei_ordinaria_years())
        for norm_type in self.PAGINATED_TYPES:
            url = self._format_search_url(self.types[norm_type], 1)
            soup = (await self.http.request(url, timeout=60)).soup()
            pagination = soup.find("ul", class_="pagination")
            pages = (
                int(pagination.find_all("li")[-2].find("a")["href"].split("page=")[-1])
                if pagination
                else 1
            )

            async def fetch_page(page: int) -> list[dict]:
                return (
                    await self._get_docs_links(norm_type, url, soup=soup)
                    if page == 1
                    else await self._get_docs_links(
                        norm_type, self._format_search_url(self.types[norm_type], page)
                    )
                )

            async for ref in self.paginate(fetch_page, pages):
                year = int(ref["year"])
                if year in self.years:
                    self._prefetched_docs[year][norm_type].append(ref)
        for norm_type in self.STATIC_INDEX_TYPES:
            url = f"https://www2.al.ce.gov.br/legislativo/{self.types[norm_type]}"
            for ref in await self._get_laws_constitution_amendments_docs_links(
                url, norm_type
            ):
                if ref.get("year") in self.years:
                    self._prefetched_docs[ref["year"]][norm_type].append(ref)

    async def _browser_pdf_to_markdown(self, url: str) -> str:
        async with self.browser.page() as page:
            await self.browser.goto(page, url, wait_until="load", timeout=60000)
            data = await page.pdf()
        return await pdf_to_markdown(data, self.ocr)
