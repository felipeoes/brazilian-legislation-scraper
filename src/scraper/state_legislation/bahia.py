import re
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.scraper.base.converter import wrap_html
from src.scraper.base.scraper import StateScraper


def _remove_summary_from_markdown(md: str, summary: str) -> str:
    """Remove the summary paragraph from text_markdown to avoid duplication.

    The Bahia document body (``field--name-body``) includes the ementa/summary
    as its second paragraph. Since the summary is already stored separately, it
    is stripped here to avoid repeating it verbatim inside ``text_markdown``.
    """
    if not summary:
        return md
    summary_norm = re.sub(r"\s+", " ", summary).strip()
    paragraphs = md.split("\n\n")
    filtered = [
        p
        for p in paragraphs
        if re.sub(r"\s+", " ", re.sub(r"[*_`]+", "", p)).strip() != summary_norm
    ]
    if len(filtered) == len(paragraphs):
        return md  # summary not found as a standalone paragraph
    return "\n\n".join(filtered)


TYPES = {
    "Constituição Estadual Atual 1989": 11,
    "Constituição Estadual 1967": 33,
    "Constituição Estadual 1947": 32,
    "Constituição Estadual 1935": 31,
    "Constituição Estadual 1891": 30,
    "Emenda Constitucional": 4,
    "Lei Complementar": 5,
    "Lei Ordinária": 7,
    "Lei Delegada": 6,
    "Decreto Financeiro": 1,
    "Decreto": 2,
    "Decreto Simples": 3,
    "Portaria do Gabinete do Governador": 26,
    "Portaria Conjunta Casa Civil": 20,
    "Portaria Casa Civil": 19,
    "Instrução Normativa Casa Civil": 92,
}


SITUATIONS: dict[str, str] = {}

_TYPE_LABELS = {name.casefold(): name for name in TYPES}
_TYPE_LABELS.update(
    {
        "emendas constitucionais": "Emenda Constitucional",
        "leis complementares": "Lei Complementar",
        "leis ordinárias": "Lei Ordinária",
        "leis delegadas": "Lei Delegada",
        "decretos financeiros": "Decreto Financeiro",
        "decretos numerados": "Decreto",
        "decretos simples": "Decreto Simples",
        "portarias do gabinete do governador": "Portaria do Gabinete do Governador",
        "portarias conjuntas casa civil": "Portaria Conjunta Casa Civil",
        "portarias casa civil": "Portaria Casa Civil",
        "instruções normativas casa civil": "Instrução Normativa Casa Civil",
    }
)


class BahiaLegislaScraper(StateScraper):
    """Webscraper for Bahia state legislation website (https://www.legislabahia.ba.gov.br/)

    Year start (earliest on source): 1891

    Example search request: https://www.legislabahia.ba.gov.br/documentos?num=&ementa=&exp=&data%5Bmin%5D=2025-01-01&data%5Bmax%5D=2025-12-31&page=0
    """

    _REVOGADO_RE = re.compile(r"\brevogad[ao]\b", re.IGNORECASE)

    def __init__(
        self,
        base_url: str = "https://www.legislabahia.ba.gov.br",
        **kwargs,
    ):
        super().__init__(base_url, name="BAHIA", types=TYPES, situations={}, **kwargs)

    def _normalize_type(self, raw_type: str) -> str:
        return super()._normalize_type(raw_type, aliases=_TYPE_LABELS)

    def _build_search_url(
        self,
        norm_type_id: int | None,
        year: int,
        page: int,
    ) -> str:
        """Build search URL from arguments (no shared state mutation)."""
        params = {
            "num": "",
            "ementa": "",
            "exp": "",
            "data[min]": f"{year}-01-01",
            "data[max]": f"{year}-12-31",
            "page": page,
        }
        if norm_type_id is not None:
            params["categoria[]"] = norm_type_id
        return f"{self.base_url}/documentos?{urlencode(params)}"

    @staticmethod
    def _get_field_item_text(soup: BeautifulSoup | Tag, class_name: str) -> str:
        field = soup.find("div", class_=class_name)
        if not field:
            return ""
        item = field.find("div", class_="field--item")
        if not item:
            return ""
        return item.get_text(" ", strip=True)

    def _get_total_pages(self, soup: BeautifulSoup) -> int:
        pagination = soup.find("ul", class_="pagination js-pager__items")
        if not pagination:
            return 1

        last_page = 0
        for page_link in pagination.find_all("a", href=True):
            href = page_link.get("href")
            if not isinstance(href, str) or "page=" not in href:
                continue
            try:
                page = int(href.split("page=")[-1].split("&")[0])
            except ValueError:
                continue
            last_page = max(last_page, page)

        return last_page + 1

    async def _get_docs_links(
        self,
        url: str,
        *,
        soup: BeautifulSoup | None = None,
    ) -> list[dict]:
        """Get documents html links from a given search page."""
        if soup is None:
            soup = await self._fetch_soup_with_retry(url)

        if soup.find("td", class_="views-empty"):
            return []

        tbody = soup.find("tbody")
        if not tbody:
            return []

        docs = []
        for item in tbody.find_all("tr"):
            tds = item.find_all("td")
            if len(tds) != 2:
                continue

            link_tag = tds[0].find("a", href=True)
            if not link_tag:
                continue
            href = link_tag.get("href")
            if not isinstance(href, str):
                continue

            title_tag = tds[0].find("b")
            title = (title_tag or link_tag).get_text(" ", strip=True)
            norm_type = self._normalize_type(tds[1].get_text(" ", strip=True))
            if not title:
                continue

            docs.append(
                {
                    "title": title,
                    "type": norm_type,
                    "html_link": href,
                }
            )

        return docs

    async def _get_doc_data(self, doc_info: dict) -> dict | None:
        """Get document data from a given document dict."""
        doc_info = dict(doc_info)
        html_link = doc_info.pop("html_link")
        url = urljoin(self.base_url, html_link)

        if self._is_already_scraped(url, doc_info.get("title", "")):
            return None

        try:
            soup, mhtml = await self._fetch_soup_and_mhtml(url)
        except Exception as exc:
            logger.error(f"Failed to get document data from URL: {url} | Error: {exc}")
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year=doc_info.get("year", ""),
                norm_type=doc_info.get("type", ""),
                html_link=url,
                error_message=f"Failed to fetch document page: {exc}",
            )
            return None

        category = self._get_field_item_text(soup, "field--name-field-categoria-doc")
        if category:
            doc_info["type"] = self._normalize_type(category)

        norm_number = self._get_field_item_text(soup, "field--name-field-numero-doc")
        date = self._get_field_item_text(soup, "field--name-field-data-doc")
        publication_date = self._get_field_item_text(
            soup, "field--name-field-data-de-publicacao-no-doe"
        )
        summary = self._get_field_item_text(soup, "field--name-field-ementa")

        norm_text_tag = soup.find("div", class_="field--name-body")
        if not isinstance(norm_text_tag, Tag):
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year=doc_info.get("year", ""),
                norm_type=doc_info.get("type", ""),
                html_link=url,
                error_message="Could not find div.field--name-body in document page",
            )
            return None

        self._clean_norm_soup(
            norm_text_tag,
            unwrap_links=False,
            remove_images=False,
            strip_styles=True,
            remove_style_tags=True,
            remove_script_tags=True,
        )

        html_string = wrap_html(norm_text_tag.prettify())

        is_revogado = bool(norm_text_tag.find("span", class_=self._REVOGADO_RE)) or any(
            self._REVOGADO_RE.match(div.get_text(strip=True))
            for div in norm_text_tag.find_all("div", class_="alteracao")
        )
        if is_revogado:
            doc_info["situation"] = "Revogado"

        doc_info["norm_number"] = norm_number
        doc_info["date"] = date
        doc_info["publication_date"] = publication_date
        doc_info["summary"] = summary
        result = await self._process_html_doc(doc_info, html_string, url, mhtml)
        if result is not None and summary:
            md = result.get("text_markdown", "")
            if md:
                result["text_markdown"] = _remove_summary_from_markdown(md, summary)
        return result

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape all norms for a specific year in a single unfiltered listing."""
        situation = "Não consta"
        url = self._build_search_url(None, year, 0)

        try:
            soup = await self._fetch_soup_with_retry(url)
        except Exception as exc:
            logger.error(f"Failed to fetch year listing for {year}: {exc}")
            await self._save_doc_error(
                title=f"Bahia Year {year}",
                year=year,
                norm_type="NA",
                html_link=url,
                error_message=f"Failed to fetch year listing: {exc}",
            )
            return []

        documents = await self._get_docs_links(url, soup=soup)
        total_pages = self._get_total_pages(soup)
        ctx = {"year": year, "type": "NA", "situation": situation}
        documents.extend(
            await self._fetch_all_pages(
                lambda p: self._get_docs_links(self._build_search_url(None, year, p)),
                total_pages - 1,
                start_page=1,
                context=ctx,
                desc=f"BAHIA | {year} | get_docs_links",
            )
        )

        for doc in documents:
            doc["year"] = year

        return await self._process_documents(
            documents,
            year=year,
            norm_type="NA",
            situation=situation,
            desc=f"BAHIA | {year}",
        )
