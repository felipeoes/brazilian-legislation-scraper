import re
from collections.abc import AsyncIterator
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup, Tag

from src.core.convert import (
    clean_norm_soup,
    normalize_type,
    remove_summary_from_markdown,
    wrap_html,
)
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

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


class BahiaLegislaScraper(BaseScraper):
    """Webscraper for Bahia state legislation website (https://www.legislabahia.ba.gov.br/).

    The site is a Drupal CMS serving static HTML — no JavaScript rendering needed,
    so all fetches use pure aiohttp (no Playwright browser).

    **Listing**: a single year-only query (no type/situation filter) returns all
    document types via ``/documentos?data[min]=YYYY-01-01&data[max]=YYYY-12-31``.
    Pagination is parsed from the ``ul.pagination`` element.

    **Document detail**: each document page contains structured metadata fields
    (category, number, date, publication date, ementa) and a ``div.field--name-body``
    with the norm text. Category from the detail page overrides the listing type.
    Revogado status is detected via ``span.revogado`` or ``div.alteracao`` text.

    **Rate limit**: capped at 1 rps — the server is unstable and returns frequent
    HTTP 500 errors at higher rates.

    Year start (earliest on source): 1891
    """

    name = "BAHIA"
    types = TYPES
    situations = {}
    base_url = "https://www.legislabahia.ba.gov.br"
    _REVOGADO_RE = re.compile("\\brevogad[ao]\\b", re.IGNORECASE)
    _REVOGADO_MARKER_RE = re.compile("<REVOGAD[AO]>", re.IGNORECASE)
    _MAX_RPS = 1

    def _normalize_type(self, raw_type: str) -> str:
        return normalize_type(raw_type, aliases=_TYPE_LABELS, known_types=self.types)

    def _clean_layout_tables(self, soup: Tag) -> None:
        """Unwrap tables used for page layout to avoid broken markdown generation.

        Many Bahia documents are MS Word exports where the entire text is wrapped
        in a giant table for page layout. MarkItDown converts this to a markdown
        table with dozens of empty columns. This method destroys the table structure
        by turning all table tags into divs, which serializes the text into blocks.
        """
        for p in soup.find_all("p", class_="EMPTYCELLSTYLE"):
            p.decompose()
        for tag_name in ["table", "tbody", "thead", "tfoot", "tr", "th", "td"]:
            for el in soup.find_all(tag_name):
                el.name = "div"

    def _strip_revogado(self, soup: Tag) -> bool:
        """Remove the REVOGADO / REVOGADA marker from the document body.

        The site injects a ``<span class='REVOGADA'>`` (or ``<span class='REVOGADO'>``)
        element containing the literal text ``<REVOGADA>`` / ``<REVOGADO>`` at the top
        of some documents. For others, the marker appears inside a ``<p class='LEI'>``
        without its own span wrapper.

        This method also detects revogado status even when the ``<p class='Alteracao'>``
        pattern is used (capital A) rather than ``<div class='alteracao'>``.

        Returns True if the document is revogado/revogada, False otherwise.
        """
        is_revogado = False
        for span in soup.find_all("span", class_=self._REVOGADO_RE):
            is_revogado = True
            span.decompose()
        for el in soup.find_all(["p", "div"]):
            cls = " ".join(el.get("class", []))
            if re.search("\\balteracao\\b", cls, re.IGNORECASE):
                if self._REVOGADO_RE.search(el.get_text(strip=True)):
                    is_revogado = True
                    break
        for el in soup.find_all(string=self._REVOGADO_MARKER_RE):
            el.replace_with("")
        return is_revogado

    def _build_search_url(self, norm_type_id: int | None, year: int, page: int) -> str:
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
        self, url: str, *, soup: BeautifulSoup | None = None
    ) -> list[dict]:
        """Get documents html links from a given search page."""
        if soup is None:
            soup = (await self.http.request(url)).soup()
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
            docs.append({"title": title, "type": norm_type, "html_link": href})
        return docs

    async def fetch(self, doc_info: dict) -> Document | None:
        """Get document data from a given document dict."""
        doc_info = dict(doc_info)
        html_link = doc_info.pop("html_link")
        url = urljoin(self.base_url, html_link)
        resp = await self.http.request(url)
        body, _ = (resp.content, resp)
        soup = BeautifulSoup(body, "lxml")
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
            raise ValueError("Could not find div.field--name-body in document page")
        clean_norm_soup(
            norm_text_tag,
            unwrap_links=False,
            strip_styles=True,
            remove_style_tags=True,
            remove_script_tags=True,
        )
        self._clean_layout_tables(norm_text_tag)
        is_revogado = self._strip_revogado(norm_text_tag)
        if is_revogado:
            doc_info["situation"] = "Revogado"
        html_string = wrap_html(str(norm_text_tag))
        doc_info["norm_number"] = norm_number
        doc_info["date"] = date
        doc_info["publication_date"] = publication_date
        doc_info["summary"] = summary
        text_markdown = await self.markdown(html=html_string)
        result = Document(
            **{
                **doc_info,
                "document_url": url,
                "text_markdown": text_markdown,
                "raw": body,
                "raw_ext": ".html",
            }
        )
        if result is not None and summary:
            md = result.text_markdown
            if md:
                result.text_markdown = remove_summary_from_markdown(md, summary)
        return result

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        url = self._build_search_url(None, year, 0)
        soup = (await self.http.request(url)).soup()
        pages = self._get_total_pages(soup)

        async def fetch_page(page: int) -> list[dict]:
            return (
                await self._get_docs_links(url, soup=soup)
                if page == 0
                else await self._get_docs_links(
                    self._build_search_url(None, year, page)
                )
            )

        async for ref in self.paginate(fetch_page, pages - 1, start=0):
            yield {
                "year": year,
                "situation": DEFAULT_VALID_SITUATION,
                **ref,
                "document_url": urljoin(self.base_url, ref["html_link"]),
            }
