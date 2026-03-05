import asyncio
import base64
import re
import urllib.parse
from typing import Any

from bs4 import BeautifulSoup

from src.scraper.base.scraper import StateScraper
from loguru import logger

# obs: LeiComp = Lei Complementar; LeiOrd = Lei Ordinária;
TYPES = [
    "Constituição Estadual",
    "Decreto",
    "Emenda",
    "LeiComp",
    "LeiOrd",
    "Resolucao",
]

# situations will be inferred from the text of the norm
VALID_SITUATIONS = []
INVALID_SITUATIONS = []
# norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class RJAlerjScraper(StateScraper):
    """Webscraper for Alesp (Assembleia Legislativa do Rio de Janeiro) website (https://www.alerj.rj.gov.br/)

    Example search request: http://alerjln1.alerj.rj.gov.br/contlei.nsf/DecretoAnoInt?OpenForm&Start=1&Count=300

    Observation: Only valid norms are published on the Alerj website (the invalid ones are archived and available only on another search engine that is not working currently), so we don't need to check for validity
    """

    def __init__(
        self,
        base_url: str = "http://alerjln1.alerj.rj.gov.br/contlei.nsf",
        **kwargs,
    ):
        super().__init__(
            base_url,
            types=TYPES,
            situations=SITUATIONS,
            name="RIO_DE_JANEIRO",
            **kwargs,
        )
        self.params = {
            "OpenForm": "",
            "Start": 1,
            "Count": 500,
        }
        self.fetched_constitution = False

    def _format_search_url(self, norm_type: str) -> str:
        """Format url for search request"""
        return f"{self.base_url}/{norm_type}AnoInt?OpenForm&Start={self.params['Start']}&Count={self.params['Count']}"

    def _get_docs_html_links(
        self, norm_type: str, soup: BeautifulSoup
    ) -> list[dict[str, Any]]:
        """Get documents html links from soup object."""
        html_links = []
        for tr in soup.find_all("tr", valign="top"):
            tds = tr.find_all("td")
            if len(tds) != 6:
                continue

            link_tag = tds[1].find("a")
            if not link_tag or not link_tag.has_attr("href"):
                continue

            url = urllib.parse.urljoin(self.base_url, link_tag["href"])
            html_links.append(
                {
                    "title": f"{norm_type} {tds[1].text.strip()}",
                    "date": tds[2].text.strip(),
                    "author": tds[3].text.strip(),
                    "summary": tds[4].text.strip(),
                    "html_link": url,
                }
            )
        return html_links

    async def _html_to_markdown(self, html_string: str) -> str:
        """Converts an HTML string to Markdown."""
        full_html = self._wrap_html(html_string)
        return (await self._get_markdown(html_content=full_html)).strip()

    async def _get_doc_data(self, doc_info: dict, year: str = "") -> dict:
        """Get document data from given html link"""
        doc_html_link = doc_info["html_link"]
        document_url = doc_html_link.strip().replace("?OpenDocument", "")

        if self._is_already_scraped(document_url, doc_info.get("title", "")):
            return None

        soup = await self.request_service.get_soup(doc_html_link)

        body = soup.body
        if not body:
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year="",
                situation="",
                norm_type="",
                html_link=doc_html_link,
                error_message="Page body is empty",
            )
            return None

        # Decompose all content after the main section
        section2_tag = body.find("a", attrs={"name": "_Section2"})
        if section2_tag:
            for tag in section2_tag.find_all_next():
                tag.decompose()
            section2_tag.decompose()

        # Clean up the HTML
        for img_tag in body.find_all("img"):
            img_tag.decompose()
        for a_tag in body.find_all("a"):
            a_tag.unwrap()

        # Determine situation
        situation = (
            "Revogada"
            if soup.find("font", text=re.compile(r"\s*\[ Revogado \]\s*"))
            else "Sem revogação expressa"
        )

        html_string = body.prettify().replace("\n", "")
        text_markdown = await self._html_to_markdown(html_string)

        full_html = self._wrap_html(html_string)
        result = {
            **doc_info,
            "year": year,
            "situation": situation,
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": document_url,
            "_raw_content": full_html.encode("utf-8"),
            "_content_extension": ".html",
        }

        return result

    def _build_constitution_section_url(self, data_role: str) -> str:
        """Builds the URL for a section of the constitution."""
        base_url = "http://www3.alerj.rj.gov.br/lotus_notes/default.asp"
        encoded_path = base64.b64encode(data_role.encode("utf-8")).decode("ascii")
        query_params = {"id": 73, "url": encoded_path}
        return f"{base_url}?{urllib.parse.urlencode(query_params)}"

    def _clean_constitution_section_soup(self, soup: BeautifulSoup):
        """Cleans the BeautifulSoup object of a constitution section."""
        for div_to_remove in soup.find_all("div", class_="alert alert-warning"):
            div_to_remove.decompose()
        for div_to_remove in soup.find_all("div", id="barraBotoes"):
            div_to_remove.decompose()
        for tag_to_remove in soup.find_all(
            text=re.compile(r"Texto do Título|Texto do Capítulo|Texto da Seção")
        ):
            parent = tag_to_remove.parent
            if parent and not parent.decomposed:
                parent.decompose()
        for img_to_remove in soup.find_all("img"):
            img_to_remove.decompose()

    async def _fetch_constitution_section(self, a_link) -> str | None:
        """Fetch a single constitution section and return its HTML."""
        section_url = self._build_constitution_section_url(a_link["data-role"])
        section_soup = await self.request_service.get_soup(section_url)
        self._clean_constitution_section_soup(section_soup)
        content_div = section_soup.find("div", id="divConteudo")
        if content_div:
            return content_div.prettify().replace("\n", "")
        return None

    async def scrape_constitution(self):
        """Scrape constitution data"""
        constitution_url = "http://www3.alerj.rj.gov.br/lotus_notes/default.asp?id=73&url=L2NvbnN0ZXN0Lm5zZi9JbmRpY2VJbnQ/T3BlbkZvcm0mU3RhcnQ9MSZDb3VudD0zMDA="

        title = "Constituição Estadual do Rio de Janeiro"
        if self._is_already_scraped(constitution_url, title):
            self.fetched_constitution = True
            return

        soup = await self.request_service.get_soup(constitution_url)

        a_links = [
            a
            for a in soup.find_all("a", attrs={"data-role": True})
            if "Indice" not in a["data-role"]
            and "OpenNavigator" not in a["data-role"]
            and "EMENDAS CONSTITUCIONAIS" not in a.text.strip().upper()
        ]

        tasks = [self._fetch_constitution_section(a) for a in a_links]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        html_parts = [r for r in results if isinstance(r, str)]

        html_string = "<hr/>".join(html_parts)
        text_markdown = await self._html_to_markdown(html_string)

        full_html = self._wrap_html(html_string)
        queue_item = {
            "year": 1989,
            "type": "Constituição Estadual",
            "title": title,
            "date": "05/10/1989",
            "author": "",
            "summary": "",
            "html_link": constitution_url,
            "html_string": full_html,
            "text_markdown": text_markdown,
            "situation": "Sem revogação expressa",
            "document_url": constitution_url,
            "_raw_content": full_html.encode("utf-8"),
            "_content_extension": ".html",
        }

        saved = await self._save_doc_result(queue_item)
        if saved is not None:
            queue_item = saved

        self._track_results([queue_item])
        self.count += 1
        self.fetched_constitution = True

    async def _scrape_type(
        self, norm_type: str, _norm_type_id, year: str
    ) -> list[dict]:
        """Scrape norms for a specific type in a year"""
        if norm_type == "Constituição Estadual":
            if not self.fetched_constitution:
                await self.scrape_constitution()
            return []

        url = self._format_search_url(norm_type)
        soup = await self.request_service.get_soup(url)

        if soup.find("tr", valign="top") is None:
            return []

        img_item = soup.find("img", alt=f"Show details for {year}")
        if not img_item:
            return []

        year_item = img_item.find_parent("a")
        if not year_item or not year_item.has_attr("href"):
            return []

        year_url = urllib.parse.urljoin(url, year_item["href"])
        soup = await self.request_service.get_soup(year_url)

        documents_html_links = self._get_docs_html_links(norm_type, soup)

        ctx = {"year": year, "type": norm_type, "situation": "N/A"}
        tasks = [
            self._with_save(self._get_doc_data(doc), ctx)
            for doc in documents_html_links
        ]
        results = await self._gather_results(
            tasks,
            context=ctx,
            desc=f"RJ - ALERJ | {norm_type}",
        )

        if self.verbose:
            logger.info(f"Scraped {len(results)} {norm_type} documents in {year}")

        return results

    # _scrape_year uses default from BaseScraper
