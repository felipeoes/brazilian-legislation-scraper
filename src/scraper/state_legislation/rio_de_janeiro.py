import base64
import copy
import re
import urllib.parse
from typing import Any

from bs4 import BeautifulSoup
from unidecode import unidecode

from src.scraper.base.scraper import StateScraper

# obs: LeiComp = Lei Complementar; LeiOrd = Lei Ordinária;
TYPES = {
    "Constituição Estadual": "Constituição Estadual",
    "Decreto": {"view_name": "DecretoInt", "page_id": 50},
    "Emenda": {"view_name": "EmendaInt", "page_id": 51},
    "LeiComp": {"view_name": "LeiCompInt", "page_id": 52},
    "LeiOrd": {"view_name": "LeiOrdInt", "page_id": 53},
    "Resolucao": {"view_name": "ResolucaoInt", "page_id": 54},
}

# situations will be inferred from the text of the norm
SITUATIONS = {}


class RJAlerjScraper(StateScraper):
    """Webscraper for Alesp (Assembleia Legislativa do Rio de Janeiro) website (https://www.alerj.rj.gov.br/)

    Year start (earliest on source): 1891

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
        self._scraped_constitution = False
        self.lotus_base_url = "https://www3.alerj.rj.gov.br/lotus_notes/default.asp"

    def _format_search_url(self, norm_type_id: dict[str, Any], start: int = 1) -> str:
        """Format url for search request"""
        inner_path = (
            f"/contlei.nsf/{norm_type_id['view_name']}?OpenForm"
            f"&Count={self.params['Count']}&Start={start}"
        )
        encoded_path = base64.b64encode(inner_path.encode("utf-8")).decode("ascii")
        params = {"id": norm_type_id["page_id"], "url": encoded_path}
        return f"{self.lotus_base_url}?{urllib.parse.urlencode(params)}"

    def _build_wrapped_data_role_url(self, page_id: int, data_role: str) -> str:
        encoded_path = base64.b64encode(data_role.encode("utf-8")).decode("ascii")
        params = {"id": page_id, "url": encoded_path}
        return f"{self.lotus_base_url}?{urllib.parse.urlencode(params)}"

    def _get_docs_html_links(
        self, norm_type: str, page_id: int, soup: BeautifulSoup, year: int
    ) -> tuple[list[dict[str, Any]], list[int]]:
        """Get documents html links from soup object."""
        html_links = []
        page_years = []
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) not in {4, 6}:
                continue

            link_tag = tds[0].find("a", attrs={"data-role": True}) or tds[0].find("a")
            if not link_tag:
                continue

            try:
                row_year = int(tds[1].get_text(strip=True))
            except ValueError:
                continue

            page_years.append(row_year)
            if row_year != year:
                continue

            doc_data_role = link_tag.get("data-role") or link_tag.get("href")
            if not doc_data_role:
                continue

            wrapped_url = self._build_wrapped_data_role_url(page_id, doc_data_role)
            document_url = urllib.parse.urljoin(self.base_url, doc_data_role)
            number = tds[0].get_text(strip=True)

            if len(tds) == 6:
                author = tds[5].get_text(strip=True)
                summary = tds[4].get_text(strip=True)
            else:
                author = tds[2].get_text(strip=True)
                summary = tds[3].get_text(strip=True)

            html_links.append(
                {
                    "title": f"{norm_type} {number}/{row_year}",
                    "date": str(row_year),
                    "author": author,
                    "summary": summary,
                    "_html_link": wrapped_url,
                    "document_url": document_url.replace("?OpenDocument", ""),
                    "_mhtml_url": wrapped_url,
                }
            )
        return html_links, page_years

    def _extract_norm_content_root(self, soup: BeautifulSoup) -> BeautifulSoup | None:
        page_content = soup.find("div", class_="pagina_central")
        div_conteudo = soup.find("div", id="divConteudo")
        if page_content and div_conteudo and div_conteudo.parent is page_content:
            fragment = BeautifulSoup("<div></div>", "html.parser")
            container = fragment.div
            for child in div_conteudo.children:
                if getattr(child, "name", None) is None and not str(child).strip():
                    continue
                container.append(copy.copy(child))
            for sibling in div_conteudo.next_siblings:
                if getattr(sibling, "name", None) is None and not str(sibling).strip():
                    continue
                container.append(copy.copy(sibling))
            if container.get_text(" ", strip=True):
                return container
        return div_conteudo or soup.body

    def _clean_norm_content_root(self, content_root: BeautifulSoup):
        for tag in content_root.find_all(["form", "map"]):
            tag.decompose()
        for div_to_remove in content_root.find_all("div", class_="alert alert-warning"):
            div_to_remove.decompose()
        for div_to_remove in content_root.find_all("div", id="barraBotoes"):
            div_to_remove.decompose()
        self._strip_html_chrome(content_root)
        self._clean_norm_soup(content_root)
        for text_node in list(content_root.strings):
            if not text_node.strip():
                continue
            parent = text_node.parent
            if parent and parent.name in {"div", "font", "span"}:
                lowered = text_node.strip().lower()
                if lowered.startswith("clique aqui caso você tenha dificuldade"):
                    parent.decompose()
                elif lowered in {"por nº", "por ano", "por autor", "por assunto"}:
                    parent.decompose()
        for tag in content_root.find_all(["div", "b", "font", "span"], recursive=False):
            text = tag.get_text(" ", strip=True)
            lowered = text.lower()
            if lowered in {
                "lei complementar",
                "decreto legislativo",
                "emenda constitucional",
                "lei ordinária",
                "resolução",
                "resoluções",
                "emendas constitucionais",
                "legislação - decretos legislativos",
            }:
                tag.decompose()
            elif lowered.startswith("texto da ") or lowered.startswith("texto do "):
                tag.decompose()

    def _trim_to_norm_start(self, content_root: BeautifulSoup):
        start_tag = None
        patterns = [
            re.compile(r"^LEI\b.*\bN[.º°o]*\b", re.IGNORECASE),
            re.compile(r"^DECRETO\b", re.IGNORECASE),
            re.compile(r"^RESOLU[ÇC][AÃ]O\b", re.IGNORECASE),
            re.compile(r"^EMENDA\b", re.IGNORECASE),
            re.compile(r"^O GOVERNADOR\b", re.IGNORECASE),
            re.compile(r"^ART\.\s*1", re.IGNORECASE),
        ]
        for tag in content_root.find_all(["b", "font", "div", "p"], recursive=False):
            text = tag.get_text(" ", strip=True)
            if any(pattern.search(text) for pattern in patterns):
                start_tag = tag
                break
        if start_tag:
            for sibling in list(start_tag.previous_siblings):
                if getattr(sibling, "extract", None):
                    sibling.extract()

    @staticmethod
    def _normalize_rj_text(text: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", unidecode(text).lower())

    @staticmethod
    def _extract_doc_number_and_year(
        title: str, date: str | None = None
    ) -> tuple[str, str]:
        title = title or ""
        date = date or ""
        match = re.search(r"(\d+(?:\.\d+)*)\s*/\s*(\d{4})", title)
        if match:
            return match.group(1), match.group(2)
        year_match = re.search(r"(\d{4})", date)
        return "", year_match.group(1) if year_match else ""

    def _find_best_matching_container(
        self,
        content_root: BeautifulSoup,
        candidate_names: list[str],
        matches: callable,
    ):
        candidate_tags = []
        for tag in content_root.find_all(candidate_names):
            tag_text = tag.get_text(" ", strip=True)
            normalized_tag_text = self._normalize_rj_text(tag_text)
            if not normalized_tag_text:
                continue
            if not matches(tag_text, normalized_tag_text):
                continue
            candidate_tags.append((len(normalized_tag_text), tag))

        if not candidate_tags:
            return None

        _, best_tag = min(candidate_tags, key=lambda item: item[0])

        container = best_tag
        while container.parent and container.parent is not content_root:
            parent = container.parent
            if getattr(parent, "name", None) not in {"table", "div", "p", "td"}:
                break
            parent_text = parent.get_text(" ", strip=True)
            normalized_parent_text = self._normalize_rj_text(parent_text)
            if not normalized_parent_text:
                break
            if not matches(parent_text, normalized_parent_text):
                break
            container = parent

        return container

    def _remove_summary_element(self, content_root: BeautifulSoup, summary: str):
        if not summary:
            return

        normalized_summary = self._normalize_rj_text(summary)
        if not normalized_summary:
            return

        def matches(tag_text: str, normalized_tag_text: str) -> bool:
            if normalized_summary not in normalized_tag_text:
                return False
            if "art." in tag_text.lower() or "o governador" in tag_text.lower():
                return False
            return True

        best_tag = self._find_best_matching_container(
            content_root,
            ["table", "div", "p", "td", "span", "font"],
            matches,
        )
        if best_tag:
            best_tag.decompose()

    def _remove_header_metadata_element(
        self, content_root: BeautifulSoup, doc_info: dict[str, Any]
    ):
        number, year = self._extract_doc_number_and_year(
            doc_info.get("title", ""),
            doc_info.get("date", ""),
        )
        normalized_number = self._normalize_rj_text(number)
        normalized_year = self._normalize_rj_text(year)

        def matches(tag_text: str, normalized_tag_text: str) -> bool:
            if "datadapromulgacao" not in normalized_tag_text:
                return False
            if normalized_number and normalized_number not in normalized_tag_text:
                return False
            if normalized_year and normalized_year not in normalized_tag_text:
                return False
            if "art." in tag_text.lower() or "o governador" in tag_text.lower():
                return False
            return True

        best_tag = self._find_best_matching_container(
            content_root,
            ["table", "div", "p", "td", "span", "font"],
            matches,
        )
        if best_tag:
            best_tag.decompose()

    @staticmethod
    def _clean_extracted_markdown(text_markdown: str) -> str:
        labels = {
            "Lei Complementar",
            "Lei Ordinária",
            "Resolução",
            "Resoluções",
            "Emenda Constitucional",
            "Emendas Constitucionais",
            "Decreto Legislativo",
            "Legislação - Decretos Legislativos",
        }
        lines = text_markdown.splitlines()
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines:
            stripped = lines[0].strip().strip("*").strip()
            lowered = stripped.lower()
            if (
                stripped in labels
                or lowered.startswith("texto da ")
                or lowered.startswith("texto do ")
            ):
                lines.pop(0)
                while lines and not lines[0].strip():
                    lines.pop(0)
                continue
            break
        return "\n".join(lines).strip()

    async def _get_doc_data(self, doc_info: dict, year: str = "") -> dict:
        """Get document data from given html link"""
        doc_html_link = doc_info["_html_link"]
        document_url = doc_info.get("document_url") or doc_html_link.strip().replace(
            "?OpenDocument", ""
        )

        if self._is_already_scraped(document_url, doc_info.get("title", "")):
            return None

        soup = await self.request_service.get_soup(doc_html_link)
        if not soup:
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                html_link=doc_html_link,
                error_message=f"Request failed: {getattr(soup, 'reason', 'unknown')}",
            )
            return None

        content_root = self._extract_norm_content_root(soup)
        if not content_root:
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                html_link=doc_html_link,
                error_message="Page body is empty",
            )
            return None

        section2_tag = content_root.find("a", attrs={"name": "_Section2"})
        if section2_tag:
            for tag in section2_tag.find_all_next():
                tag.decompose()
            section2_tag.decompose()

        self._clean_norm_content_root(content_root)
        self._trim_to_norm_start(content_root)
        self._remove_header_metadata_element(content_root, doc_info)
        self._remove_summary_element(content_root, doc_info.get("summary", ""))

        situation = (
            "Revogada"
            if content_root.find(
                string=re.compile(r"\[\s*Revogad[ao]\s*\]", re.IGNORECASE)
            )
            else "Sem revogação expressa"
        )

        html_string = content_root.prettify().replace("\n", "")
        full_html = self._wrap_html(html_string)
        text_markdown = (await self._get_markdown(html_content=full_html)).strip()
        text_markdown = self._clean_extracted_markdown(text_markdown)

        valid, reason = self._valid_markdown(text_markdown)
        if not valid:
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                html_link=doc_html_link,
                error_message=f"Invalid markdown: {reason}",
            )
            return None
        result = {
            **doc_info,
            "year": year,
            "situation": situation,
            "text_markdown": text_markdown,
            "document_url": document_url,
            "_mhtml_url": doc_html_link,
            "_raw_content": full_html.encode("utf-8"),
            "_content_extension": ".html",
        }
        result.pop("_html_link", None)

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
            string=re.compile(r"Texto do Título|Texto do Capítulo|Texto da Seção")
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
        if not section_soup:
            return None
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
            self._scraped_constitution = True
            return

        soup = await self.request_service.get_soup(constitution_url)
        if not soup:
            return

        a_links = [
            a
            for a in soup.find_all("a", attrs={"data-role": True})
            if "Indice" not in a["data-role"]
            and "OpenNavigator" not in a["data-role"]
            and "EMENDAS CONSTITUCIONAIS" not in a.text.strip().upper()
        ]

        tasks = [self._fetch_constitution_section(a) for a in a_links]
        html_parts = await self._gather_results(
            tasks, desc="RJ - ALERJ | Constituição Estadual"
        )

        html_string = "<hr/>".join(html_parts)
        full_html = self._wrap_html(html_string)
        text_markdown = (await self._get_markdown(html_content=full_html)).strip()

        valid, reason = self._valid_markdown(text_markdown)
        if not valid:
            self._scraped_constitution = True
            return
        queue_item = {
            "year": 1989,
            "type": "Constituição Estadual",
            "title": title,
            "date": "05/10/1989",
            "author": "",
            "summary": "",
            "text_markdown": text_markdown,
            "situation": "Sem revogação expressa",
            "document_url": constitution_url,
            "_mhtml_url": constitution_url,
            "_raw_content": full_html.encode("utf-8"),
            "_content_extension": ".html",
        }

        saved = await self._save_doc_result(queue_item)
        if saved is not None:
            queue_item = saved

        self._track_results([queue_item])
        self.count += 1
        self._scraped_constitution = True

    async def _before_scrape(self) -> None:
        await self.scrape_constitution()

    async def _scrape_type(self, norm_type: str, norm_type_id, year: int) -> list[dict]:
        """Scrape norms for a specific type in a year"""
        if norm_type == "Constituição Estadual":
            return []

        documents_html_links: list[dict[str, Any]] = []
        start = 1

        while True:
            url = self._format_search_url(norm_type_id, start=start)
            soup = await self.request_service.get_soup(url)
            if not soup:
                break

            page_docs, page_years = self._get_docs_html_links(
                norm_type,
                norm_type_id["page_id"],
                soup,
                year,
            )
            documents_html_links.extend(page_docs)

            if not page_years:
                break

            if max(page_years) < year or len(page_years) < self.params["Count"]:
                break

            start += self.params["Count"]

        return await self._process_documents(
            documents_html_links,
            year=year,
            norm_type=norm_type,
            desc=f"RJ - ALERJ | {norm_type}",
            doc_data_kwargs={"year": year},
        )

    # _scrape_year uses default from BaseScraper
