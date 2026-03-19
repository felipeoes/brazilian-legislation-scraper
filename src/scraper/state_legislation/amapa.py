from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument

import asyncio
import re
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.scraper.base.converter import (
    calc_pages,
    valid_markdown,
    wrap_html,
)
from src.scraper.base.scraper import StateScraper, flatten_results

TYPES = {
    "Decreto Legislativo": 14,
    "Lei Complementar": 12,
    "Lei Ordinária": 13,
    "Resolução": 15,
    "Emenda Constitucional": 11,
}

SITUATIONS = {"Não consta": "Não consta"}

_RESULTS_PER_PAGE = 20
_WHITESPACE_RE = re.compile(r"\s+")
_TOTAL_RESULTS_RE = re.compile(r"Encontramos\s*(\d+)\s*res", re.IGNORECASE)
_TITLE_TOTAL_RE = re.compile(r"BUSCAR\s+LEGISLA..ES\s*\((\d+)\)", re.IGNORECASE)
_TITLE_DATE_RE = re.compile(r"\b(\d{2}/\d{2}/\d{2,4})\b")
_CONSOLIDATED_PATH = "ver_texto_consolidado.php"
_TEXT_PATH = "ver_texto_lei.php"


class AmapaAlapScraper(StateScraper):
    """Webscraper for Amapa state legislation website (https://al.ap.leg.br).

    Year start (earliest on source): 1991

    Example search request: https://al.ap.leg.br/pagina.php?pg=buscar_legislacao&aba=legislacao&submenu=listar_legislacao&especie_documento=&ano=2025&pesquisa=&n_doeB=&n_leiB=&data_inicial=&data_final=&orgaoB=&autor=&legislaturaB=&pagina=2
    """

    def __init__(
        self,
        base_url: str = "https://al.ap.leg.br",
        **kwargs,
    ):
        super().__init__(base_url, name="AMAPA", types=TYPES, situations={}, **kwargs)
        self._global_scraped_keys: set[tuple[str, str]] = set()
        self._global_scraped_keys_loaded = False
        self._pending_flush_years: set[int] = set()

    @staticmethod
    def _clean_text(value: str) -> str:
        return _WHITESPACE_RE.sub(" ", value or "").strip()

    @classmethod
    def _canonical_doc_link(cls, link: str | None) -> str | None:
        if not link:
            return None
        return link.replace(_CONSOLIDATED_PATH, _TEXT_PATH, 1)

    def _normalize_type(self, raw_type: str) -> str:
        return super()._normalize_type(raw_type, fallback="Legislação")

    def _format_search_url(
        self,
        year: int,
        page: int,
    ) -> str:
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
        soup = await self.request_service.get_soup(url)
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

    def _remove_summary_element(
        self, container: Tag | BeautifulSoup, summary: str
    ) -> None:
        if not summary:
            return
        normalized_summary = self._clean_text(summary).casefold()
        if not normalized_summary:
            return

        candidates: list[tuple[int, Tag]] = []
        for tag in container.find_all(["p", "td", "div", "span", "font", "table"]):
            tag_text = self._clean_text(tag.get_text(" ", strip=True))
            normalized_tag = tag_text.casefold()
            if normalized_summary not in normalized_tag:
                continue
            if "art." in tag_text.lower():
                continue
            candidates.append((len(normalized_tag), tag))

        if candidates:
            _, best = min(candidates, key=lambda x: x[0])
            best.decompose()

    async def _extract_source_content(
        self, source_url: str, summary: str = ""
    ) -> dict | None:
        response = await self.request_service.make_request(source_url)
        if not response or not hasattr(response, "read"):
            return None

        response_obj = cast(Any, response)
        soup = BeautifulSoup(await response_obj.read(), "html.parser")
        container = self._extract_document_container(soup)
        if container is None:
            return None

        self._clean_norm_soup(
            cast(BeautifulSoup, container),
            unwrap_fonts=True,
            strip_styles=True,
        )
        self._remove_summary_element(cast(BeautifulSoup, container), summary)
        container_text = self._clean_text(container.get_text(" ", strip=True))
        if not container_text:
            return None

        html_string = wrap_html(container.decode_contents())
        text_markdown = await self._get_markdown(html_content=html_string)
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

    async def _load_scraped_keys(self, year: int) -> None:
        if not self.saver or self.overwrite:
            self._global_scraped_keys = set()
            self._scraped_keys = set()
            self._global_scraped_keys_loaded = True
            return

        if not self._global_scraped_keys_loaded:
            scraped_keys: set[tuple[str, str]] = set()
            save_root = Path(self.saver.save_dir)
            saved_years: list[int] = []

            if save_root.exists():
                for entry in save_root.iterdir():
                    if not entry.is_dir():
                        continue
                    try:
                        saved_years.append(int(entry.name))
                    except ValueError:
                        continue

            for saved_year in sorted(set(saved_years)):
                scraped_keys.update(await self.saver.get_scraped_keys(saved_year))

            self._global_scraped_keys = scraped_keys
            self._global_scraped_keys_loaded = True
            if scraped_keys:
                logger.debug(
                    f"{self.__class__.__name__}: loaded {len(scraped_keys)} global scraped keys"
                )

        self._scraped_keys = self._global_scraped_keys

    async def _save_doc_result(self, doc_result: dict) -> dict | None:
        saved = await super()._save_doc_result(doc_result)
        if saved is None:
            return None

        document_url = str(saved.get("document_url", ""))
        title = str(saved.get("title", ""))
        if document_url:
            self._global_scraped_keys.add((document_url, title))
            self._scraped_keys = self._global_scraped_keys

        year = saved.get("year")
        if year is not None:
            try:
                self._pending_flush_years.add(int(year))
            except (TypeError, ValueError):
                pass

        return saved

    async def _flush_touched_years(self) -> None:
        if not self.saver or not self._pending_flush_years:
            return

        years = sorted(self._pending_flush_years)
        self._pending_flush_years.clear()
        await asyncio.gather(*(self.saver.flush(year) for year in years))

    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        doc_data = dict(doc_info)
        title = doc_data.get("title", "")
        html_link = doc_data.get("html_link")
        consolidated_link = doc_data.get("consolidated_link")
        canonical_link = html_link or self._canonical_doc_link(consolidated_link)
        canonical_url = (
            urljoin(self.base_url, canonical_link) if canonical_link else None
        )

        if canonical_url and self._is_already_scraped(canonical_url, title):
            return None

        if not canonical_url:
            await self._save_doc_error(
                title=title,
                year=doc_data.get("year", ""),
                situation=doc_data.get("situation", "Não consta"),
                norm_type=doc_data.get("type", "Legislação"),
                html_link="",
                error_message="No text link found for document",
            )
            return None

        source_links = []
        if consolidated_link:
            source_links.append(urljoin(self.base_url, consolidated_link))
        if canonical_url:
            source_links.append(canonical_url)
        source_links = list(dict.fromkeys(source_links))

        invalid_sources: list[str] = []
        selected_content: dict | None = None
        for source_url in source_links:
            extracted = await self._extract_source_content(
                source_url, summary=doc_data.get("summary", "")
            )
            if extracted is None:
                invalid_sources.append(f"{source_url}: empty or inaccessible content")
                continue
            if extracted["valid"]:
                selected_content = extracted
                break
            invalid_sources.append(f"{source_url}: {extracted['reason']}")

        if selected_content is None:
            await self._save_doc_error(
                title=title,
                year=doc_data.get("year", ""),
                situation=doc_data.get("situation", "Não consta"),
                norm_type=doc_data.get("type", "Legislação"),
                html_link=canonical_url,
                error_message="; ".join(invalid_sources)
                or "Failed to fetch or validate document content",
            )
            return None

        document_url = canonical_url
        if self._is_already_scraped(document_url, title):
            return None

        doc_data["text_markdown"] = selected_content["text_markdown"]
        doc_data["document_url"] = document_url
        capture_url = selected_content["source_url"]
        if capture_url != document_url:
            doc_data["content_source_url"] = capture_url

        try:
            mhtml = await self._capture_mhtml(capture_url)
        except Exception as exc:
            await self._save_doc_error(
                title=title,
                year=doc_data.get("year", ""),
                situation=doc_data.get("situation", "Não consta"),
                norm_type=doc_data.get("type", "Legislação"),
                html_link=capture_url,
                error_message=f"MHTML capture failed: {exc}",
            )
            return None

        from src.scraper.base.schemas import ScrapedDocument

        return ScrapedDocument(
            **doc_data,
            raw_content=mhtml,
            content_extension=".mhtml",
        )

    async def _get_year_documents(self, year: int) -> list[dict]:
        first_url = self._format_search_url(year, page=1)
        first_soup = await self.request_service.get_soup(first_url)
        if not first_soup or not isinstance(first_soup, BeautifulSoup):
            raise ValueError(f"Failed to get soup for URL: {first_url}")

        documents = self._parse_listing_documents(first_soup, query_year=year)
        total_results = self._parse_total_results(first_soup)
        total_pages = calc_pages(total_results, _RESULTS_PER_PAGE)
        if total_pages <= 1:
            return documents

        context = {"year": year, "type": "Legislação", "situation": "Não consta"}
        tasks = [
            self._get_docs_links(
                self._format_search_url(year, page=page), query_year=year
            )
            for page in range(2, total_pages + 1)
        ]
        remaining_pages = await self._gather_results(
            tasks,
            context=context,
            desc=f"AMAPA | Year {year} | listings",
        )
        documents.extend(flatten_results(remaining_pages))
        return documents

    async def _scrape_year(self, year: int) -> list[dict]:
        documents = await self._get_year_documents(year)
        if not documents:
            await self._flush_touched_years()
            return []

        results = await self._process_documents(
            documents,
            year=year,
            norm_type="Legislação",
            situation="Não consta",
            desc=f"AMAPA | Year {year}",
        )
        await self._flush_touched_years()
        return results


if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument
