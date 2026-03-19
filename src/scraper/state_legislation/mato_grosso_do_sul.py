from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from typing import TYPE_CHECKING, cast

import aiohttp
from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.scraper.base.converter import wrap_html
from src.scraper.base.scraper import (
    DEFAULT_INVALID_SITUATION,
    StateScraper,
    flatten_results,
)

TYPES = {
    "Constituição Estadual": "/Web%5CConstituição%20Estadual",
    "Decreto": "/Decreto",
    "Decreto E": "/DecretoE",
    "Decreto E Conjunto": "/Web%5CDecretoE%20Conjunto",
    "Decreto-Lei": "/Decreto-Lei",
    "Deliberação Conselho de Governança": "/Web%5CDeliberacaoConselhoGov",
    "Emenda Constitucional": "/Emenda",
    "Lei Complementar": "/Lei%20Complementar",
    "Lei Estadual": "/Lei%20Estadual",
    "Mensagem Vetada": "/Mensagem%20Veto",
    "Resolução": "/Resolucoes",
    "Resolução Conjunta": "/Web%5CResolução%20Conjunta",
}

SITUATIONS = {
    "Não consta": "Não consta",
    DEFAULT_INVALID_SITUATION: DEFAULT_INVALID_SITUATION,
}

_NSF_BASE = "/appls/legislacao/secoge/govato.nsf"
_YEAR_FIELD_NAMES = ("wano", "$246")
_SUMMARY_FIELD_NAMES = ("wementa", "Ato_Ementa", "$246")
_SUMMARY_NORMALIZE_RE = re.compile(r"[^\w]", re.UNICODE)
_RE_REVOKED_NOTE = re.compile(
    r"\bRevogad[ao]\b\s+(?:pela|pelo|por)\b",
    re.IGNORECASE,
)


class MSAlemsScraper(StateScraper):
    """Webscraper for Mato Grosso do Sul state legislation (Domino/Notes web server).

    Year start (earliest on source): 1979

    Uses the Domino ``?ReadViewEntries`` XML API to list documents per type and
    year without relying on session-based full-text search or HTML view parsing.

    Example entry endpoint:
        GET https://aacpdappls.net.ms.gov.br/appls/legislacao/secoge/govato.nsf/Lei%20Estadual?ReadViewEntries&Count=200&Start=2&Expand=2

    Documents are fetched by UNID:
        GET https://aacpdappls.net.ms.gov.br/appls/legislacao/secoge/govato.nsf/0/{UNID}?OpenDocument
    """

    def __init__(
        self,
        base_url: str = "https://aacpdappls.net.ms.gov.br",
        **kwargs,
    ):
        super().__init__(
            base_url,
            types=TYPES,
            situations=SITUATIONS,
            name="MATO_GROSSO_DO_SUL",
            **kwargs,
        )
        self._nsf = f"{self.base_url}{_NSF_BASE}"
        self._type_year_index: dict[str, dict[int, tuple[str, int]]] = {}

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    def _view_entries_url(
        self,
        type_path: str,
        start: int | str = 1,
        count: int = 200,
        expand: int | str | None = None,
    ) -> str:
        """Build a ``?ReadViewEntries`` URL for a given type view."""
        url = f"{self._nsf}{type_path}?ReadViewEntries&Count={count}&Start={start}"
        if expand is not None:
            url += f"&Expand={expand}"
        return url

    def _doc_url(self, unid: str) -> str:
        return f"{self._nsf}/0/{unid}?OpenDocument"

    def _type_items(self) -> list[tuple[str, str]]:
        return list(cast(dict[str, str], self.types).items())

    # ------------------------------------------------------------------
    # XML parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _entrydata_text(data: ET.Element) -> str:
        for tag_name in ("text", "number"):
            el = data.find(tag_name)
            if el is not None and el.text:
                value = el.text.strip()
                if value:
                    return value
        return ""

    @classmethod
    def _entry_text(cls, entry: ET.Element, col_name: str | tuple[str, ...]) -> str:
        names = (col_name,) if isinstance(col_name, str) else col_name
        for name in names:
            for data in entry.findall("entrydata"):
                if data.get("name") == name:
                    value = cls._entrydata_text(data)
                    if value:
                        return value
        return ""

    @classmethod
    def _entry_year(cls, entry: ET.Element) -> int | None:
        raw_year: str = cls._entry_text(entry, _YEAR_FIELD_NAMES)
        if not raw_year:
            return None
        try:
            return int(raw_year)
        except ValueError:
            return None

    @classmethod
    def _build_type_year_index(cls, root: ET.Element) -> dict[int, tuple[str, int]]:
        year_index: dict[int, tuple[str, int]] = {}
        for entry in root.findall("viewentry"):
            position = (entry.get("position") or "").strip()
            if not position or "." in position:
                continue

            year = cls._entry_year(entry)
            if year is None:
                continue

            try:
                doc_count = int(entry.get("descendants") or "0")
            except (TypeError, ValueError):
                continue

            if doc_count <= 0:
                continue

            year_index[year] = (position, doc_count)

        return year_index

    @staticmethod
    def _decode_html_bytes(body: bytes, charset: str | None) -> str:
        for encoding in dict.fromkeys([charset, "utf-8", "latin-1", "cp1252"]):
            if not encoding:
                continue
            try:
                return body.decode(encoding)
            except (LookupError, UnicodeDecodeError):
                continue
        return body.decode("utf-8", errors="replace")

    @staticmethod
    def _notes_container(body: Tag) -> Tag | None:
        notes_root = body.find("ul")
        if isinstance(notes_root, Tag):
            return notes_root

        notes_label = body.find(string=re.compile(r"^\s*Notas:\s*$", re.IGNORECASE))
        if notes_label is None:
            return None

        notes_cell = notes_label.find_parent("td")
        return notes_cell if isinstance(notes_cell, Tag) else None

    @staticmethod
    def _normalize_for_compare(text: str) -> str:
        return _SUMMARY_NORMALIZE_RE.sub("", text).casefold()

    @classmethod
    def _remove_summary_element(cls, body: Tag, summary: str) -> None:
        if not summary:
            return

        notes_root = cls._notes_container(body)
        if not isinstance(notes_root, Tag):
            return

        normalized_summary = cls._normalize_for_compare(summary)
        if not normalized_summary:
            return

        tag_priority = {
            "table": 0,
            "td": 1,
            "div": 2,
            "p": 3,
            "i": 4,
            "span": 5,
            "font": 6,
        }
        candidates: list[tuple[int, int, int, Tag]] = []
        for tag in notes_root.find_all(
            ["table", "td", "div", "p", "i", "span", "font"]
        ):
            tag_text = tag.get_text(" ", strip=True)
            normalized_tag = cls._normalize_for_compare(tag_text)
            if not normalized_tag or normalized_summary not in normalized_tag:
                continue
            if "notas" in tag_text.casefold():
                continue
            candidates.append(
                (
                    0 if normalized_tag == normalized_summary else 1,
                    tag_priority.get(tag.name, 99),
                    len(normalized_tag),
                    tag,
                )
            )

        if not candidates:
            return

        _, _, _, best_tag = min(candidates, key=lambda item: item[:3])
        best_tag.decompose()

    @classmethod
    def _has_revogada_note(cls, body: Tag) -> bool:
        notes_root = cls._notes_container(body)
        if not isinstance(notes_root, Tag):
            return False
        return bool(_RE_REVOKED_NOTE.search(notes_root.get_text(" ", strip=True)))

    async def _load_type_year_index(
        self, type_name: str, type_path: str
    ) -> dict[int, tuple[str, int]]:
        cached = self._type_year_index.get(type_name)
        if cached is not None:
            return cached

        cat_url = self._view_entries_url(type_path, start=1, count=200)
        cat_resp = await self.request_service.make_request(cat_url)
        if not cat_resp:
            logger.warning(f"MSAlems | {type_name} | Failed to fetch category list")
            self._type_year_index[type_name] = {}
            return {}

        cat_response = cast(aiohttp.ClientResponse, cat_resp)
        cat_xml = await cat_response.text()
        try:
            cat_root = ET.fromstring(cat_xml)
        except ET.ParseError as exc:
            logger.warning(
                f"MSAlems | {type_name} | XML parse error on categories: {exc}"
            )
            self._type_year_index[type_name] = {}
            return {}

        year_index = self._build_type_year_index(cat_root)
        self._type_year_index[type_name] = year_index
        return year_index

    async def _before_scrape(self) -> None:
        self._type_year_index = {}
        type_items = self._type_items()
        await self._gather_results(
            [
                self._load_type_year_index(type_name, type_path)
                for type_name, type_path in type_items
            ],
            context={"year": "", "type": "", "situation": ""},
            desc="MATO GROSSO DO SUL | year index",
        )

    # ------------------------------------------------------------------
    # Per-type year listing
    # ------------------------------------------------------------------

    async def _get_type_year_docs(
        self, type_name: str, type_path: str, year: int
    ) -> list[dict]:
        """Return all documents for *type_name* + *year* using ReadViewEntries."""
        year_index = await self._load_type_year_index(type_name, type_path)
        expand_info = year_index.get(year)
        if expand_info is None:
            return []

        expand_pos, doc_count = expand_info
        if doc_count <= 0:
            return []

        # --- Fetch all document entries in one request ---
        docs_url = self._view_entries_url(
            type_path,
            start=expand_pos,
            count=doc_count + 1,  # +1 for the category header row
            expand=expand_pos,
        )
        docs_resp = await self.request_service.make_request(docs_url)
        if not docs_resp:
            logger.warning(
                f"MSAlems | {type_name} | Failed to fetch entries for {year}"
            )
            return []

        docs_response = cast(aiohttp.ClientResponse, docs_resp)
        docs_xml = await docs_response.text()
        try:
            docs_root = ET.fromstring(docs_xml)
        except ET.ParseError as exc:
            logger.warning(f"MSAlems | {type_name} | XML parse error on entries: {exc}")
            return []

        prefix = f"{expand_pos}."
        docs: list[dict] = []
        for entry in docs_root.findall("viewentry"):
            pos = entry.get("position", "")
            if not pos.startswith(prefix):
                continue
            unid = entry.get("unid")
            if not unid:
                continue

            title = self._entry_text(entry, "wnumero")
            summary = self._entry_text(entry, _SUMMARY_FIELD_NAMES)
            if not title:
                continue

            docs.append(
                {
                    "title": title,
                    "summary": summary,
                    "html_link": self._doc_url(unid),
                }
            )

        return docs

    # ------------------------------------------------------------------
    # Document data
    # ------------------------------------------------------------------

    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        """Fetch and convert an individual Domino document page to markdown."""
        doc_info = dict(doc_info)
        url = doc_info.pop("html_link")

        if self._is_already_scraped(url, doc_info.get("title", "")):
            return None

        result = await self.request_service.fetch_bytes(url)
        if not result:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                norm_type=doc_info.get("type", ""),
                html_link=url,
                error_message=f"Failed to fetch document page: {result.reason}",
            )
            return None

        if not isinstance(result, tuple):
            return None

        raw_body, response = result
        html = self._decode_html_bytes(raw_body, response.charset)
        soup = BeautifulSoup(html, "html.parser")

        body = soup.find("body")
        if not isinstance(body, Tag):
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                norm_type=doc_info.get("type", ""),
                html_link=url,
                error_message="No <body> tag found in document page",
            )
            return None

        if self._has_revogada_note(body):
            doc_info["situation"] = DEFAULT_INVALID_SITUATION

        self._remove_summary_element(body, doc_info.get("summary", ""))

        for tag in body.find_all(["script", "applet"]):
            tag.decompose()
        for table in body.find_all("table", border="1"):
            table.decompose()

        html_string = wrap_html(body.decode_contents())
        text_markdown = await self._get_markdown(html_content=html_string)
        return await self._process_doc(
            doc_info,
            url,
            text_markdown,
            raw_body,
            ".html",
            error_prefix="Invalid markdown",
        )

    # ------------------------------------------------------------------
    # Year-level scrape (overrides base class)
    # ------------------------------------------------------------------

    async def _scrape_year(self, year: int) -> list[dict]:
        """List all types for *year* via ReadViewEntries, then process docs."""
        situation = self.default_situation

        # Fetch document listings for all types concurrently.
        # Each task returns a (type_name, docs) tuple so the association is
        # preserved regardless of which tasks succeed or fail.
        async def _listing_task(
            type_name: str, type_path: str
        ) -> tuple[str, list[dict]]:
            docs = await self._get_type_year_docs(type_name, type_path, year)
            return type_name, docs

        listing_tasks = [
            _listing_task(type_name, type_path)
            for type_name, type_path in self.types.items()
        ]
        listing_results: list[tuple[str, list[dict]]] = await self._gather_results(
            listing_tasks,
            context={"year": year, "type": "NA", "situation": situation},
            desc=f"MATO GROSSO DO SUL | {year} | listing",
        )

        by_type: dict[str, list] = defaultdict(list)
        for type_name, docs in listing_results:
            for doc in docs:
                if not self._is_already_scraped(
                    doc.get("html_link", ""), doc.get("title", "")
                ):
                    by_type[type_name].append(doc)

        if not by_type:
            return []

        process_tasks = [
            self._process_documents(
                type_docs,
                year=year,
                norm_type=nt,
                situation=situation,
                desc=f"MATO GROSSO DO SUL | {nt} {year}",
            )
            for nt, type_docs in by_type.items()
        ]
        valid = await self._gather_results(
            process_tasks,
            context={"year": year, "type": "NA", "situation": situation},
            desc=f"MATO GROSSO DO SUL | Year {year}",
        )
        return flatten_results(valid)


if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument
