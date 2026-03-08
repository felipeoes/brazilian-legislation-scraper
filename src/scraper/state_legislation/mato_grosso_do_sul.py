import xml.etree.ElementTree as ET
from collections import defaultdict

from loguru import logger

from src.scraper.base.scraper import StateScraper

TYPES = {
    "Constituição Estadual": "/Web%5CConstituição%20Estadual",
    "Decreto": "/Decreto",
    "Decreto E": "/DecretoE",
    "Decreto-Lei": "/Decreto-Lei",
    "Deliberação Conselho de Governança": "/Web%5CDeliberacaoConselhoGov",
    "Emenda Constitucional": "/Emenda",
    "Lei Complementar": "/Lei%20Complementar",
    "Lei Estadual": "/Lei%20Estadual",
    "Mensagem Vetada": "/Mensagem%20Veto",
    "Resolução": "/Resolucoes",
    "Resolução Conjunta": "/Web%5CResolução%20Conjunta",
}

SITUATIONS = {"Não consta": "Não consta"}

_NSF_BASE = "/appls/legislacao/secoge/govato.nsf"


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

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    def _view_entries_url(
        self,
        type_path: str,
        start: int = 1,
        count: int = 200,
        expand: int | None = None,
    ) -> str:
        """Build a ``?ReadViewEntries`` URL for a given type view."""
        url = f"{self._nsf}{type_path}?ReadViewEntries&Count={count}&Start={start}"
        if expand is not None:
            url += f"&Expand={expand}"
        return url

    def _doc_url(self, unid: str) -> str:
        return f"{self._nsf}/0/{unid}?OpenDocument"

    # ------------------------------------------------------------------
    # XML parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _entry_text(entry, col_name: str) -> str:
        for data in entry.findall("entrydata"):
            if data.get("name") == col_name:
                el = data.find("text")
                if el is None:
                    el = data.find("number")
                return (el.text or "").strip() if el is not None else ""
        return ""

    # ------------------------------------------------------------------
    # Per-type year listing
    # ------------------------------------------------------------------

    async def _get_type_year_docs(
        self, type_name: str, type_path: str, year: int
    ) -> list[dict]:
        """Return all documents for *type_name* + *year* using ReadViewEntries."""
        # --- Step 1: find which expand position corresponds to *year* ---
        cat_url = self._view_entries_url(type_path, start=1, count=200)
        cat_resp = await self.request_service.make_request(cat_url)
        if not cat_resp:
            logger.warning(f"MSAlems | {type_name} | Failed to fetch category list")
            return []

        cat_xml = await cat_resp.text()
        try:
            cat_root = ET.fromstring(cat_xml)
        except ET.ParseError as exc:
            logger.warning(
                f"MSAlems | {type_name} | XML parse error on categories: {exc}"
            )
            return []

        expand_pos: int | None = None
        doc_count: int = 0
        for entry in cat_root.findall("viewentry"):
            for data in entry.findall("entrydata"):
                if data.get("name") == "wano" and data.get("columnnumber") == "0":
                    num_el = data.find("number")
                    if num_el is not None:
                        try:
                            if int(num_el.text) == year:
                                expand_pos = int(entry.get("position", 0))
                                doc_count = int(entry.get("descendants", 0))
                        except (ValueError, TypeError):
                            pass

        if expand_pos is None:
            return []

        if doc_count == 0:
            return []

        # --- Step 2: fetch all document entries in one request ---
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

        docs_xml = await docs_resp.text()
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
            summary = self._entry_text(entry, "wementa")
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

    async def _get_doc_data(self, doc_info: dict) -> dict | None:
        """Fetch and convert an individual document page to markdown.

        Domino documents use ``<font>``/``<table>``/``<ul>`` instead of ``<p>``
        tags.  We strip the navigation ``<form>`` and use the remaining body
        content for conversion.
        """
        doc_info = dict(doc_info)
        url = doc_info.pop("html_link")

        if self._is_already_scraped(url, doc_info.get("title", "")):
            return None

        soup = await self.request_service.get_soup(url)
        if not soup:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=url,
                error_message="Failed to retrieve document page",
            )
            return None

        # All document content sits inside a single <form action=""> element.
        # Only strip the action-bar navigation table (border="1") and non-content
        # elements; do NOT remove the form itself.
        for tag in soup.find_all(["script", "applet"]):
            tag.decompose()
        for table in soup.find_all("table", border="1"):
            table.decompose()

        body = soup.find("body")
        if not body:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=url,
                error_message="No <body> tag found in document page",
            )
            return None

        html_string = self._wrap_html(body.decode_contents())
        return await self._process_html_doc(doc_info, html_string, url)

    # ------------------------------------------------------------------
    # Year-level scrape (overrides base class)
    # ------------------------------------------------------------------

    async def _scrape_year(self, year: int) -> list[dict]:
        """List all types for *year* via ReadViewEntries, then process docs."""
        situation = next(iter(self.situations), "Não consta")

        # Fetch document listings for all types concurrently
        listing_tasks = [
            self._get_type_year_docs(type_name, type_path, year)
            for type_name, type_path in self.types.items()
        ]
        listing_results = await self._gather_results(
            listing_tasks,
            context={"year": year, "type": "NA", "situation": situation},
            desc=f"MATO GROSSO DO SUL | {year} | listing",
        )

        by_type: dict[str, list] = defaultdict(list)
        for i, (type_name, _) in enumerate(self.types.items()):
            docs = listing_results[i] if i < len(listing_results) else None
            if docs:
                for doc in docs:
                    # Filter already-scraped before processing
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
        return self._flatten_results(valid)
