import json
import re
from collections.abc import AsyncIterator
from datetime import datetime, timezone

from loguru import logger

from src.core.convert import clean_norm_soup
from src.core.document import Document
from src.core.scraper import BaseScraper

QUERY_URL = "https://wabi-brazil-south-api.analysis.windows.net/public/reports/querydata?synchronous=true"
RESOURCE_KEY = "0d484acd-a8f7-46f0-ad8f-9bfd5486eee3"
MODEL_ID = 10709107
DATASET_ID = "38b8dbf3-82ac-48b0-8572-174917b99a75"
REPORT_ID = "eaa22fb4-6467-401d-8a8e-19d69ef0e490"
VISUAL_ID = "e996abc337e7ea22a270"
PAGE_SIZE = 500
COLUMNS = [
    "condicao",
    "publicacao",
    "instrumento",
    "ementa",
    "link_dou",
    "ato",
    "assunto",
]
TYPES = {
    "Instrução Normativa": "INSTRUÇÕES NORMATIVAS",
    "Portaria": "PORTARIAS",
    "Outros Atos": "OUTROS ATOS",
}
_IN_TITLE_RE = re.compile("^in(?:\\s+[a-zà-ÿ/.-]+)*\\s+\\d+", re.IGNORECASE)
_PORTARIA_TITLE_RE = re.compile("^portari[ae]\\w*", re.IGNORECASE)
VALID_SITUATIONS = ["em vigência", "em vigência com alteração", "não consta"]
INVALID_SITUATIONS = ["revogado", "vencido", "sem efeito"]
SITUATIONS = {s: s for s in VALID_SITUATIONS + INVALID_SITUATIONS}


class ICMBioScraper(BaseScraper):
    """Scraper for ICMBio normative acts via the PowerBI querydata API.

    Year start (earliest on source): 2016

    Instead of driving a Playwright browser through the PowerBI dashboard,
    this scraper calls the underlying REST API directly.  Each API request
    returns up to ``PAGE_SIZE`` (500) rows of structured data — including
    the ``link_dou`` field — so there is no need for clipboard-based URL
    extraction or virtual-scroll DOM interaction.
    """

    name = "ICMBIO"
    yearless = True
    types = TYPES
    situations = SITUATIONS
    base_url = "https://app.powerbi.com/view?r=eyJrIjoiMGQ0ODRhY2QtYThmNy00NmYwLWFkOGYtOWJmZDU0ODZlZWUzIiwidCI6ImMxNGUyYjU2LWM1YmMtNDNiZC1hZDljLTQwOGNmNmNjMzU2MCJ9"

    @staticmethod
    def _build_query_payload(restart_tokens: list | None = None) -> dict:
        """Build the PowerBI semantic query payload.

        Args:
            restart_tokens: Pagination tokens from a previous response.
                            ``None`` for the first page.
        """
        select = [
            {
                "Column": {
                    "Expression": {"SourceRef": {"Source": "d"}},
                    "Property": col,
                },
                "Name": f"DINFI_atos.{col}",
            }
            for col in COLUMNS
        ]
        window: dict = {"Count": PAGE_SIZE}
        if restart_tokens:
            window["RestartTokens"] = restart_tokens
        return {
            "version": "1.0.0",
            "queries": [
                {
                    "Query": {
                        "Commands": [
                            {
                                "SemanticQueryDataShapeCommand": {
                                    "Query": {
                                        "Version": 2,
                                        "From": [
                                            {
                                                "Name": "d",
                                                "Entity": "DINFI_atos",
                                                "Type": 0,
                                            }
                                        ],
                                        "Select": select,
                                        "OrderBy": [
                                            {
                                                "Direction": 2,
                                                "Expression": {
                                                    "Column": {
                                                        "Expression": {
                                                            "SourceRef": {"Source": "d"}
                                                        },
                                                        "Property": "publicacao",
                                                    }
                                                },
                                            }
                                        ],
                                    },
                                    "Binding": {
                                        "Primary": {
                                            "Groupings": [
                                                {
                                                    "Projections": list(
                                                        range(len(COLUMNS))
                                                    )
                                                }
                                            ]
                                        },
                                        "DataReduction": {
                                            "DataVolume": 3,
                                            "Primary": {"Window": window},
                                        },
                                        "Version": 1,
                                    },
                                }
                            }
                        ]
                    },
                    "ApplicationContext": {
                        "DatasetId": DATASET_ID,
                        "Sources": [{"ReportId": REPORT_ID, "VisualId": VISUAL_ID}],
                    },
                }
            ],
            "modelId": MODEL_ID,
        }

    @staticmethod
    def _parse_dsr_rows(
        ds: dict, accumulated_dicts: dict[str, list] | None = None
    ) -> list[dict]:
        """Parse the PowerBI DSR (Data Shape Result) into flat row dicts.

        The DSR format uses:
        - ``ValueDicts`` (Dx): lookup tables for repeated string values.
        - ``S`` (schema): defines column types and which ValueDict to use.
        - ``C`` (cluster): the actual cell values per row.
        - ``R`` (repeat bitmask): bits indicating which columns carry over
          from the previous row.

        Args:
            ds: The DS object from the DSR response.
            accumulated_dicts: Merged ValueDicts from all pages seen so far.
                               When non-empty, takes priority over the page's
                               own ``ValueDicts`` so that cross-page index
                               references resolve correctly.
        """
        ph_rows = ds["PH"][0]["DM0"]
        value_dicts: dict[str, list] = (
            accumulated_dicts if accumulated_dicts else ds.get("ValueDicts", {})
        )
        schema: list[dict] = []
        for row in ph_rows:
            if "S" in row:
                schema = row["S"]
                break
        parsed: list[dict] = []
        prev_values: list = [None] * len(COLUMNS)
        for row in ph_rows:
            c = row.get("C", [])
            r_flag = row.get("R", 0)
            current: list = list(prev_values)
            c_idx = 0
            for i in range(len(COLUMNS)):
                if r_flag & 1 << i:
                    pass
                elif c_idx < len(c):
                    current[i] = c[c_idx]
                    c_idx += 1
            row_dict: dict = {}
            for i, col in enumerate(COLUMNS):
                val = current[i]
                if i < len(schema) and "DN" in schema[i]:
                    dn = schema[i]["DN"]
                    if dn in value_dicts and isinstance(val, int):
                        lookup = value_dicts[dn]
                        if val < len(lookup):
                            val = lookup[val]
                        else:
                            val = ""
                if col == "publicacao" and isinstance(val, (int, float)):
                    try:
                        dt = datetime.fromtimestamp(val / 1000, tz=timezone.utc)
                        val = dt.strftime("%m/%d/%Y")
                    except OSError, ValueError:
                        pass
                if val is None:
                    val = ""
                row_dict[col] = val
            parsed.append(row_dict)
            prev_values = current
        return parsed

    async def _fetch_page(
        self, restart_tokens: list | None = None
    ) -> tuple[dict | None, list | None, bool]:
        """Fetch a single page of data from the PowerBI querydata API.

        Returns:
            (ds_dict, next_restart_tokens, is_complete)
            ds_dict is the raw DS object for later parsing.
        """
        payload = self._build_query_payload(restart_tokens)
        headers = {
            "X-PowerBI-ResourceKey": RESOURCE_KEY,
            "Content-Type": "application/json;charset=UTF-8",
        }
        response = await self.http.request(
            url=QUERY_URL, method="POST", json=payload, headers=headers, timeout=60
        )
        text = response.text
        try:
            data = json.loads(text)
            dsr = data["results"][0]["result"]["data"]["dsr"]
            ds = dsr["DS"][0]
        except (json.JSONDecodeError, KeyError, IndexError, TypeError) as e:
            logger.error(f"Unexpected PowerBI response: {e}")
            raise
        is_complete = ds.get("IC", True)
        next_tokens = ds.get("RT") if not is_complete else None
        return (ds, next_tokens, is_complete)

    @staticmethod
    def _classify_type(title: str) -> str:
        """Determine the document type from its title.

        Uses ``instrução normativa`` substring match or a numeric ``IN``
        prefix (e.g. "IN 42, de ...") to identify normative instructions.
        The ``lower.startswith("in ")`` shorthand is intentionally avoided
        because it would match portarias whose titles start with that
        sequence for unrelated reasons.
        """
        lower = " ".join(title.lower().split())
        if "instrução normativa" in lower or _IN_TITLE_RE.match(lower):
            return "Instrução Normativa"
        if _PORTARIA_TITLE_RE.match(lower):
            return "Portaria"
        return "Outros Atos"

    def _row_to_doc(self, row: dict) -> dict | None:
        """Convert a parsed API row into a standardized document dict.

        Returns None if the row has no usable DOU link.
        """
        parts = (row.get("link_dou") or "").split()
        link_dou = parts[0].strip().rstrip(";") if parts else ""
        if not link_dou or "in.gov.br" not in link_dou:
            title = row.get("ato", "<sem título>")
            raw_link = row.get("link_dou", "")
            logger.debug(
                f"ICMBIO | Skipping row without valid DOU link | title={title!r} | link_dou={raw_link!r}"
            )
            return None
        title = row.get("ato", "")
        date_str = str(row.get("publicacao", ""))
        try:
            year = int(date_str.strip().split("/")[-1])
        except ValueError, IndexError:
            return None
        situation = row.get("condicao") or ""
        if not situation or situation in ("(Blank)", "(Em branco)"):
            situation = "não consta"
        return {
            "year": year,
            "title": title,
            "summary": row.get("ementa", ""),
            "type": self._classify_type(title),
            "document_url": link_dou,
            "situation": situation.lower().strip(),
            "subject": row.get("assunto", ""),
            "publication_info": row.get("instrumento", ""),
        }

    async def fetch(self, doc_info: dict) -> Document | None:
        """Fetch the DOU page HTML and convert to markdown."""
        document_url = doc_info.get("document_url", "")
        if not document_url:
            raise ValueError("Invalid or missing document URL")
        try:
            soup, mhtml = await self.browser.snapshot(
                document_url, wait_for_selector="div.texto-dou"
            )
        except Exception as exc:
            logger.warning(f"Failed to fetch {document_url}: {exc}")
            raise ValueError(f"Failed to fetch page: {exc}")
        text_div = soup.find("div", class_="texto-dou")
        if not text_div:
            logger.warning(f"div.texto-dou not found at {document_url}")
            raise ValueError("Could not find div.texto-dou in page")
        for p in text_div.find_all("p", class_="identifica"):
            p.decompose()
        for p in text_div.find_all("p", class_="ementa"):
            p.decompose()
        text_div = clean_norm_soup(
            text_div, unwrap_links=True, remove_disclaimers=True, remove_empty_tags=True
        )
        text_markdown = await self.markdown(html=str(text_div), base_url=document_url)
        if not text_markdown or not text_markdown.strip():
            logger.warning(f"Empty markdown from {document_url}")
            raise ValueError("Empty markdown after HTML conversion")
        if "the requested url was not found" in text_markdown.lower():
            raise ValueError("Document URL returned 'not found' message")
        result = Document(
            **doc_info, text_markdown=text_markdown, raw=mhtml, raw_ext=".mhtml"
        )
        return result

    async def list_documents(self, year: int | None) -> AsyncIterator[dict]:
        dictionaries: dict[str, list] = {}
        restart = None
        seen_tokens = set()
        while True:
            ds, next_tokens, complete = await self._fetch_page(restart)
            if ds is None:
                raise ValueError("PowerBI listing returned no dataset")
            for key, values in ds.get("ValueDicts", {}).items():
                dictionaries.setdefault(key, []).extend(values)
            for row in self._parse_dsr_rows(ds, dictionaries):
                ref = self._row_to_doc(row)
                if ref:
                    yield ref
            if complete:
                break
            token_key = json.dumps(next_tokens)
            if not next_tokens or token_key in seen_tokens:
                raise ValueError("PowerBI pagination did not advance")
            seen_tokens.add(token_key)
            restart = next_tokens
