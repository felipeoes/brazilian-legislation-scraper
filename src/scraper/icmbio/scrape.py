"""ICMBio scraper using the PowerBI querydata REST API.

Fetches normative acts from the ICMBio (Instituto Chico Mendes de Conservação
da Biodiversidade) PowerBI dashboard by calling the underlying querydata
endpoint directly — no browser required.

PowerBI dashboard:
    https://app.powerbi.com/view?r=eyJrIjoiMGQ0ODRhY2QtYThmNy00NmYwLWFkOGYtOWJmZDU0ODZlZWUzIiwidCI6ImMxNGUyYjU2LWM1YmMtNDNiZC1hZDljLTQwOGNmNmNjMzU2MCJ9
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from bs4 import BeautifulSoup
from loguru import logger

from src.scraper.base.scraper import BaseScraper

# ── PowerBI API constants ──────────────────────────────────────────
QUERY_URL = "https://wabi-brazil-south-api.analysis.windows.net/public/reports/querydata?synchronous=true"
RESOURCE_KEY = "0d484acd-a8f7-46f0-ad8f-9bfd5486eee3"
MODEL_ID = 10709107
DATASET_ID = "38b8dbf3-82ac-48b0-8572-174917b99a75"
REPORT_ID = "eaa22fb4-6467-401d-8a8e-19d69ef0e490"
VISUAL_ID = "e996abc337e7ea22a270"
PAGE_SIZE = 500

# Columns we request from the DINFI_atos entity.
# Order here determines the index in the DSR response.
COLUMNS = [
    "condicao",  # 0 – Situação
    "publicacao",  # 1 – Publication date (epoch ms)
    "instrumento",  # 2 – DOU edition/section/page
    "ementa",  # 3 – Summary
    "link_dou",  # 4 – DOU URL
    "ato",  # 5 – Title / act identifier
    "assunto",  # 6 – Subject
]

TYPES = {
    "Instrução Normativa": "INSTRUÇÕES NORMATIVAS",
    "Portaria": "PORTARIAS",
    "Outros Atos": "OUTROS ATOS",
}

VALID_SITUATIONS = [
    "em vigência",
    "em vigência com alteração",
    "não consta",
]
INVALID_SITUATIONS = ["revogado", "vencido", "sem efeito"]

SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class ICMBioScraper(BaseScraper):
    """Scraper for ICMBio normative acts via the PowerBI querydata API.

    Instead of driving a Playwright browser through the PowerBI dashboard,
    this scraper calls the underlying REST API directly.  Each API request
    returns up to ``PAGE_SIZE`` (500) rows of structured data — including
    the ``link_dou`` field — so there is no need for clipboard-based URL
    extraction or virtual-scroll DOM interaction.
    """

    def __init__(
        self,
        base_url: str = "https://app.powerbi.com/view?r=eyJrIjoiMGQ0ODRhY2QtYThmNy00NmYwLWFkOGYtOWJmZDU0ODZlZWUzIiwidCI6ImMxNGUyYjU2LWM1YmMtNDNiZC1hZDljLTQwOGNmNmNjMzU2MCJ9",
        **kwargs,
    ):
        # Remove browser-related kwargs so they don't reach BaseScraper
        kwargs.pop("use_browser", None)
        kwargs.pop("multiple_pages", None)
        kwargs.pop("headless", None)
        super().__init__(
            base_url,
            name="ICMBIO",
            types=TYPES,
            situations=SITUATIONS,
            **kwargs,
        )

    # ── Query building ─────────────────────────────────────────────

    @staticmethod
    def _build_query_payload(
        restart_tokens: list | None = None,
    ) -> dict:
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
                                                "Direction": 2,  # descending (newest first)
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
                        "Sources": [
                            {
                                "ReportId": REPORT_ID,
                                "VisualId": VISUAL_ID,
                            }
                        ],
                    },
                }
            ],
            "modelId": MODEL_ID,
        }

    # ── DSR response parsing ───────────────────────────────────────

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
        """
        ph_rows = ds["PH"][0]["DM0"]
        value_dicts: dict[str, list] = accumulated_dicts or ds.get("ValueDicts", {})

        # Parse schema from the first row that has an 'S' key
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

            # Build current row: R bitmask says which cols repeat from prev
            current: list = list(prev_values)
            c_idx = 0
            for i in range(len(COLUMNS)):
                if r_flag & (1 << i):
                    # Repeated from previous row
                    pass
                else:
                    if c_idx < len(c):
                        current[i] = c[c_idx]
                        c_idx += 1

            # Resolve ValueDict references
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
                            # Unresolved index — treat as empty
                            val = ""

                # Convert epoch-ms timestamps to date strings
                if col == "publicacao" and isinstance(val, (int, float)):
                    try:
                        dt = datetime.fromtimestamp(val / 1000, tz=timezone.utc)
                        val = dt.strftime("%m/%d/%Y")
                    except (OSError, ValueError):
                        pass

                # Ensure all values are strings (not leftover ints)
                if val is None:
                    val = ""

                row_dict[col] = val

            parsed.append(row_dict)
            prev_values = current

        return parsed

    # ── API fetching ───────────────────────────────────────────────

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

        response = await self.request_service.make_request(
            url=QUERY_URL,
            method="POST",
            json=payload,
            headers=headers,
            timeout=60,
        )

        if not response or response.status != 200:
            status = response.status if response else "No response"
            logger.error(f"PowerBI API error: {status}")
            return None, None, True

        # Response content-type is text/plain, so read as text then parse
        text = await response.text()
        data = json.loads(text)

        dsr = data["results"][0]["result"]["data"]["dsr"]
        ds = dsr["DS"][0]

        is_complete = ds.get("IC", True)
        next_tokens = ds.get("RT") if not is_complete else None

        return ds, next_tokens, is_complete

    async def _fetch_all_rows(self) -> list[dict]:
        """Paginate through the PowerBI API to collect all rows."""
        all_rows: list[dict] = []
        accumulated_dicts: dict[str, list] = {}
        restart_tokens = None
        page = 0

        while True:
            page += 1
            ds, next_tokens, is_complete = await self._fetch_page(restart_tokens)

            if ds is None:
                break

            # Merge this page's ValueDicts into the accumulated set
            page_dicts = ds.get("ValueDicts", {})
            for key, values in page_dicts.items():
                if key not in accumulated_dicts:
                    accumulated_dicts[key] = []
                accumulated_dicts[key].extend(values)

            rows = self._parse_dsr_rows(ds, accumulated_dicts)
            all_rows.extend(rows)

            if self.verbose:
                logger.info(
                    f"ICMBIO | Fetched page {page}: {len(rows)} rows "
                    f"(total: {len(all_rows)}, complete: {is_complete})"
                )

            if is_complete or not next_tokens:
                break

            restart_tokens = next_tokens

        return all_rows

    # ── Row → document conversion ─────────────────────────────────

    @staticmethod
    def _classify_type(title: str) -> str:
        """Determine the document type from its title."""
        lower = title.lower()
        if "instrução normativa" in lower or lower.startswith("in "):
            return "Instrução Normativa"
        if "portaria" in lower:
            return "Portaria"
        return "Outros Atos"

    def _row_to_doc(self, row: dict) -> dict | None:
        """Convert a parsed API row into a standardized document dict.

        Returns None if the row has no usable DOU link.
        """
        parts = (row.get("link_dou") or "").split()
        link_dou = parts[0].strip() if parts else ""
        if not link_dou or "in.gov.br" not in link_dou:
            return None

        title = row.get("ato", "")
        date_str = row.get("publicacao", "")

        # Extract year from date string (MM/DD/YYYY)
        try:
            year = int(date_str.strip().split("/")[-1])
        except (ValueError, IndexError):
            return None

        situation = row.get("condicao") or ""
        if not situation or situation in ("(Blank)", "(Em branco)"):
            situation = "não consta"

        return {
            "year": str(year),
            "title": title,
            "summary": row.get("ementa", ""),
            "type": self._classify_type(title),
            "document_url": link_dou,
            "situation": situation.lower().strip(),
            "subject": row.get("assunto", ""),
            "publication_info": row.get("instrumento", ""),
        }

    # ── Document content fetching ─────────────────────────────────

    async def _get_doc_data(self, doc_info: dict) -> dict | None:
        """Fetch the DOU page HTML and convert to markdown."""
        document_url = doc_info.get("document_url", "")
        doc_title = doc_info.get("title", "Sem título")

        if document_url and self._is_already_scraped(document_url, doc_title):
            return None

        year = doc_info.get("year", "")
        situation = doc_info.get("situation", "")
        doc_type = doc_info.get("type", "")

        if not document_url:
            await self._save_doc_error(
                title=doc_title,
                year=year,
                situation=situation,
                norm_type=doc_type,
                html_link="",
                error_message="Invalid or missing document URL",
            )
            return None

        # Fetch the DOU page via RequestService (no browser needed)
        response = await self.request_service.make_request(document_url)
        if not response or response.status != 200:
            status = response.status if response else "No response"
            logger.warning(f"Failed to fetch {document_url}: {status}")
            await self._save_doc_error(
                title=doc_title,
                year=year,
                situation=situation,
                norm_type=doc_type,
                html_link=document_url,
                error_message=f"HTTP {status}",
            )
            return None

        html = await response.text()
        soup = BeautifulSoup(html, "html.parser")
        text_div = soup.find("div", class_="texto-dou")

        if not text_div:
            logger.warning(f"div.texto-dou not found at {document_url}")
            await self._save_doc_error(
                title=doc_title,
                year=year,
                situation=situation,
                norm_type=doc_type,
                html_link=document_url,
                error_message="Could not find div.texto-dou in page",
            )
            return None

        html_string = self._wrap_html(text_div.prettify())

        text_markdown = await self._get_markdown(html_content=html_string)
        if not text_markdown or not text_markdown.strip():
            logger.warning(f"Empty markdown from {document_url}")
            await self._save_doc_error(
                title=doc_title,
                year=year,
                situation=situation,
                norm_type=doc_type,
                html_link=document_url,
                error_message="Empty markdown after HTML conversion",
            )
            return None

        # Check for server error pages
        if "the requested url was not found" in text_markdown.lower():
            await self._save_doc_error(
                title=doc_title,
                year=year,
                situation=situation,
                norm_type=doc_type,
                html_link=document_url,
                error_message="Document URL returned 'not found' message",
            )
            return None

        result = {
            **doc_info,
            "text_markdown": text_markdown,
            "_raw_content": html_string.encode("utf-8"),
            "_content_extension": ".html",
        }
        await self._save_doc_result(result)
        return result

    # ── Year-level orchestration ───────────────────────────────────

    async def _scrape_year(self, year: str):
        """Scrape all ICMBio documents for a specific year."""
        year_int = int(year)

        # Fetch all rows from the API once (the dataset is not very large)
        if not hasattr(self, "_all_rows"):
            logger.info("ICMBIO | Fetching all rows from PowerBI API...")
            self._all_rows = await self._fetch_all_rows()
            logger.info(f"ICMBIO | Total rows fetched: {len(self._all_rows)}")

        # Filter rows for this year and convert to doc dicts
        docs = []
        for row in self._all_rows:
            doc = self._row_to_doc(row)
            if doc and int(doc["year"]) == year_int:
                docs.append(doc)

        if not docs:
            logger.warning(f"No documents found for year {year}")
            return []

        logger.info(f"ICMBIO | Year {year}: {len(docs)} documents to process")

        # Process documents concurrently
        tasks = [self._get_doc_data(doc) for doc in docs]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": "NA", "situation": "NA"},
            desc=f"ICMBIO | Processing docs for {year}",
        )

        results = [r for r in valid_results if r is not None]

        if self.verbose:
            logger.info(f"Finished scraping for Year: {year} | Results: {len(results)}")

        return results
