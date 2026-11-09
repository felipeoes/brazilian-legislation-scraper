# Repository development guide

## Commands

Python 3.14+. Install with `uv sync --group dev` and `uv run playwright install chromium`.

```bash
uv run main.py list
uv run main.py run -s Acre Conama --year 2024
uv run main.py run --all --jobs 16
uv run main.py stats
uv run main.py export -s Acre --format jsonl --out exports/acre
uv run main.py import-json -s Acre
uv run ruff check --fix src main.py tests
uv run ruff format src main.py tests
uv run pytest tests -m "not integration" -n 4
uv run pytest tests -m integration
uv run pre-commit run --all-files
```

`run` requires `-s` or `--all`. It supports `--year`, `--overwrite`, `--verbose`, and `--jobs`. Several scrapers run as one child process each (`--jobs` bounds them, default all up to the CPU count; each child gets the full `LLM_CONCURRENCY` and its own `PDF_WORKERS`); a single scraper runs in-process. Any scraper or cleanup failure returns exit 1. `stats` prints each archive's last run (`runs.stats`) as a Markdown table. Pre-commit excludes integration tests and uses a 60-second test timeout.

## Architecture

- `src/registry.py`: `ScraperSpec` entries (`state()` for state scrapers) for 30 scrapers, aliases, start years, rates, workers, OCR/proxy settings, and storage roots.
- `src/core/document.py`: Pydantic `Document`; required metadata and validated Markdown, excluded raw bytes, extra source fields allowed. No dictionary accessor shims.
- `src/core/store.py`: SQLite per scraper; atomic document transactions, indexed resume keys and summaries, errors and runs (with `stats` performance JSON), streaming JSON/JSONL export. Raw files are content-addressed, atomically replaced, and stored as paths relative to the archive directory; replacing a row (or a failed write) unlinks only raw files the store wrote. Database operations run in threads under one async lock.
- `src/core/scraper.py`: `BaseScraper` implements bounded queue workers and owns save/resume/error handling. `async with scraper` owns resource cleanup through `AsyncExitStack`. One bounded queue serves all years, so workers never wait at a year boundary. A failed year, type, or listing page is recorded and the rest continue. Yearless sources enumerate once. Resume uses original listing URL/title globally, including documents filed under another year.
- `src/core/sapl.py`: shared SAPL logic for Paraíba, Piauí, and Roraima.
- `src/core/http.py`: `HttpClient.request()` returns a fully buffered `Response` (`content`, `text`, `json()`, `soup()`, `filename`, `content_type`). HTTP failures raise `HttpError`; never test responses for truthiness. Reads happen inside the tenacity retry loop. Charsets resolve like browsers: header, BOM, then `<meta>` (HTML only); undeclared bytes decode as UTF-8 when valid, else windows-1252, and Latin-1/ASCII labels mean windows-1252. `pause_on_403` (per `ScraperSpec`) makes every request wait out a site's client block instead of failing against it (Paraná). `http.stats` counts requests, retries, and bytes.
- `src/core/browser.py`: one lazy bounded `BrowserPool` per scraper, shared HTTP navigation limiter, no overflow pages. Use `async with browser.page()` and `browser.goto()`, or `browser.snapshot()` for soup plus MHTML.
- `src/core/ocr.py`: one PydanticAI `OCR` per scraper process, batch semaphore, per-model/per-run usage, all-or-nothing document conversion when a batch fails. Blank successful batches are permitted.
- `src/core/convert.py`: explicit-dependency functions for HTML/PDF conversion and cleaning. PDF chain: pymupdf4llm → PyMuPDF plain text → optional OCR, which also replaces unreadable text layers (`readable_text`). PDF text extraction runs in a process pool (`_in_process`, `PDF_WORKERS`) because pymupdf4llm holds the GIL; worker functions must not log. Unit tests run it inline (`tests/conftest.py`). DOCX converts through mammoth to HTML; legacy OLE2 `.doc` fails explicitly. `sniff_extension` trusts content over file names.
- `src/core/images.py`: concurrent image fetching and base64 inlining; `src/core/proxy.py`: round-robin proxy rotation, used only by specs with `needs_proxy` (Paraná). Each proxy gets its own `rps` limiter and verified TLS; a proxy that answers 403 rests for `pause_on_403`, one that fails to connect or answer rests 5 minutes, and with none left requests go direct. `request(use_proxy=False)` forces the direct IP.
- `src/scrapers/states/<uf>.py`, `federal/camara.py`, `environment/{conama,icmbio}.py`: source-specific extraction.

## Scraper contract

Constructors only set attributes; inject `store`, `http`, `years`, optional `ocr`/`browser`, `workers`, and `overwrite`. Implement:

```python
async def list_type(self, year: int | None, name: str, value: Any) -> AsyncIterator[dict]: ...  # once per `types` item
async def list_documents(self, year: int | None) -> AsyncIterator[dict]: ...  # override instead when listing isn't per type
async def fetch(self, ref: dict) -> Document: ...
```

Listing refs contain stable `document_url` and `title`, plus year/type/situation and source-specific fields needed by fetch. Build `Document` only in fetch. The base persists results and records exceptions; a `None` fetch result is an error, so skip unwanted refs while listing. Ref keys starting with `_` (`_pdf_link`, `_html_link`) are listing-only and never stored. Use `setup()` for initialization requiring I/O. Use `download`, `markdown`, `fetch_pdf`, and `paginate` helpers where they fit. Keep session/JSF/ASP.NET pagination sequential.

## Style and tests

Annotate all signatures, use built-in generics and `X | Y`, and format with Ruff. Python 3.14 defers annotations; future annotation imports are unnecessary. No inline lint suppressions without a concrete reason. Centralize project environment settings in `src/config.py`; provider credentials use PydanticAI's standard environment variables.

Do not swallow errors or implement scraper retry loops. Let HTTP/browser retries handle transport failures. Reject malformed listing responses instead of treating them as empty years. Optional source alternatives may catch specific failures, but a document with no valid alternative must fail visibly.

Preserve images: keep `embed_images=True` for PDF conversion, inline HTML images before html-to-markdown, and retain base64 Markdown images during cleaning. Use BeautifulSoup's `lxml` parser.

Tests construct scrapers normally with fakes (`tests/conftest.py`); no `object.__new__`. Async pytest mode is automatic. Mark every live-network test `integration`. Add focused regression tests for parsing, fetch metadata, failure propagation, resume, and bounded resources. Avoid running full archives during unit tests.

## Configuration and data

See `.env.example` and README. `LLM_MODEL` accepts PydanticAI model strings or a comma-separated fallback list; `LLM_PDF_INPUT`, `LLM_BATCH_SIZE`, and `LLM_CONCURRENCY` control OCR; `PDF_WORKERS` sizes the PDF process pool. Standard provider credentials include `OPENAI_API_KEY`, `OPENAI_BASE_URL`, AWS settings, `ANTHROPIC_API_KEY`, and `GOOGLE_API_KEY`.

Archives live at `<save_dir>/<NAME>/documents.db`. `import-json` reads `YEAR/data.json` (a list or the legacy `{"summary", "documents"}` file) and shards without deleting inputs, preserves raw file paths, and records invalid rows in `errors` (exit 1). Exports keep `ref_url`/`ref_title` so re-imports resume correctly; legacy Camara, DF, and CE archives lack them, and AP, PA, SP, SE, and RJ keys changed with their rewrites, so re-scrape those instead of importing. `export` streams files to per-year directories. `--overwrite` re-scrapes without deleting anything; each save replaces the old row. Summary queries must never load document bodies.
