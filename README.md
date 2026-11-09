# Brazilian Legislation Scraper

Async scrapers for Brazilian federal legislation, all 27 state jurisdictions, CONAMA, and ICMBio. Documents are validated, stored in SQLite, and exported as JSON or JSONL. PDFs and HTML become Markdown with embedded images; scanned documents can use LLM OCR.

This project does not scrape leisestaduais.com.br.

## Install

Requires Python 3.14 or newer and [uv](https://docs.astral.sh/uv/).

```bash
uv sync --group dev
uv run playwright install chromium
cp .env.example .env
```

Most sources are scraped over plain HTTP. Chromium is launched lazily, only by the few sources that still need a rendered page.

## Usage

```bash
uv run main.py list
uv run main.py run -s Acre Conama --year 2024
uv run main.py run -s PB PI RR
uv run main.py run --all
uv run main.py run --all --jobs 8
uv run main.py stats
uv run main.py run -s RS --year 2024 --overwrite
uv run main.py run -s Pernambuco --year 2024 --verbose
uv run main.py export -s Acre --format jsonl --out exports/acre
uv run main.py export -s Acre --format json
uv run main.py import-json -s Acre
```

Names, class names, and the aliases shown by `list` are case insensitive. Omit `--year` to scrape the registered date range. A run returns exit code 1 if any listing, document, or cleanup fails; errors are recorded in SQLite. With no command, the CLI prints usage and exits 2.

When several scrapers are selected, each runs in its own process, so CPU-heavy parsing in one source never stalls another. `--jobs` limits how many run at once (default: all selected, up to the CPU count). Each child has its own OCR limit (`LLM_CONCURRENCY`) and PDF worker pool. A single scraper runs in-process.

`stats` prints a Markdown table of each archive's latest run. It covers wall time, listing time, documents per minute, time per document fetch (p50/p95/max, including rate-limit waits), HTTP requests, retries and megabytes, browser navigations, and LLM requests. The numbers come from `runs.stats`.

Resume checks use the original listing URL and title, even when the fetched document has a different URL or publication year. `--overwrite` re-scrapes the selected years and replaces each stored document as it is saved; nothing is deleted up front, so documents removed from the source stay in the archive. Acre, Rio de Janeiro, and ICMBio enumerate their yearless listings once and filter by each document's year. Amapá's eLegis filters by the year of the originating bill, so `--year Y` lists norms from bills filed in Y and files each under its publication year; `--all` covers every norm once. Other sources list years in order through one shared worker queue. Constitutions retain their promulgation year.

## Configuration

All project settings are read in `src/config.py`, including `.env`.

| Variable | Purpose | Default |
| --- | --- | --- |
| `SAVE_DIR` | Root of scraper archives | `outputs/legislation` |
| `STATE_LEGISLATION_SAVE_DIR` | Optional state archive root | `SAVE_DIR` |
| `SPECIFIC_LEGISLATION_SAVE_DIR` | Optional CONAMA/ICMBio archive root | `SAVE_DIR` |
| `LOG_DIR` | Per-scraper runtime logs | `logs/legislation` |
| `LLM_MODEL` | PydanticAI model string, or comma-separated fallback models | Empty: OCR disabled |
| `LLM_PDF_INPUT` | Send PDF chunks instead of rendered PNG pages | `false` |
| `LLM_BATCH_SIZE` | Pages per OCR request | `5` |
| `LLM_CONCURRENCY` | Maximum concurrent OCR batches per scraper process | `8` |
| `PDF_WORKERS` | PDF text-extraction processes per scraper process (multi-scraper runs set it from `--jobs`) | `min(8, CPUs)` |
| `PROXY_FILE_PATH` | Optional newline-separated proxy list (`scheme://host:port`), used by Paraná only. Each proxy gets Paraná's own rate (0.3 rps), so load is proxies × 0.3 rps; list only working proxies with distinct exit IPs | Empty |
| `PROXY_ENDPOINT` | Optional proxy-list endpoint | Empty |

For example, set `LLM_MODEL=openai:gpt-4.1-mini` and `OPENAI_API_KEY`. OpenAI-compatible services also use `OPENAI_BASE_URL`. Other model strings use their standard provider credentials, such as `AWS_*`, `AWS_BEARER_TOKEN_BEDROCK`, `ANTHROPIC_API_KEY`, or `GOOGLE_API_KEY`. Installed PydanticAI provider extras cover OpenAI, Bedrock, Anthropic, and Google. Model selection and whether PDF input is supported depend on the provider.

Each scraper process has one OCR agent and semaphore. A failed OCR batch rejects the whole document; successful blank batches are allowed. A PDF whose text layer is unreadable goes to OCR when a model is configured; without a model the noisy text is kept. Unreadable means mostly odd-shaped tokens and few common Portuguese words, as in old scans with a noisy OCR layer. On the 2012 archives this sends 13 of about 11,700 PDFs, 111 pages, to OCR. Usage per model is stored in `runs.llm_usage`.

## Storage and export

Each scraper writes `<archive root>/<SCRAPER>/documents.db` with `documents`, `errors`, and `runs` tables. SQLite uses WAL and atomic transactions. Raw artifacts live under `YEAR/docs/` with fixed-length content-addressed filenames and are written by atomic replacement. `file_path` is relative to the scraper's archive directory, so archives can be moved. Rows written by older versions may hold absolute paths. Replacing a document removes its previous raw file, and a failed write removes the file it just wrote. The store never deletes files it did not write, and an interrupted write never replaces a previous document's raw content.

`export` streams records to `YEAR/data.jsonl` or `YEAR/data.json`. It includes document metadata, `text_markdown`, source-specific extra fields, `file_path`, and the resume key (`ref_url`, `ref_title`) so `import-json` can restore it; raw bytes and listing-only keys starting with `_` are excluded. The default export directory is `<archive root>/<SCRAPER>/export`. With multiple scrapers and `--out`, each scraper gets a separate subdirectory.

`import-json` reads `YEAR/data.json` (a list, or the legacy `{"summary", "documents"}` file) and `YEAR/shards/*.json` from the selected scraper's archive directory. It validates each record, preserves existing `file_path` values, and never deletes its inputs. Invalid records are skipped, recorded in the `errors` table, and make the command exit 1; unreadable files fail loudly. Rerunning safely replaces duplicate keys. Legacy Câmara, DF, and Ceará archives were keyed by the fetched URL, not the listing URL that resume uses, so re-scrape those instead of importing them. The same applies to archives made before the Amapá, Pará, São Paulo, Sergipe, and Rio de Janeiro rewrites, whose sources or titles changed.

## Architecture

```text
main.py                      CLI, one process per scraper, run stats
src/config.py                Environment settings
src/registry.py              30 scraper registrations and runtime defaults
src/core/document.py         Validated Document model and listing metadata type
src/core/store.py            SQLite, resume keys, errors, summaries, import/export
src/core/http.py             Buffered responses, rate limiting, retries
src/core/browser.py          Lazy, bounded Playwright page pool
src/core/ocr.py               Shared PydanticAI OCR
src/core/convert.py           HTML/PDF conversion and content helpers
src/core/scraper.py           Bounded listing → fetch → save pipeline
src/core/sapl.py              Shared SAPL API scraper
src/core/images.py           Image inlining
src/core/proxy.py            Proxy rotation
src/scrapers/states/<uf>.py   State-specific listing and document extraction
src/scrapers/federal/         Câmara dos Deputados
src/scrapers/environment/     CONAMA and ICMBio
```

Scrapers implement `list_type(year, name, value)` (called once per type, so one failing type doesn't stop the others) or override `list_documents(year)`, both async generators of lightweight metadata, and `fetch(ref)` returning a `Document`. Constructors only accept dependencies and set attributes. The base run loop owns persistence, error reporting, and resume; fetch workers never collect whole archives in memory. One queue capped at twice the worker count serves all years. Stateful pagination remains sequential; stateless page requests use bounded batches. HTTP requests and browser navigation share the scraper's rate limiter. Browser pools never exceed their configured page limit.

Digital PDFs use pymupdf4llm, then PyMuPDF plain text, then optional OCR. Text extraction runs in a pool of worker processes, because pymupdf4llm holds the GIL. A crashed worker fails only its own document. Word documents convert to HTML with mammoth. File types are detected from content (`%PDF`, DOCX zip, OLE2, HTML) before names or headers; legacy Word `.doc` files fail with a clear error, since no converter is bundled. HTML uses the lxml parser and html-to-markdown, with image sources inlined as base64 data URIs. Spacer images under 100 bytes are dropped. Responses decode as browsers do: the declared charset (header, BOM or `<meta>`), otherwise UTF-8 when valid and windows-1252 when not; ISO-8859-1 labels also mean windows-1252.

## Development

```bash
uv run ruff check src main.py tests
uv run ruff format src main.py tests
uv run pytest tests -m "not integration" -n 4
uv run pytest tests -m integration
uv run pre-commit run --all-files
```

Pre-commit runs whitespace, large-file, Ruff, and offline pytest checks with a 60-second per-test timeout. Integration tests make real network requests and may require Chromium. Unit tests inject buffered responses and fake browser/OCR dependencies through normal constructors.

To add a scraper, subclass `BaseScraper` or `SAPLBaseScraper`, implement listing and fetch, and register a `ScraperSpec`. Keep persistence in the base class. Add parser fixtures and a fetch test that returns a validated `Document`.

## License

See [LICENSE](LICENSE).
