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

Chromium is launched lazily for sites that require rendering or MHTML snapshots.

## Usage

```bash
uv run main.py list
uv run main.py run -s Acre Conama --year 2024
uv run main.py run -s PB PI RR
uv run main.py run --all
uv run main.py run -s RS --year 2024 --overwrite
uv run main.py run -s Pernambuco --year 2024 --verbose
uv run main.py export -s Acre --format jsonl --out exports/acre
uv run main.py export -s Acre --format json
uv run main.py import-json -s Acre
```

Names, class names, and the aliases shown by `list` are case insensitive. Omit `--year` to scrape the registered date range. A run returns exit code 1 if any listing, document, or cleanup fails; errors are recorded in SQLite. With no command, the CLI prints usage and exits 2.

Resume checks use the original listing URL and title, even when the fetched document has a different URL or publication year. `--overwrite` removes stored documents and raw files for the selected years before scraping them again. Acre, Amapá, Rio de Janeiro, and ICMBio enumerate their yearless listings once and filter by each document's year. Other sources scrape years sequentially. Constitutions retain their promulgation year.

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
| `LLM_CONCURRENCY` | Maximum concurrent OCR batches across all scrapers | `8` |
| `PROXY_FILE_PATH` | Optional newline-separated or JSON proxy list | Empty |
| `PROXY_ENDPOINT` | Optional proxy-list endpoint | Empty |

For example, set `LLM_MODEL=openai:gpt-4.1-mini` and `OPENAI_API_KEY`. OpenAI-compatible services also use `OPENAI_BASE_URL`. Other model strings use their standard provider credentials, such as `AWS_*`, `AWS_BEARER_TOKEN_BEDROCK`, `ANTHROPIC_API_KEY`, or `GOOGLE_API_KEY`. Installed PydanticAI provider extras cover OpenAI, Bedrock, Anthropic, and Google. Model selection and whether PDF input is supported depend on the provider.

One OCR agent and semaphore are shared across the run. A failed OCR batch rejects the whole document; successful blank batches are allowed. Usage per model is stored in `runs.llm_usage`.

## Storage and export

Each scraper writes `<archive root>/<SCRAPER>/documents.db` with `documents`, `errors`, and `runs` tables. SQLite uses WAL and atomic transactions. Raw artifacts live under `YEAR/docs/` with fixed-length content-addressed filenames and are written by atomic replacement. An interrupted database write can leave an unreferenced raw file; it cannot replace a previous document's raw content.

`export` streams records to `YEAR/data.jsonl` or `YEAR/data.json`. It includes document metadata, `text_markdown`, source-specific extra fields, and `file_path`; raw bytes and internal resume keys are excluded. The default export directory is `<archive root>/<SCRAPER>/export`. With multiple scrapers and `--out`, each scraper gets a separate subdirectory.

`import-json` reads `YEAR/data.json` and `YEAR/shards/*.json` from the selected scraper's archive directory. It validates each record, preserves existing `file_path` values, and never deletes its inputs. Invalid files fail loudly; records committed before the failure remain available, and rerunning safely replaces duplicate keys.

## Architecture

```text
main.py                      CLI and shared OCR lifecycle
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

Scrapers implement `list_documents(year)` as an async generator of lightweight metadata, and `fetch(ref)` returning a `Document`. Constructors only accept dependencies and set attributes. The base run loop owns persistence, error reporting, and resume; fetch workers never collect whole archives in memory. Each year has a queue capped at twice the worker count. Stateful pagination remains sequential; stateless page requests use bounded batches. HTTP requests and browser navigation share the scraper's rate limiter. Browser pools never exceed their configured page limit.

Digital PDFs use pymupdf4llm, then PyMuPDF plain text, then optional OCR. CPU-heavy PDF work runs in threads. HTML uses the lxml parser and html-to-markdown, with image sources inlined as base64 data URIs.

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
