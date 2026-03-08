# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
uv sync                              # install deps
uv sync --group dev                  # install dev deps
uv run playwright install chromium   # install browser (needed by 3 state scrapers)

uv run main.py                       # run enabled scrapers
uv run main.py --scrapers AcreLegis Conama  # run specific scrapers by short name or class name
uv run main.py --list                # list all scrapers and their on/off status
uv run main.py --verbose             # show all info/debug logs (default shows only warnings + tqdm bars)
uv run main.py --overwrite           # ignore resume logic, re-scrape everything
```

## Lint & Format

```bash
uv run ruff check --fix src/ main.py
uv run ruff format src/ main.py
uv run pre-commit run --all-files     # trailing-whitespace, end-of-file-fixer, ruff, pytest
```

```bash
uv run pytest tests/                          # run all tests (including integration)
uv run pytest tests/test_core.py             # run a single test file
uv run pytest tests/ -m "not integration"    # skip live-network tests
uv run pytest tests/ -m integration          # run only integration tests (requires network)
```

Tests use `pytest-asyncio` with `asyncio_mode = auto`. Integration tests are marked `@pytest.mark.integration` and make real network requests — deselect them in CI with `-m "not integration"`. Per-scraper tests (`tests/test_<state>_scraper.py`) instantiate scrapers via `object.__new__` to avoid triggering I/O in `__init__`. Pre-commit runs only non-integration tests (`-m "not integration"`) on every commit.

## Architecture

### Class Hierarchy

```
BaseScraper                          # src/scraper/base/scraper.py — async HTTP, PDF/OCR, markdown, save/resume
  └─ StateScraper                    # same file — sets STATE_LEGISLATION_SAVE_DIR default
       ├─ SAPLBaseScraper            # src/scraper/base/sapl_scraper.py — for SAPL REST API sites (Paraíba, Piauí, Roraima)
       └─ (27 state scrapers)       # src/scraper/state_legislation/<state>.py
  └─ CamaraDepScraper               # federal
  └─ ConamaScraper, ICMBioScraper   # regulatory bodies
```

All scrapers inherit from `BaseScraper`. State scrapers go through `StateScraper`. States using the SAPL REST API extend `SAPLBaseScraper`.

### Services (composed into BaseScraper)

- **RequestService** (`src/services/request/service.py`) — `aiohttp` with per-scraper `RateLimiter`, retries via `tenacity`, optional proxy rotation. On failure, `make_request` / `get_soup` return a **falsy** `FailedRequest` sentinel (with `.url`, `.status`, `.reason`) instead of `None` — always use `if not resp:` (not `is None`) to check for errors.
- **LLMOCRService** (`src/services/ocr/llm.py`) — renders PDF pages to PNG via PyMuPDF, sends to LLM vision model. Provider clients live in `src/services/ocr/clients/` and implement the `LLMClient` protocol (`src/services/ocr/protocol.py`): `OpenAIClient`, `BedrockClient`, `SnowflakeClient`.
- **BrowserService** (`src/services/browser/playwright.py`) — Playwright page pool for JS-rendered sites. Used by Maranhão, Paraná, and Pernambuco.
- **ProxyService** (`src/services/proxy/service.py`) — proxy rotation from file or HTTP endpoint.
- **FileSaver** (`src/database/saver.py`) — async JSON persistence via `aiofiles`. Saves documents grouped by year into `data.json` files with document-level resume support. MHTML capture is injected via `mhtml_capture_fn` callback (not owned).

### Concurrency Model

- **Years** — scraped **sequentially** (one year completes before next).
- **Types/Situations within a year** — scraped **concurrently** via `asyncio.gather()`.
- **Pages and documents** — scraped **concurrently** via `_gather_results()` (wraps `asyncio.gather` with error filtering and progress bars).
- **Rate limiting** — each scraper has its own `RateLimiter` for HTTP (via `rps` param). All scrapers share a single `RateLimiter` for LLM API calls.
- **CPU-bound work** (PDF rendering) — offloaded via `asyncio.to_thread()` / `run_in_thread()`.

### Adding a New State Scraper

1. Create `src/scraper/state_legislation/<state>.py`.
2. Subclass `StateScraper` (or `SAPLBaseScraper` for SAPL API sites).
3. Define module-level `TYPES` dict/list and `SITUATIONS` list.
4. Implement `_format_search_url()`, `_get_docs_links()`, `_get_doc_data()`, and `_scrape_type()`.
5. Export the class from `src/scraper/state_legislation/__init__.py`.
6. Add a `ScraperConfig` entry in `main.py` → `build_scraper_configs()`.

### Key Patterns

- Use `_process_documents()` to run `_get_doc_data` → `_with_save` → `_gather_results` in one call (replaces 5-line boilerplate). For scrapers passing extra kwargs to `_get_doc_data`, use `doc_data_kwargs` or `doc_data_fn`.
- Use `_save_doc_result()` to persist documents immediately (supports raw file saving + `data.json` append).
- Use `_save_doc_error()` to log document-level failures.
- Use `_is_already_scraped()` for resume support — checks `(document_url, title)` keys loaded by `_load_scraped_keys()`.
- Use `_get_markdown()` for flexible content-to-markdown conversion (accepts url, response, stream, or html_content).
- Use `_download_and_convert()` when you also need the raw bytes (e.g., for saving source PDFs).
- LLM configuration uses the `LLMConfig` dataclass (`src/services/ocr/config.py`), passed directly to `BaseScraper` and `LLMOCRService`.
- Environment variables are centralized in `src/config.py` — import `SAVE_DIR`, `STATE_LEGISLATION_SAVE_DIR`, `ERROR_LOG_DIR`, etc. from there.
- The LLM OCR prompt is in Portuguese and defined as the module-level constant `DEFAULT_LLM_PROMPT` in `src/scraper/base/scraper.py`. Override `llm_prompt` only if a scraper needs different extraction instructions.

## Environment

Python >= 3.12, managed with `uv`. Config via `.env` (copy `.env.example`). Key variables: `LLM_PROVIDER`, `LLM_API_KEY`, `LLM_MODEL`, `PROVIDER_BASE_URL`, `SAVE_DIR`, `STATE_LEGISLATION_SAVE_DIR`.

LLM providers: `openai` (OpenAI-compatible API, default), `bedrock` (AWS Bedrock Converse), `snowflake` (Snowflake Cortex).
