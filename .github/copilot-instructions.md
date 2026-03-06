# Copilot Instructions

## Build & Run

```bash
uv sync                              # install deps
uv sync --group dev                  # install dev deps
uv run playwright install chromium   # install browser (needed by 3 state scrapers)

uv run main.py                       # run enabled scrapers
uv run main.py --scrapers AcreLegisScraper ConamaScraper  # run specific scrapers by class name
uv run main.py --list                # list all scrapers and their on/off status
uv run main.py --verbose             # show all info/debug logs (default shows only warnings + tqdm bars)
uv run main.py --overwrite           # ignore resume logic, re-scrape everything
```

## Lint & Format

```bash
uv run ruff check --fix src/ main.py
uv run ruff format src/ main.py
uv run pre-commit run --all-files     # trailing-whitespace, end-of-file-fixer, ruff
```

There are no tests in this project.

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

- **RequestService** (`src/services/request/service.py`) — `aiohttp` with per-scraper `RateLimiter`, retries via `tenacity`, optional proxy rotation.
- **LLMOCRService** (`src/services/ocr/llm.py`) — renders PDF pages to PNG via PyMuPDF, sends to LLM vision model. Supports OpenAI-compatible API and Bedrock Converse (`BedrockClient`).
- **BrowserService** (`src/services/browser/playwright.py`) — Playwright page pool for JS-rendered sites. Used by Maranhão, Paraná, and Pernambuco.
- **ProxyService** (`src/services/proxy/service.py`) — proxy rotation from file or HTTP endpoint.
- **FileSaver** (`src/database/saver.py`) — async JSON persistence via `aiofiles`. Saves documents grouped by year into `data.json` files with document-level resume support.

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

- Use `_build_doc_result()` to construct output dicts with a consistent schema (`year`, `type`, `situation`, `title`, `text_markdown`, `document_url`).
- Use `_save_doc_result()` to persist documents immediately (supports raw file saving + `data.json` append).
- Use `_save_doc_error()` to log document-level failures.
- Use `_is_already_scraped()` for resume support — checks `(document_url, title)` keys loaded by `_load_scraped_keys()`.
- Use `_get_markdown()` for flexible content→markdown conversion (accepts url, response, stream, or html_content).
- Use `_download_and_convert()` when you also need the raw bytes (e.g., for saving source PDFs).
- The LLM OCR prompt is in Portuguese and defined as a default parameter in `BaseScraper.__init__`. Override `llm_prompt` only if a scraper needs different extraction instructions.

## Environment

Python ≥ 3.12, managed with `uv`. Config via `.env` (copy `.env.example`). Key variables: `LLM_API_KEY`, `LLM_MODEL`, `PROVIDER_BASE_URL`, `SAVE_DIR`, `STATE_LEGISLATION_SAVE_DIR`.
