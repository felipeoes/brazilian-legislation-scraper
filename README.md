# Brazilian Legislation Scraper

Web scraper for legal documents regarding Brazilian legislation — federal, state, and regulatory bodies (CONAMA, ICMBio).

> **Note:** This project does **not** scrape [leisestaduais.com.br](https://leisestaduais.com.br) as it is explicitly forbidden by their `robots.txt`.

## Features

- **27 state scrapers** — covering all Brazilian states with dedicated scrapers for each state legislature website
- **Federal legislation** — scrapes Câmara dos Deputados
- **Regulatory bodies** — CONAMA and ICMBio scrapers
- **Async concurrency** — built on `asyncio` + `aiohttp` for non-blocking I/O with independent per-scraper rate limiting for HTTP and shared rate limiting for LLM API calls
- **LLM providers** — supports OpenAI-compatible APIs and AWS Bedrock Converse for OCR
- **PDF & image extraction** — converts PDFs to Markdown, with optional LLM-powered OCR for image-based documents
- **Playwright support** — async Chromium automation for JavaScript-rendered pages (3 state scrapers)
- **Proxy rotation** — optional proxy support from a file or HTTP endpoint
- **SAPL integration** — dedicated base class for state legislatures using the SAPL REST API
- **Structured output** — saves scraped data as JSON files grouped by year via `FileSaver`
- **CLI interface** — select scrapers by name, list available scrapers

## Requirements

- Python >= 3.12
- [uv](https://docs.astral.sh/uv/) package manager
- Playwright Chromium browser (`uv run playwright install chromium`)

## Installation

1. Install `uv` if you don't have it:
   ```bash
   pip install uv
   ```

2. Install project dependencies:
   ```bash
   uv sync
   ```

3. Install Playwright browsers:
   ```bash
   uv run playwright install chromium
   ```

4. Set up environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your values
   ```

## Configuration

Copy `.env.example` to `.env` and configure the following variables:

| Variable | Description | Default |
|---|---|---|
| `LLM_PROVIDER` | LLM provider to use (`openai` or `bedrock`) | `openai` |
| `LLM_API_KEY` | API key for the LLM provider (used for OCR on image-based PDFs) | — |
| `LLM_MODEL` | Model name (e.g. `gpt-4o`); comma-separated for multiple models | — |
| `PROVIDER_BASE_URL` | LLM provider base URL | `https://openrouter.ai/api/v1` |
| `SAVE_DIR` | Base directory for scraped JSON output | `outputs/legislation` |
| `STATE_LEGISLATION_SAVE_DIR` | Directory for state legislation documents | — |
| `SPECIFIC_LEGISLATION_SAVE_DIR` | Directory for CONAMA/ICMBio documents | — |
| `LOG_DIR` | Directory for per-scraper runtime logs and error logs | `logs/legislation` |
| `PROXY_FILE_PATH` | Path to a file containing proxy URLs (one per line) | — |
| `PROXY_ENDPOINT` | HTTP endpoint that returns proxy URLs | — |

## Usage

### Run all enabled scrapers

```bash
uv run main.py
```

By default, only scrapers with `run=True` in their config will execute. Edit `main.py` → `build_scraper_configs()` to toggle which scrapers are enabled.

### Verbose logging

```bash
# Default: Shows only warnings, errors, and progress bars
uv run main.py

# Verbose mode: Shows all info logs including debug messages
uv run main.py --verbose
```

When `--verbose` is not specified, scrapers only log warnings, errors, and show tqdm progress bars in the terminal. With `--verbose`, all debug and info messages are displayed in the terminal.

Each scraper also writes a debug-level runtime log to `LOG_DIR/<SCRAPER>/runtime.log` on every run. Structured document error JSON files are saved in the same scraper log directory.

### Run specific scrapers by name

```bash
uv run main.py --scrapers MTAlmt Conama SaoPauloAlesp
```

### Overwrite previously scraped data

```bash
uv run main.py --overwrite
```

By default, scrapers resume from where they left off using document-level `(document_url, title)` keys. The `--overwrite` flag disables this resume logic and re-scrapes everything.

### List all available scrapers

```bash
uv run main.py --list
```

## Project Structure

```
├── main.py                          # Entry point & CLI (asyncio.run)
├── pyproject.toml                   # Project config & dependencies
├── .env.example                     # Environment variable template
├── src/
│   ├── database/
│   │   └── saver.py                 # FileSaver — async JSON persistence (aiofiles) with document-level resume
│   ├── scraper/
│   │   ├── base/
│   │   │   ├── scraper.py           # BaseScraper & StateScraper — async HTTP, PDF/OCR, markdown, save/resume
│   │   │   ├── sapl_scraper.py      # SAPLBaseScraper — base for SAPL REST API sites (Paraíba, Piauí, Roraima)
│   │   │   └── concurrency.py       # RateLimiter, bounded_gather(), run_in_thread()
│   │   ├── federal_legislation/
│   │   │   └── scrape.py            # CamaraDepScraper
│   │   ├── conama/
│   │   │   └── scrape.py            # ConamaScraper
│   │   ├── icmbio/
│   │   │   └── scrape.py            # ICMBioScraper
│   │   └── state_legislation/       # 27 state-specific scrapers
│   │       ├── acre.py
│   │       ├── alagoas.py
│   │       ├── ...
│   │       └── tocantins.py
│   ├── services/
│   │   ├── browser/
│   │   │   └── playwright.py        # BrowserService — Playwright page pool
│   │   ├── ocr/
│   │   │   ├── llm.py               # LLMOCRService — PDF/image-to-Markdown via LLM vision
│   │   │   └── bedrock.py           # BedrockClient — AWS Bedrock Converse API adapter
│   │   ├── proxy/
│   │   │   └── service.py           # ProxyService — proxy rotation from file or endpoint
│   │   └── request/
│   │       └── service.py           # RequestService — async HTTP with rate limiting & retries
│   └── utils/
│       └── __init__.py              # clean_md_tag() — strips markdown code block wrappers
```

## Architecture

The project uses an **async-first** concurrency model with optimized parallelism:

### Concurrency Levels
- **Years** — scraped **sequentially** (one year completes before next starts)
  - Prevents resource exhaustion with many years
  - Better progress visibility and logging
  - Each year's data completes fully before moving to next
- **Types/Situations** — scraped **concurrently** within each year via `asyncio.gather()`
  - All document types for a year scrape in parallel
  - Nested situation+type combinations run concurrently
  - Significantly faster when a year has many types
- **Pages** — scraped **concurrently** via `asyncio.gather()`
- **Documents** — scraped **concurrently** via `asyncio.gather()`

### Rate Limiting
- **HTTP requests** — each scraper has its own `RateLimiter` (via `RequestService`), configured independently with its `rps` parameter. Scrapers targeting different websites do not interfere with each other.
- **LLM requests** — all scrapers share a single `RateLimiter` instance for LLM API calls, since they all query the same LLM endpoint. This prevents exceeding the provider's rate limit when multiple scrapers run in parallel.

### Technology Stack
- **HTTP I/O** — `aiohttp.ClientSession` for non-blocking requests with per-scraper sliding-window rate limiting
- **LLM OCR** — vision model-based PDF/image extraction via OpenAI-compatible API or AWS Bedrock Converse API
- **File I/O** — `aiofiles` for non-blocking JSON writes
- **Browser automation** (3 scrapers) — Playwright async API (natively async, no thread wrappers)
- **CPU-bound work** (PDF/image conversion) — offloaded via `asyncio.to_thread()`
- **Retries** — `tenacity` for async retry logic with exponential backoff
- **Proxy support** — optional proxy rotation from a file or HTTP endpoint

## Development

Install dev dependencies:

```bash
uv sync --group dev
```

Lint and format:

```bash
uv run ruff check --fix src/ main.py
uv run ruff format src/ main.py
```

Run tests:

```bash
# Fast tests (recommended for development) - skips slow integration tests
uv run pytest -m "not integration"

# Parallel execution (faster on multi-core systems)
uv run pytest -n auto -m "not integration"

# Run all tests (including slow integration tests)
uv run pytest tests/

# Run a single test file
uv run pytest tests/test_core.py

# Run only integration tests (requires network and credentials)
uv run pytest tests/ -m integration

# Run tests with timing information
uv run pytest --durations=10
```

**Test Performance**: The test suite has been optimized for speed:
- **Fast tests**: ~12 seconds (98.3% faster than before)
- **Integration tests**: Include live network calls and may take several minutes
- **Parallel execution**: Use `-n auto` for multi-core speedup

Integration tests make real network requests to live legislative websites. They require internet access and (for Snowflake tests) valid credentials in `.env`. Use `-m "not integration"` for fast offline or CI runs.

Run pre-commit hooks on all files:

```bash
uv run pre-commit run --all-files
```

Pre-commit runs trailing-whitespace, end-of-file, large-file checks, ruff linting/formatting, and the full pytest suite (including integration tests) on every commit.

## License

See [LICENSE](LICENSE) for details.
