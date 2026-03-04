# Brazilian Legislation Scraper

Web scraper for legal documents regarding Brazilian legislation — federal, state, and regulatory bodies (CONAMA, ICMBio).

> **Note:** This project does **not** scrape [leisestaduais.com.br](https://leisestaduais.com.br) as it is explicitly forbidden by their `robots.txt`.

## Features

- **27 state scrapers** — covering all Brazilian states with dedicated scrapers for each state legislature website
- **Federal legislation** — scrapes Câmara dos Deputados
- **Regulatory bodies** — CONAMA and ICMBio scrapers
- **Async concurrency** — built on `asyncio` + `aiohttp` for non-blocking I/O with sliding-window rate limiting (`rps` for HTTP, `llm_rps` for LLM API calls)
- **PDF & image extraction** — converts PDFs to Markdown, with optional LLM-powered OCR for image-based documents
- **Playwright support** — async Chromium automation for JavaScript-rendered pages, with optional VPN extension integration
- **Structured output** — saves scraped data as JSON files grouped by year via `FileSaver`
- **CLI interface** — select scrapers by name, list available scrapers

## Requirements

- Python >= 3.12
- [uv](https://docs.astral.sh/uv/) package manager
- Google Chrome (only for Paraná scraper's VPN extension — other browser scrapers use Playwright's bundled Chromium)

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
| `LLM_API_KEY` | API key for the LLM provider (used for OCR on image-based PDFs) | — |
| `LLM_MODEL` | Model name (e.g. `gpt-4o`) | — |
| `PROVIDER_BASE_URL` | LLM provider base URL | `https://api.openai.com/v1` |
| `SAVE_DIR` | Base directory for scraped JSON output | `outputs/legislation` |
| `STATE_LEGISLATION_SAVE_DIR` | Directory for state legislation documents | — |
| `SPECIFIC_LEGISLATION_SAVE_DIR` | Directory for CONAMA/ICMBio documents | — |
| `ERROR_LOG_DIR` | Directory for error logs | `logs/legislation` |

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

When `--verbose` is not specified, scrapers only log warnings, errors, and show tqdm progress bars. With `--verbose`, all debug and info messages are displayed.

### Run specific scrapers by name

```bash
uv run main.py --scrapers MTAlmt CONAMA SPAlesp
```

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
│   │   └── saver.py                 # FileSaver — async JSON persistence (aiofiles)
│   ├── scraper/
│   │   ├── base/
│   │   │   ├── scraper.py           # BaseScraper — async HTTP via aiohttp
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
│   └── utils/
│       └── openvpn.py               # OpenVPN manager (used by Paraná scraper)
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

### Technology Stack
- **HTTP I/O** — `aiohttp.ClientSession` for non-blocking requests with sliding-window rate limiting (`rps`)
- **File I/O** — `aiofiles` for non-blocking JSON writes
- **Browser automation** (4 scrapers) — Playwright async API (natively async, no thread wrappers)
- **CPU-bound work** (PDF/image conversion) — offloaded via `asyncio.to_thread()`
- **Retries** — `tenacity` for async retry logic with exponential backoff

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

Run pre-commit hooks on all files:

```bash
uv run pre-commit run --all-files
```

## License

See [LICENSE](LICENSE) for details.
