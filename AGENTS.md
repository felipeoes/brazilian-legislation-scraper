# AGENTS.md

Agent instructions for the Brazilian Legislation Scraper repository.

## Build & Run

```bash
uv sync                              # install deps
uv sync --group dev                  # install dev deps
uv run playwright install chromium   # install browser (needed by 3 state scrapers)

uv run main.py                       # run all enabled scrapers
uv run main.py --scrapers AcreLegis Conama  # run specific scrapers by short name or class name
uv run main.py --list                # list all scrapers and their on/off status
uv run main.py --verbose             # show all info/debug logs (default: warnings + tqdm bars)
uv run main.py --overwrite           # ignore resume logic, re-scrape everything
```

## Lint & Format

```bash
uv run ruff check --fix src/ main.py
uv run ruff format src/ main.py
uv run pre-commit run --all-files    # trailing-whitespace, end-of-file-fixer, ruff, pytest
```

Ruff runs with **default settings** — there is no `ruff.toml` or `[tool.ruff]` section in
`pyproject.toml`. Do not add inline `# noqa` suppressions without a concrete reason.

## Testing

```bash
uv run pytest tests/                              # run all tests (includes integration)
uv run pytest tests/test_core.py                 # run a single test file
uv run pytest tests/test_core.py::test_foo       # run a single test by name
uv run pytest tests/ -m "not integration"        # skip live-network tests (used by pre-commit)
uv run pytest tests/ -m integration              # run only integration tests (requires network)
uv run pytest tests/ -k "acre" -m "not integration"  # filter by keyword, skip integration
```

- `asyncio_mode = auto` — all async tests work without any decorator.
- Integration tests are marked `@pytest.mark.integration` and make real network requests.
- Per-scraper tests instantiate scrapers via `object.__new__` to avoid triggering I/O in
  `__init__`. Follow this pattern when adding new scraper tests.
- Default timeout per test: 300 s (`pytest.ini`).

## Architecture

### Class Hierarchy

```
BaseScraper                          # src/scraper/base/scraper.py
  └─ StateScraper                    # same file — sets STATE_LEGISLATION_SAVE_DIR default
       ├─ SAPLBaseScraper            # src/scraper/base/sapl_scraper.py (Paraíba, Piauí, Roraima)
       └─ <27 state scrapers>        # src/scraper/state_legislation/<state>.py
  └─ CamaraDepScraper               # src/scraper/federal_legislation/
  └─ ConamaScraper, ICMBioScraper   # src/scraper/conama/, src/scraper/icmbio/
```

### Services (composed into BaseScraper)

- **RequestService** (`src/services/request/service.py`) — `aiohttp`, per-scraper
  `RateLimiter`, retries via `tenacity`, optional proxy rotation.
- **LLMOCRService** (`src/services/ocr/llm.py`) — renders PDF pages to PNG via PyMuPDF,
  sends to LLM vision model. Clients in `src/services/ocr/clients/` implement `LLMClient`
  protocol: `OpenAIClient`, `BedrockClient`, `SnowflakeClient`.
- **BrowserService** (`src/services/browser/playwright.py`) — Playwright page pool for
  JS-rendered sites (Maranhão, Paraná, Pernambuco).
- **ProxyService** (`src/services/proxy/service.py`) — proxy rotation from file or endpoint.
- **FileSaver** (`src/database/saver.py`) — async JSON persistence via `aiofiles`, grouped
  by year into `data.json` files with document-level resume support.

### Concurrency Model

- **Years** — scraped **sequentially** (one year completes before the next begins).
- **Types/Situations within a year** — scraped **concurrently** via `asyncio.gather()`.
- **Pages and documents** — scraped **concurrently** via `_gather_results()`.
- **Rate limiting** — each scraper has its own `RateLimiter` for HTTP (`rps` param); all
  scrapers share a single `RateLimiter` for LLM API calls.
- **CPU-bound work** (PDF rendering) — offloaded via `asyncio.to_thread()` / `run_in_thread()`.

## Code Style

### Language & Formatting

- Python **≥ 3.12**. Use `from __future__ import annotations` at the top of every module.
- Formatting is enforced by `ruff format` (Black-compatible). Never manually reformat; just
  run the formatter.
- Line length: ruff default (88 characters).

### Type Annotations

- Annotate **all** function signatures (parameters and return types).
- Use built-in generics (`list[str]`, `dict[str, int]`, `tuple[int, ...]`) — no `List`/`Dict`
  from `typing` unless targeting < 3.9 (not applicable here).
- Use `X | Y` union syntax, not `Union[X, Y]`.
- Use `TYPE_CHECKING` guards for imports needed only for annotations:
  ```python
  from typing import TYPE_CHECKING
  if TYPE_CHECKING:
      from src.services.ocr.config import LLMConfig
  ```

### Import Order

Follow ruff/isort conventions (enforced automatically):

1. `from __future__ import annotations`
2. Standard library
3. Third-party packages (`aiohttp`, `bs4`, `loguru`, `tqdm`, …)
4. Internal packages (`src.*`)

Group with a blank line between each section. No wildcard imports.

### Naming Conventions

| Construct | Convention | Example |
|---|---|---|
| Module-level constant | `UPPER_CASE` | `YEAR_START`, `DEFAULT_LLM_PROMPT` |
| Class | `PascalCase` | `AcreLegisScraper`, `RequestService` |
| Public method/function | `snake_case` | `scrape()`, `make_request()` |
| Internal method | `_snake_case` | `_get_docs_links()`, `_scrape_type()` |
| Variable | `snake_case` | `doc_info`, `norm_type_id` |
| Type alias | `PascalCase` | `DocResult` |

Scraper classes must be named `<StateName>Scraper` (e.g., `AcreLegisScraper`).

### Error Handling

- `RequestService.make_request()` / `get_soup()` return a **falsy** `FailedRequest` sentinel
  on failure — **always** check `if not resp:`, never `if resp is None:`.
- Log document-level failures with `await self._save_doc_error(...)`, not bare `logger.error`.
- Use `tenacity` retry decorators (already wired into `RequestService`) — do not implement
  manual retry loops.
- Never swallow exceptions silently; at minimum log with `logger.exception(...)`.

### Async Patterns

- All I/O methods must be `async def`. Synchronous blocking calls inside async code must be
  offloaded: `await asyncio.to_thread(fn, *args)`.
- Use `asyncio.gather(*coros)` for fan-out concurrency; wrap with `_gather_results()` inside
  scrapers to get error filtering and progress bars automatically.

### Configuration & Environment

- All environment variables are centralized in `src/config.py`. Import constants from there
  (`SAVE_DIR`, `STATE_LEGISLATION_SAVE_DIR`, `LOG_DIR`, etc.) — never call
  `os.environ` directly outside `src/config.py`.
- Config via `.env` (copy `.env.example`). Key variables: `LLM_PROVIDER`, `LLM_API_KEY`,
  `LLM_MODEL`, `PROVIDER_BASE_URL`, `SAVE_DIR`, `STATE_LEGISLATION_SAVE_DIR`,
  `LOG_DIR`.
- LLM providers: `openai` (default, OpenAI-compatible), `bedrock` (AWS Bedrock), `snowflake`.

## Key Helper Methods (BaseScraper)

| Method | Purpose |
|---|---|
| `_process_documents(docs, ...)` | Runs `_get_doc_data` → `_with_save` → `_gather_results` |
| `_save_doc_result(result, ...)` | Persists a document (raw file + `data.json` append) |
| `_save_doc_error(title, ...)` | Logs a document-level failure to the error log |
| `_is_already_scraped(url, title)` | Resume support — returns `True` if already saved |
| `_get_markdown(...)` | Converts url/response/stream/html to Markdown |
| `_download_and_convert(url)` | Fetches raw bytes and converts to Markdown |
| `_gather_results(coros, desc)` | `asyncio.gather` with error filtering + tqdm bar |

The LLM OCR prompt (`DEFAULT_LLM_PROMPT`) is defined in `src/scraper/base/scraper.py`.
Override the `llm_prompt` attribute only when a scraper genuinely needs different instructions.

## Adding a New State Scraper

1. Create `src/scraper/state_legislation/<state>.py`.
2. Subclass `StateScraper` (or `SAPLBaseScraper` for SAPL REST API sites).
3. Define module-level `TYPES` dict and `SITUATIONS` dict/list.
4. Implement `_format_search_url()`, `_get_docs_links()`, `_get_doc_data()`, `_scrape_type()`.
5. Export the class from `src/scraper/state_legislation/__init__.py`.
6. Add a `ScraperConfig` entry in `main.py` → `build_scraper_configs()`.
