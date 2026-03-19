# Brazilian Legislation Scraper

## Project Overview
This is a Python-based asynchronous web scraper designed to collect legal documents regarding Brazilian legislation at the federal and state levels, as well as from environment regulatory bodies like CONAMA and ICMBio. It extracts structured data and saves it as JSON files grouped by year.

**Key Features & Architecture:**
- **Concurrency:** Built heavily on `asyncio` and `aiohttp` for non-blocking I/O.
- **Data Extraction:** Supports PDF to Markdown conversion and includes optional LLM-powered OCR (using OpenAI-compatible APIs or AWS Bedrock Converse) for image-based documents.
- **Browser Automation:** Uses Playwright for JavaScript-rendered pages.
- **Rate Limiting:** Independent per-scraper rate limiting for HTTP requests and shared rate limiting for LLM API calls.
- **Proxy Support:** Includes optional proxy rotation from a file or HTTP endpoint.

**Tech Stack:** Python (>=3.12), `uv` (package manager), `aiohttp`, `Playwright`, `openai`, `aiofiles`, `loguru`, `tenacity`.

## Building and Running

The project relies on `uv` for dependency management and execution.

### Environment Setup
1. Install dependencies: `uv sync`
2. Install Playwright browsers: `uv run playwright install chromium`
3. Configure environment variables by copying `.env.example` to `.env` and filling in the necessary API keys and paths.

### Execution Commands
*   **Run enabled scrapers:**
    ```bash
    uv run main.py
    ```
*   **Run specific scrapers by name:**
    ```bash
    uv run main.py --scrapers Conama SaoPauloAlesp
    ```
*   **List all available scrapers:**
    ```bash
    uv run main.py --list
    ```
*   **Run with verbose logging:**
    ```bash
    uv run main.py --verbose
    ```
*   **Force re-scrape (ignore resume logic):**
    ```bash
    uv run main.py --overwrite
    ```

## Development Conventions

*   **Dependency Management:** Always use `uv` for managing dependencies and running scripts.
*   **Asynchronous Code:** The codebase is heavily asynchronous (`async`/`await`). Maintain this pattern for network and file I/O operations.
*   **Linting and Formatting:** The project uses `ruff` for both linting and formatting.
    *   Check and fix: `uv run ruff check --fix src/ main.py`
    *   Format code: `uv run ruff format src/ main.py`
*   **Pre-commit Hooks:** Use pre-commit to ensure code quality before pushing.
    *   Run hooks on all files: `uv run pre-commit run --all-files`
*   **Testing:** Not explicitly found in the root structure, but any new additions should ideally conform to the existing `async` patterns and error-handling strategies using `tenacity` for retries.
