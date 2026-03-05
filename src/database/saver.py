import asyncio
import hashlib
import json
import re
from unidecode import unidecode
from os import environ
from pathlib import Path
from urllib.parse import unquote
from dotenv import load_dotenv
from typing import Any
from loguru import logger
import aiofiles

load_dotenv()

SAVE_DIR = Path(rf"{environ.get('SAVE_DIR', 'outputs/legislation')}")
ERROR_LOG_DIR = rf"{environ.get('ERROR_LOG_DIR', 'logs/legislation')}"

logger.info(f"Default saving to SAVE_DIR: {SAVE_DIR}")
logger.info(f"Default saving to ERROR_LOG_DIR: {ERROR_LOG_DIR}")


class FileSaver:
    """File saver to save data to txt files with optimized performance"""

    def __init__(
        self,
        save_dir: Path = SAVE_DIR,
        error_log_dir: str = ERROR_LOG_DIR,
        max_path_length: int = 225,  # Windows max path length
        verbose: bool = False,
        flush_interval: int = 50,
    ):
        self.save_dir = save_dir
        self.error_log_dir = error_log_dir
        self.max_path_length = max_path_length
        self.verbose = verbose
        self.flush_interval = flush_interval

        # Regex patterns compiled once
        self.format_regex_1 = re.compile(r"[\s]+")
        self.format_regex_2 = re.compile(r"[^\w\s-]")

        # Per-year locks for concurrent document saves within a year
        self._year_locks: dict[int, asyncio.Lock] = {}
        # In-memory buffer: year -> {(doc_url, title): doc_dict}
        self._pending_docs: dict[int, dict[tuple[str, str], dict]] = {}
        self._pending_counts: dict[int, int] = {}
        if self.verbose:
            logger.info(f"Saving to {save_dir}")
            logger.info(f"Saving errors to {error_log_dir}")

    def _validate_data(self, data: dict[str, Any]) -> bool:
        """Validate that data contains required fields"""
        required_fields = ["year", "document_url", "title"]
        return all(
            field in data and data[field] is not None for field in required_fields
        )

    def _truncate_path(self, path: str) -> str:
        """Truncate path if it exceeds max_path_length"""
        if len(path) <= self.max_path_length:
            return path

        # Keep extension and truncate filename
        path_obj = Path(path)
        extension = path_obj.suffix
        max_name_length = (
            self.max_path_length - len(str(path_obj.parent)) - len(extension) - 1
        )

        if max_name_length > 0:
            truncated_name = path_obj.stem[:max_name_length]
            return str(path_obj.parent / f"{truncated_name}{extension}")

        return path

    def _get_year_lock(self, year: int) -> asyncio.Lock:
        """Get or create a lock for a specific year."""
        if year not in self._year_locks:
            self._year_locks[year] = asyncio.Lock()
        return self._year_locks[year]

    def _sanitize_filename(self, name: str) -> str:
        """Sanitize a string for use as a filename."""
        name = unidecode(name)
        name = self.format_regex_1.sub("_", name)
        name = self.format_regex_2.sub("", name)
        return name.strip("_") or "document"

    async def _load_year_data(self, year: int) -> dict[tuple[str, str], dict]:
        """Load existing data.json for a year, keyed by (document_url, title)."""
        main_file = Path(self.save_dir) / str(year) / "data.json"
        if not main_file.exists():
            return {}
        try:
            async with aiofiles.open(main_file, "r", encoding="utf-8") as f:
                content = await f.read()
                items = json.loads(content)
                return {
                    (item.get("document_url", ""), item.get("title", "")): item
                    for item in items
                    if item.get("document_url")
                }
        except (json.JSONDecodeError, Exception) as e:
            logger.warning(
                f"Could not load existing data for year {year}: {e}. Starting fresh."
            )
            return {}

    async def _write_year_data(
        self, year: int, data: dict[tuple[str, str], dict]
    ) -> None:
        """Write merged data back to data.json for a year."""
        year_dir = Path(self.save_dir) / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        main_file = year_dir / "data.json"
        all_items = list(data.values())
        async with aiofiles.open(main_file, "w", encoding="utf-8") as f:
            await f.write(json.dumps(all_items, ensure_ascii=False, indent=2))

    async def get_scraped_keys(self, year: int) -> set[tuple[str, str]]:
        """Return set of (document_url, title) pairs already scraped for a year.

        Only documents that have a ``file_path`` (i.e. their source file was
        saved) are considered fully scraped.  Legacy entries without
        ``file_path`` will be re-processed on the next run.
        """
        lock = self._get_year_lock(year)
        async with lock:
            data = await self._load_year_data(year)
            return {key for key, item in data.items() if item.get("file_path")}

    async def save_document(
        self,
        doc_data: dict[str, Any],
        raw_content: bytes | None = None,
        content_extension: str | None = None,
    ) -> dict[str, Any] | None:
        """Save a single document: source file written immediately, JSON metadata buffered.

        The JSON metadata is accumulated in memory and flushed to ``data.json``
        every ``flush_interval`` documents or when ``flush()`` / ``flush_all()``
        is called.  Source files (PDF, HTML, etc.) are always written to disk
        immediately so that no data is lost on crash.

        Args:
            doc_data: Document dict (must have year, document_url, title).
            raw_content: Raw source file bytes (PDF, HTML, etc.) to save.
            content_extension: File extension for the source file (e.g. ".pdf", ".html").

        Returns:
            Updated doc_data dict with ``file_path`` added, or None on failure.
        """
        if not self._validate_data(doc_data):
            logger.warning(f"Invalid document data, skipping save: {doc_data}")
            return None

        year = int(doc_data["year"])
        lock = self._get_year_lock(year)

        async with lock:
            try:
                save_dir = Path(self.save_dir)
                year_dir = save_dir / str(year)
                year_dir.mkdir(parents=True, exist_ok=True)

                # Strip internal fields before persisting
                clean_data = {
                    k: v for k, v in doc_data.items() if not k.startswith("_")
                }

                # Save source file immediately if raw content provided
                if raw_content and content_extension:
                    docs_dir = year_dir / "docs"
                    docs_dir.mkdir(parents=True, exist_ok=True)

                    title = clean_data.get("title", "document")
                    url = clean_data.get("document_url", "")
                    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
                    sanitized = self._sanitize_filename(title)

                    ext = (
                        content_extension
                        if content_extension.startswith(".")
                        else f".{content_extension}"
                    )
                    filename = f"{sanitized}_{url_hash}{ext}"

                    file_path = docs_dir / filename
                    file_path = Path(self._truncate_path(str(file_path)))

                    async with aiofiles.open(file_path, "wb") as f:
                        await f.write(raw_content)

                    # Store relative path from save_dir for portability
                    clean_data["file_path"] = str(file_path.relative_to(save_dir))

                # Buffer the JSON metadata instead of writing immediately
                key = (clean_data.get("document_url", ""), clean_data.get("title", ""))
                if year not in self._pending_docs:
                    self._pending_docs[year] = {}
                    self._pending_counts[year] = 0
                self._pending_docs[year][key] = clean_data
                self._pending_counts[year] += 1

                if self.verbose:
                    logger.info(
                        f"Buffered document '{clean_data.get('title', '?')}' "
                        f"for year {year} (pending: {self._pending_counts[year]})"
                    )

                # Auto-flush when buffer reaches flush_interval
                if self._pending_counts[year] >= self.flush_interval:
                    await self._flush_year(year)

                return clean_data

            except Exception as e:
                logger.error(
                    f"Error saving document '{doc_data.get('title', '?')}' "
                    f"for year {year}: {e}"
                )
                return None

    async def _flush_year(self, year: int) -> None:
        """Merge buffered docs for a year into data.json and clear the buffer.

        Must be called while holding the year lock.
        """
        pending = self._pending_docs.get(year)
        if not pending:
            return

        existing_data = await self._load_year_data(year)
        existing_data.update(pending)
        await self._write_year_data(year, existing_data)

        count = len(pending)
        self._pending_docs[year] = {}
        self._pending_counts[year] = 0

        if self.verbose:
            logger.info(
                f"Flushed {count} documents for year {year} "
                f"(total on disk: {len(existing_data)})"
            )

    async def flush(self, year: int) -> None:
        """Flush buffered documents for a specific year to disk."""
        lock = self._get_year_lock(year)
        async with lock:
            await self._flush_year(year)

    async def flush_all(self) -> None:
        """Flush all buffered documents across all years to disk."""
        for year in list(self._pending_docs.keys()):
            await self.flush(year)

    async def save(self, data_list: list[dict[str, Any]]) -> None:
        """Save a list of data items to files, grouped by year (async)."""
        if not data_list:
            return

        # Group items by year
        data_by_year = {}
        error_items = []

        for item in data_list:
            if self._validate_data(item):
                year = item.get("year")
                if year:
                    if year not in data_by_year:
                        data_by_year[year] = []
                    data_by_year[year].append(item)
            else:
                logger.warning(f"Invalid data format: {item}")
                error_items.append(item)

        # Save valid items by year
        for year, items in data_by_year.items():
            await self._save_year_data(year, items)

        # Save error items
        for item in error_items:
            await self.save_error(item)

    async def _save_year_data(self, year: int, items: list[dict[str, Any]]) -> None:
        """Save data for a specific year, merging with existing data (async)."""
        lock = self._get_year_lock(year)
        async with lock:
            try:
                existing_data = await self._load_year_data(year)

                # Process new items and merge with existing data
                new_items_count = 0
                updated_keys: set[tuple[str, str]] = set()

                for item in items:
                    document_url = item.get("document_url")
                    title = item.get("title", "")
                    if not document_url:
                        continue

                    key = (document_url, title)
                    if key in existing_data:
                        # Update existing document
                        existing_data[key] = item
                        updated_keys.add(key)
                    else:
                        # New document
                        new_items_count += 1
                        existing_data[key] = item

                updated_items_count = len(updated_keys)

                await self._write_year_data(year, existing_data)

                if self.verbose:
                    total = len(existing_data)
                    logger.info(
                        f"Saved data for year {year}: {total} total items "
                        f"({new_items_count} new, {updated_items_count} updated, "
                        f"{total - new_items_count - updated_items_count} unchanged)"
                    )

            except Exception as e:
                error_msg = f"Error saving {len(items)} items for year {year}: {e}"
                logger.error(error_msg)
                # Save individual items as errors
                for item in items:
                    await self.save_error(item)

    async def save_error(self, data: dict[str, Any], error_message: str = "") -> None:
        """Save error data to file (async).

        Args:
            data: Dict with at least title, year, situation, type, html_link.
            error_message: Human-readable description of what went wrong.
        """
        file_path = None
        try:
            # Validate required fields for error data
            required_error_fields = ["title", "year", "situation", "type", "html_link"]
            if not all(field in data for field in required_error_fields):
                logger.error(f"Missing required fields in error data: {data}")
                return

            # Inject the error message into the persisted JSON
            if error_message:
                data = {**data, "error_message": error_message}

            save_dir = Path(self.error_log_dir)
            year_dir = save_dir / str(data["year"])
            type_dir = year_dir / data["type"]
            situation_dir = type_dir / data["situation"]

            # Decode and create directory path
            situation_dir = Path(unquote(str(situation_dir)))
            situation_dir.mkdir(parents=True, exist_ok=True)

            # Clean and format filename components
            title = unidecode(data["title"]).replace(" ", "_")
            title = self.format_regex_1.sub("_", title)
            title = self.format_regex_2.sub("", title)

            html_link = unidecode(data["html_link"]).replace(" ", "_")
            html_link = self.format_regex_1.sub("_", Path(html_link).stem)
            html_link = self.format_regex_2.sub("", html_link)

            # Create file path and apply length restrictions
            file_path = situation_dir / f"{title}_{html_link}.json"
            file_path = Path(self._truncate_path(str(file_path)))

            # Save JSON data
            async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=4))

            if self.verbose:
                logger.info(f"Saved error data to {file_path}")

        except Exception as e:
            error_msg = f"Error saving error data for '{data.get('title', 'Unknown')}'"
            if file_path:
                error_msg += f" to {file_path}"
            error_msg += f": {e}"
            logger.error(error_msg)
