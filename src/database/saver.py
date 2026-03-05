from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Callable, Awaitable
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

MHTMLCaptureFn = Callable[[str], Awaitable[bytes]]


class FileSaver:
    """Async file saver with buffered JSON writes and per-year locking."""

    def __init__(
        self,
        save_dir: Path = SAVE_DIR,
        error_log_dir: str = ERROR_LOG_DIR,
        max_path_length: int = 225,
        verbose: bool = False,
        flush_interval: int = 50,
        max_workers: int = 16,
        mhtml_capture_fn: MHTMLCaptureFn | None = None,
    ):
        self.save_dir = save_dir
        self.error_log_dir = error_log_dir
        self.max_path_length = max_path_length
        self.verbose = verbose
        self.flush_interval = flush_interval
        self.max_workers = max_workers
        self._mhtml_capture_fn = mhtml_capture_fn

        self._format_regex_ws = re.compile(r"[\s]+")
        self._format_regex_special = re.compile(r"[^\w\s-]")

        self._year_locks: dict[int, asyncio.Lock] = {}
        self._pending_docs: dict[int, dict[tuple[str, str], dict]] = {}

        if self.verbose:
            logger.info(f"Saving to {save_dir}")
            logger.info(f"Saving errors to {error_log_dir}")

    _ERROR_PAGE_MARKERS = (
        b"Azion - Default error page",
        b"<title>403 Forbidden</title>",
        b"<title>Access Denied</title>",
        b"<title>Error</title>",
    )

    def _is_error_page(self, content: bytes) -> bool:
        """Return True if *content* looks like a CDN / WAF error page."""
        head = content[:2048]
        return any(marker in head for marker in self._ERROR_PAGE_MARKERS)

    def _validate_data(self, data: dict[str, Any]) -> bool:
        """Validate that data contains required fields."""
        required_fields = ["year", "document_url", "title"]
        return all(
            field in data and data[field] is not None for field in required_fields
        )

    def _truncate_path(self, path: str) -> str:
        """Truncate path if it exceeds max_path_length."""
        if len(path) <= self.max_path_length:
            return path

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
        name = self._format_regex_ws.sub("_", name)
        name = self._format_regex_special.sub("", name)
        return name.strip("_") or "document"

    async def cleanup(self) -> None:
        """Flush remaining documents."""
        await self.flush_all()

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
            await f.write(
                json.dumps(all_items, ensure_ascii=False, indent=2, sort_keys=True)
            )

    async def get_scraped_keys(self, year: int) -> set[tuple[str, str]]:
        """Return set of (document_url, title) pairs already scraped for a year."""
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
        """Save a single document: source file written immediately, JSON metadata buffered."""
        if not self._validate_data(doc_data):
            logger.warning(f"Invalid document data, skipping save: {doc_data}")
            return None

        # Capture MHTML before acquiring the year lock (slow I/O).
        # Falls back to the original HTML when capture fails or returns
        # an error page (e.g. Azion CDN blocking headless browsers).
        if (
            raw_content
            and content_extension
            and content_extension.lstrip(".") == "html"
            and self._mhtml_capture_fn
        ):
            doc_url = doc_data.get("document_url", "")
            if doc_url:
                try:
                    mhtml_bytes = await self._mhtml_capture_fn(doc_url)
                    if self._is_error_page(mhtml_bytes):
                        logger.debug(
                            f"MHTML captured an error page for {doc_url}, "
                            "falling back to raw HTML"
                        )
                    else:
                        raw_content = mhtml_bytes
                        content_extension = ".mhtml"
                except Exception as e:
                    logger.debug(
                        f"MHTML capture failed for {doc_url}, "
                        f"falling back to raw HTML: {e}"
                    )

        year = int(doc_data["year"])
        lock = self._get_year_lock(year)

        async with lock:
            try:
                save_dir = Path(self.save_dir)
                year_dir = save_dir / str(year)
                year_dir.mkdir(parents=True, exist_ok=True)

                clean_data = {
                    k: v
                    for k, v in doc_data.items()
                    if not k.startswith("_") and k != "html_string"
                }

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

                    clean_data["file_path"] = str(file_path.relative_to(save_dir))

                key = (clean_data.get("document_url", ""), clean_data.get("title", ""))
                if year not in self._pending_docs:
                    self._pending_docs[year] = {}
                self._pending_docs[year][key] = clean_data

                pending_count = len(self._pending_docs[year])

                if pending_count >= self.flush_interval:
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

    async def save_error(self, data: dict[str, Any], error_message: str = "") -> None:
        """Save error data to file (async)."""
        file_path = None
        try:
            required_error_fields = ["title", "year", "situation", "type", "html_link"]
            if not all(field in data for field in required_error_fields):
                logger.error(f"Missing required fields in error data: {data}")
                return

            if error_message:
                data = {**data, "error_message": error_message}

            save_dir = Path(self.error_log_dir)
            year_dir = save_dir / str(data["year"])
            type_dir = year_dir / data["type"]
            situation_dir = type_dir / data["situation"]

            situation_dir = Path(unquote(str(situation_dir)))
            situation_dir.mkdir(parents=True, exist_ok=True)

            title = self._sanitize_filename(data["title"])
            html_link = self._sanitize_filename(Path(data["html_link"]).stem)

            file_path = situation_dir / f"{title}_{html_link}.json"
            file_path = Path(self._truncate_path(str(file_path)))

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
