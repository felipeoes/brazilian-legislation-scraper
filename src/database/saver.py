import asyncio
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
    ):
        self.save_dir = save_dir
        self.error_log_dir = error_log_dir
        self.max_path_length = max_path_length
        self.verbose = verbose

        # Regex patterns compiled once
        self.format_regex_1 = re.compile(r"[\s]+")
        self.format_regex_2 = re.compile(r"[^\w\s-]")

        # Timing controls
        self.last_year = None

        self._set_last_year()
        # Lock to prevent concurrent writes to the same year file
        self._write_lock = asyncio.Lock()
        if self.verbose:
            logger.info(f"Saving to {save_dir}")
            logger.info(f"Saving errors to {error_log_dir}")

    def _set_last_year(self) -> None:
        """Set the last year that was saved (always the year before the current year in save_dir)"""
        save_dir = Path(self.save_dir)

        if not save_dir.exists():
            self.last_year = None
            return

        years = []
        for year_dir in save_dir.iterdir():
            if year_dir.is_dir() and year_dir.name.isdigit():
                years.append(int(year_dir.name))

        self.last_year = max(years) - 1 if years else None

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
        async with self._write_lock:
            try:
                save_dir = Path(self.save_dir)
                year_dir = save_dir / str(year)
                year_dir.mkdir(parents=True, exist_ok=True)
                main_file = year_dir / "data.json"

                # Load existing data if file exists
                existing_data = {}
                if main_file.exists():
                    try:
                        async with aiofiles.open(main_file, "r", encoding="utf-8") as f:
                            content = await f.read()
                            existing_list = json.loads(content)
                            # Convert to dict keyed by (document_url, title) for easy lookup
                            for item in existing_list:
                                document_url = item.get("document_url")
                                title = item.get("title", "")
                                if document_url:
                                    existing_data[(document_url, title)] = item
                    except (json.JSONDecodeError, Exception) as e:
                        logger.warning(
                            f"Could not load existing data for year {year}: {e}. Starting fresh."
                        )
                        existing_data = {}

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

                # Convert back to list and save
                all_items = list(existing_data.values())

                async with aiofiles.open(main_file, "w", encoding="utf-8") as f:
                    await f.write(json.dumps(all_items, ensure_ascii=False, indent=2))

                if self.verbose:
                    logger.info(
                        f"Saved data for year {year}: {len(all_items)} total items "
                        f"({new_items_count} new, {updated_items_count} updated, "
                        f"{len(all_items) - new_items_count - updated_items_count} unchanged)"
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
