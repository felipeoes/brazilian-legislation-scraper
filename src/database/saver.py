from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from unidecode import unidecode
from pathlib import Path
from urllib.parse import unquote
from typing import Any
from loguru import logger
import aiofiles

from src.config import LOG_DIR, SAVE_DIR


@dataclass
class _YearState:
    """Per-year mutable state for FileSaver (lock + pending buffer + shard sequence)."""

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    pending_docs: dict[tuple[str, str], dict] = field(default_factory=dict)
    shard_seq: int | None = None  # None = not yet scanned from disk


def aggregate_types_summary(
    items: list[dict[str, Any]],
    summary: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Aggregate documents into a ``{type: {total, situations}}`` summary dict.

    If *summary* is provided, counts are added to it in-place (and returned).
    Otherwise a new dict is created.
    """
    result = summary if summary is not None else {}
    for doc in items:
        doc_type = _normalize_summary_value(doc.get("type"))
        doc_situation = _normalize_summary_value(doc.get("situation"))
        if doc_type not in result:
            result[doc_type] = {"total": 0, "situations": {}}
        result[doc_type]["total"] += 1
        result[doc_type]["situations"][doc_situation] = (
            result[doc_type]["situations"].get(doc_situation, 0) + 1
        )
    return result


def _normalize_year(value: Any) -> int | None:
    """Normalize a year-like value to ``int`` when possible."""
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _normalize_summary_value(value: Any, *, fallback: str = "Unknown") -> str:
    """Normalize summary bucket labels and collapse placeholder values."""
    normalized = str(value or "").strip()
    if not normalized:
        return fallback
    if normalized.casefold() in {"na", "n/a", "all"}:
        return fallback
    return normalized


class FileSaver:
    """Async file saver with buffered JSON writes and per-year locking."""

    def __init__(
        self,
        save_dir: Path = SAVE_DIR,
        log_dir: Path | str = LOG_DIR,
        max_path_length: int = 225,
        verbose: bool = False,
        flush_interval: int = 100,
        max_workers: int = 50,
    ):
        self.save_dir = save_dir
        self.log_dir = Path(log_dir)
        self.max_path_length = max_path_length
        self.verbose = verbose
        self.flush_interval = flush_interval
        self.max_workers = max_workers

        self._format_regex_ws = re.compile(r"[\s]+")
        self._format_regex_special = re.compile(r"[^\w\s-]")

        self._shard_name_regex = re.compile(r"^chunk_(\d{6})\.json$")
        self._year_states: dict[int, _YearState] = {}

        if self.verbose:
            logger.info(f"Saving to {save_dir}")
            logger.info(f"Saving logs to {self.log_dir}")

    def _validate_data(self, data: dict[str, Any]) -> bool:
        """Quickly validate required fields are present and non-empty before locking.

        Checks all fields that ``SavedDocument`` requires as non-empty so that
        bad data is caught cheaply (before file I/O and the year lock) with a
        clear log message pinpointing the missing/empty field.
        """
        # Fields that must be non-None and (for strings) non-blank.
        required_nonempty = [
            "year",
            "document_url",
            "title",
            "type",
            "situation",
            "text_markdown",
        ]
        for field_name in required_nonempty:
            value = data.get(field_name)
            if value is None:
                logger.warning(
                    f"Document validation failed: field '{field_name}' is missing "
                    f"(title={data.get('title', '?')!r})"
                )
                return False
            if isinstance(value, str) and not value.strip():
                logger.warning(
                    f"Document validation failed: field '{field_name}' is empty "
                    f"(title={data.get('title', '?')!r})"
                )
                return False
        return True

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

    def _get_year_state(self, year: int) -> _YearState:
        """Get or create the per-year state object."""
        if year not in self._year_states:
            self._year_states[year] = _YearState()
        return self._year_states[year]

    def _get_year_lock(self, year: int) -> asyncio.Lock:
        """Get the asyncio lock for a specific year."""
        return self._get_year_state(year).lock

    def _year_dir(self, year: int) -> Path:
        """Return the directory used for a given year."""
        return Path(self.save_dir) / str(year)

    def _main_data_file(self, year: int) -> Path:
        """Return the canonical ``data.json`` path for a given year."""
        return self._year_dir(year) / "data.json"

    def _shards_dir(self, year: int) -> Path:
        """Return the shard directory for a given year."""
        return self._year_dir(year) / "shards"

    @staticmethod
    def _doc_key(item: dict[str, Any]) -> tuple[str, str] | None:
        """Build ``(document_url, title)`` key for a saved document row."""
        document_url = str(item.get("document_url", ""))
        if not document_url:
            return None
        return document_url, str(item.get("title", ""))

    async def _read_items_file(self, file_path: Path) -> list[dict[str, Any]]:
        """Load a JSON list of documents from disk.

        Supports both the legacy plain-list format and the new
        ``{"summary": ..., "documents": [...]}`` envelope.
        """
        if not file_path.exists():
            return []
        try:
            async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
                content = await f.read()
            data = json.loads(content)
            if isinstance(data, dict) and "documents" in data:
                data = data["documents"]
            if not isinstance(data, list):
                logger.warning(
                    f"Expected a list in {file_path}, got {type(data).__name__}"
                )
                return []
            return [item for item in data if isinstance(item, dict)]
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Could not read {file_path}: {e}")
            return []

    def _list_shard_files(self, year: int) -> list[Path]:
        """List shard files for a year, sorted by sequence."""
        shards_dir = self._shards_dir(year)
        if not shards_dir.exists():
            return []

        shard_files = [
            p
            for p in shards_dir.glob("chunk_*.json")
            if p.is_file() and self._shard_name_regex.match(p.name)
        ]
        return sorted(shard_files)

    def _next_shard_path(self, year: int) -> Path:
        """Return the next shard file path for a given year."""
        state = self._get_year_state(year)
        if state.shard_seq is None:
            max_seq = 0
            for shard_path in self._list_shard_files(year):
                match = self._shard_name_regex.match(shard_path.name)
                if match:
                    max_seq = max(max_seq, int(match.group(1)))
            state.shard_seq = max_seq

        state.shard_seq += 1
        shards_dir = self._shards_dir(year)
        shards_dir.mkdir(parents=True, exist_ok=True)
        return shards_dir / f"chunk_{state.shard_seq:06d}.json"

    def _years_with_shards(self) -> set[int]:
        """Return years that currently have shard files on disk."""
        years: set[int] = set()
        root = Path(self.save_dir)
        if not root.exists():
            return years

        for entry in root.iterdir():
            if not entry.is_dir():
                continue
            try:
                year = int(entry.name)
            except ValueError:
                continue
            shard_files = list((entry / "shards").glob("chunk_*.json"))
            if shard_files:
                years.add(year)

        return years

    def _saved_years(self) -> list[int]:
        """Return all numeric year directories tracked by this saver."""
        years = set(self._year_states)
        root = Path(self.save_dir)
        if root.exists():
            for entry in root.iterdir():
                if not entry.is_dir():
                    continue
                try:
                    years.add(int(entry.name))
                except ValueError:
                    continue
        return sorted(years)

    async def _load_main_year_data(self, year: int) -> dict[tuple[str, str], dict]:
        """Load canonical ``data.json`` rows for a year keyed by document key."""
        main_file = self._main_data_file(year)
        items = await self._read_items_file(main_file)
        data: dict[tuple[str, str], dict] = {}
        for item in items:
            key = self._doc_key(item)
            if key is not None:
                data[key] = item
        return data

    def _sanitize_filename(self, name: str) -> str:
        """Sanitize a string for use as a filename."""
        name = unidecode(name)
        name = self._format_regex_ws.sub("_", name)
        name = self._format_regex_special.sub("", name)
        return name.strip("_") or "document"

    async def cleanup(self) -> None:
        """Flush remaining documents."""
        await self.flush_all()

    async def _load_shard_data(
        self, shard_files: list[Path]
    ) -> dict[tuple[str, str], dict]:
        """Load and merge shard files into a keyed dict."""
        if not shard_files:
            return {}
        data: dict[tuple[str, str], dict] = {}
        shard_results = await asyncio.gather(
            *[self._read_items_file(p) for p in shard_files]
        )
        for items in shard_results:
            for item in items:
                key = self._doc_key(item)
                if key is not None:
                    data[key] = item
        return data

    async def _load_year_data(self, year: int) -> dict[tuple[str, str], dict]:
        """Load full year data from canonical file + pending shards."""
        data = await self._load_main_year_data(year)
        shard_data = await self._load_shard_data(self._list_shard_files(year))
        data.update(shard_data)
        pending = self._get_year_state(year).pending_docs
        if pending:
            data.update(pending)
        return data

    @staticmethod
    def _build_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
        """Compute summary statistics for a list of document dicts."""
        return {
            "count": len(items),
            "types_summary": aggregate_types_summary(items),
            "last_updated": datetime.now().isoformat(),
        }

    async def _write_year_data(
        self, year: int, data: dict[tuple[str, str], dict]
    ) -> None:
        """Atomically write merged data back to ``data.json`` for a year."""
        year_dir = self._year_dir(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        main_file = self._main_data_file(year)
        temp_file = year_dir / "data.json.tmp"
        all_items = list(data.values())
        envelope = {
            "summary": self._build_summary(all_items),
            "documents": all_items,
        }
        async with aiofiles.open(temp_file, "w", encoding="utf-8") as f:
            await f.write(
                json.dumps(envelope, ensure_ascii=False, indent=2, sort_keys=True)
            )
        temp_file.replace(main_file)

    async def get_scraped_keys(self, year: int) -> set[tuple[str, str]]:
        """Return set of (document_url, title) pairs already scraped for a year."""
        lock = self._get_year_lock(year)
        async with lock:
            data = await self._load_year_data(year)
            return set(data.keys())

    async def get_year_documents(self, year: int) -> list[dict[str, Any]]:
        """Return all saved document rows for a given year."""
        lock = self._get_year_lock(year)
        async with lock:
            data = await self._load_year_data(year)
            return list(data.values())

    async def get_all_documents(self) -> list[dict[str, Any]]:
        """Return all saved document rows across all years."""
        years = self._saved_years()
        if not years:
            return []

        year_batches = await asyncio.gather(
            *(self.get_year_documents(year) for year in years)
        )
        return [doc for batch in year_batches for doc in batch]

    async def get_dataset_summary(self) -> dict[str, Any]:
        """Build aggregate summary stats from all saved documents on disk."""
        documents = await self.get_all_documents()
        years = [
            year for doc in documents if (year := _normalize_year(doc.get("year")))
        ]
        return {
            "year_start": min(years) if years else None,
            "year_end": max(years) if years else None,
            "total_documents": len(documents),
            "types_summary": aggregate_types_summary(documents),
        }

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

                from src.scraper.base.schemas import SavedDocument

                try:
                    validated_doc = SavedDocument(**clean_data)
                    clean_data = validated_doc.model_dump()
                except Exception as e:
                    logger.error(
                        f"Pydantic validation failed for document '{clean_data.get('title', 'Unknown')}': {e}"
                    )
                    return None

                key = (clean_data.get("document_url", ""), clean_data.get("title", ""))
                state = self._get_year_state(year)
                state.pending_docs[key] = clean_data
                pending_count = len(state.pending_docs)

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
        """Flush buffered docs for a year into a new shard file.

        Must be called while holding the year lock.
        """
        state = self._get_year_state(year)
        pending = state.pending_docs
        if not pending:
            return

        shard_path = self._next_shard_path(year)
        payload = list(pending.values())
        async with aiofiles.open(shard_path, "w", encoding="utf-8") as f:
            await f.write(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
            )

        state.pending_docs = {}

    async def _compact_year(self, year: int) -> None:
        """Compact ``data.json`` + shard files into a single canonical file."""
        shard_files = self._list_shard_files(year)
        if not shard_files:
            return

        merged_data = await self._load_main_year_data(year)
        shard_data = await self._load_shard_data(shard_files)
        merged_data.update(shard_data)
        merged_from_shards = len(shard_data)

        await self._write_year_data(year, merged_data)

        for shard_path in shard_files:
            try:
                shard_path.unlink()
            except OSError as e:
                logger.warning(f"Could not remove shard {shard_path}: {e}")

        shards_dir = self._shards_dir(year)
        try:
            if shards_dir.exists() and not any(shards_dir.iterdir()):
                shards_dir.rmdir()
        except OSError as e:
            logger.debug(f"Could not remove shards directory {shards_dir}: {e}")

        logger.debug(
            f"Compacted year {year}: merged {merged_from_shards} docs from "
            f"{len(shard_files)} shards (total on disk: {len(merged_data)})"
        )

    async def flush(self, year: int) -> None:
        """Flush buffered documents and compact shard files for a year."""
        lock = self._get_year_lock(year)
        async with lock:
            await self._flush_year(year)
            await self._compact_year(year)

    async def flush_all(self) -> None:
        """Flush all buffered documents across all years to disk."""
        years = {
            y for y, s in self._year_states.items() if s.pending_docs
        } | self._years_with_shards()
        if years:
            await asyncio.gather(*(self.flush(year) for year in sorted(years)))

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

            year_dir = self.log_dir / str(data["year"])
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

        except Exception as e:
            error_msg = f"Error saving error data for '{data.get('title', 'Unknown')}'"
            if file_path:
                error_msg += f" to {file_path}"
            error_msg += f": {e}"
            logger.error(error_msg)
