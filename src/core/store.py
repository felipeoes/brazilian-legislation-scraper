"""Transactional document storage and streaming archive export."""

import asyncio
import hashlib
import json
import os
import re
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from src.core.document import Document, clean_title

_SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    ref_url TEXT NOT NULL, ref_title TEXT NOT NULL,
    document_url TEXT NOT NULL, title TEXT NOT NULL, year INTEGER NOT NULL,
    type TEXT NOT NULL, situation TEXT NOT NULL, summary TEXT NOT NULL,
    text_markdown TEXT NOT NULL, file_path TEXT, extra TEXT NOT NULL,
    scraped_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (ref_url, ref_title)
);
CREATE INDEX IF NOT EXISTS documents_year ON documents(year);
CREATE TABLE IF NOT EXISTS errors (
    run_id INTEGER, at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    year INTEGER, type TEXT, situation TEXT, title TEXT, url TEXT, message TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY, scraper TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    finished_at TEXT, docs INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0, llm_usage TEXT NOT NULL DEFAULT '{}',
    stats TEXT NOT NULL DEFAULT '{}'
);
"""
# Raw files this store wrote: <year>/docs/<sha256><ext>, relative to the archive.
_OWNED_RAW_RE = re.compile("\\d+/docs/[0-9a-f]{64}\\.[a-zA-Z0-9]+")


class Store:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self.root = Path(path).parent
        self._lock = asyncio.Lock()
        self._connection: sqlite3.Connection | None = None

    async def _call[T](self, operation: Callable[..., T], *args: Any) -> T:
        async with self._lock:
            task = asyncio.create_task(asyncio.to_thread(operation, *args))
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                # Finish the SQLite call before another task can use the connection.
                await task
                raise

    def _open(self) -> None:
        if self._connection is not None:
            return
        if self.path != ":memory:":
            self.root.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, check_same_thread=False)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.executescript(_SCHEMA)
            columns = {row[1] for row in connection.execute("PRAGMA table_info(runs)")}
            if "stats" not in columns:  # archives created before run stats
                connection.execute(
                    "ALTER TABLE runs ADD COLUMN stats TEXT NOT NULL DEFAULT '{}'"
                )
        except BaseException:
            connection.close()
            raise
        self._connection = connection

    async def open(self) -> None:
        await self._call(self._open)

    @property
    def connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("Store is not open")
        return self._connection

    async def close(self) -> None:
        def close() -> None:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

        await self._call(close)

    async def __aenter__(self) -> Store:
        try:
            await self.open()
        except BaseException:
            await self.close()
            raise
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def _insert(
        self, doc: Document, ref_url: str, ref_title: str, file_path: str | None
    ) -> str | None:
        """Write the row (and raw file); returns the stored file_path."""
        if doc.raw is not None:
            if self.path == ":memory:":
                raise ValueError("Raw files require a filesystem store")
            file_path = self._raw_path(doc, ref_url, ref_title)
            target = self.root / file_path
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(target.name + ".tmp")
            try:
                temporary.write_bytes(doc.raw)
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
        data = doc.model_dump()
        # Keys starting with "_" are listing-only helpers, never archive data.
        extra = {
            key: value
            for key, value in data.items()
            if key not in Document.model_fields and not key.startswith("_")
        }
        extra.pop("file_path", None)
        self.connection.execute(
            "INSERT OR REPLACE INTO documents\n                (ref_url, ref_title, document_url, title, year, type, situation,\n                 summary, text_markdown, file_path, extra)\n                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                ref_url,
                ref_title,
                doc.document_url,
                doc.title,
                doc.year,
                doc.type,
                doc.situation,
                doc.summary,
                doc.text_markdown,
                file_path,
                json.dumps(extra, ensure_ascii=False),
            ),
        )
        return file_path

    @staticmethod
    def _raw_path(doc: Document, ref_url: str, ref_title: str) -> str:
        ext = doc.raw_ext or ".bin"
        if not re.fullmatch("\\.[a-zA-Z0-9]+", ext):
            raise ValueError(f"Invalid raw extension: {ext!r}")
        identity = json.dumps([ref_url, ref_title], ensure_ascii=False).encode()
        # Content-addressed, so a failed transaction never overwrites the old file.
        digest = hashlib.sha256(identity + b"\x00" + doc.raw).hexdigest()
        return f"{doc.year}/docs/{digest}{ext}"

    def _owned_raw(self, file_path: str | None) -> Path | None:
        """The raw file behind file_path, only if this store wrote it."""
        if not file_path:
            return None
        path = Path(file_path)
        if path.is_absolute():  # archives written before paths became relative
            if not path.is_relative_to(self.root):
                return None
            file_path = path.relative_to(self.root).as_posix()
        return self.root / file_path if _OWNED_RAW_RE.fullmatch(file_path) else None

    def _save(
        self, doc: Document, ref_url: str, ref_title: str, file_path: str | None
    ) -> None:
        row = self.connection.execute(
            "SELECT file_path FROM documents WHERE ref_url=? AND ref_title=?",
            (ref_url, ref_title),
        ).fetchone()
        old = self._owned_raw(row[0]) if row else None
        # Validates raw_ext before anything is written.
        new = (
            self.root / self._raw_path(doc, ref_url, ref_title)
            if doc.raw is not None
            else None
        )
        try:
            with self.connection:
                self._insert(doc, ref_url, ref_title, file_path)
        except BaseException:
            # Content-addressed: a path other than the old one is a fresh orphan.
            if new is not None and new != old and self.path != ":memory:":
                new.unlink(missing_ok=True)
            raise
        if old is not None and old != new:
            old.unlink(missing_ok=True)

    async def save(
        self,
        doc: Document,
        *,
        ref_url: str | None = None,
        ref_title: str | None = None,
        file_path: str | None = None,
    ) -> None:
        await self._call(
            self._save,
            doc,
            ref_url or doc.document_url,
            ref_title or doc.title,
            file_path,
        )

    async def keys(self, year: int | None = None) -> set[tuple[str, str]]:
        def select() -> set[tuple[str, str]]:
            sql = "SELECT ref_url, ref_title FROM documents"
            rows = self.connection.execute(
                sql if year is None else sql + " WHERE year=?",
                () if year is None else (year,),
            )
            return {(row[0], row[1]) for row in rows}

        return await self._call(select)

    async def start_run(self, scraper: str) -> int:
        def start() -> int:
            with self.connection:
                cursor = self.connection.execute(
                    "INSERT INTO runs(scraper) VALUES (?)", (scraper,)
                )
                return cursor.lastrowid

        return await self._call(start)

    async def finish_run(
        self,
        run_id: int,
        docs: int,
        errors: int,
        usage: dict,
        stats: dict | None = None,
    ) -> None:
        def finish() -> None:
            with self.connection:
                self.connection.execute(
                    "UPDATE runs SET finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),\n                    docs=?, errors=?, llm_usage=?, stats=? WHERE id=?",
                    (docs, errors, json.dumps(usage), json.dumps(stats or {}), run_id),
                )

        await self._call(finish)

    async def record_error(
        self,
        *,
        message: str,
        run_id: int | None = None,
        year: int | None = None,
        type: str = "",
        situation: str = "",
        title: str = "",
        url: str = "",
    ) -> None:
        def insert() -> None:
            with self.connection:
                self.connection.execute(
                    "INSERT INTO errors(run_id, year, type, situation, title, url, message) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (run_id, year, type, situation, title, url, message),
                )

        await self._call(insert)

    async def summary(self) -> list[dict]:
        return await self._call(
            lambda: [
                dict(row)
                for row in self.connection.execute(
                    "SELECT year, type, situation, count(*) AS documents FROM documents GROUP BY year, type, situation ORDER BY year, type, situation"
                )
            ]
        )

    async def export(self, out_dir: str | Path, fmt: str = "jsonl") -> int:
        if fmt not in {"json", "jsonl"}:
            raise ValueError("Export format must be json or jsonl")

        def export() -> int:
            count = 0
            years = [
                row[0]
                for row in self.connection.execute(
                    "SELECT DISTINCT year FROM documents ORDER BY year"
                )
            ]
            for year in years:
                target = Path(out_dir) / str(year) / f"data.{fmt}"
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary = target.with_suffix(target.suffix + ".tmp")
                try:
                    with temporary.open("w", encoding="utf-8") as output:
                        if fmt == "json":
                            output.write("[\n")
                        first = True
                        for row in self.connection.execute(
                            "SELECT * FROM documents WHERE year=? ORDER BY ref_url, ref_title",
                            (year,),
                        ):
                            data = dict(row)
                            extra = json.loads(data.pop("extra"))
                            # ref_url/ref_title stay: import needs them as resume keys.
                            data.pop("scraped_at")
                            if fmt == "json" and (not first):
                                output.write(",\n")
                            output.write(
                                json.dumps({**extra, **data}, ensure_ascii=False)
                            )
                            if fmt == "jsonl":
                                output.write("\n")
                            first = False
                            count += 1
                        if fmt == "json":
                            output.write("\n]\n")
                    os.replace(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
            return count

        return await self._call(export)

    async def import_json(self, directory: str | Path) -> tuple[int, int]:
        """Import JSON archives; returns (imported, rejected). Rejected rows go to errors."""

        def import_files() -> tuple[int, int]:
            imported = rejected = 0
            root = Path(directory)
            paths = sorted(root.glob("[0-9]*/data.json")) + sorted(
                root.glob("[0-9]*/shards/*.json")
            )
            for path in paths:
                rows = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(rows, dict):  # legacy saver: {"summary", "documents"}
                    rows = rows.get("documents")
                if not isinstance(rows, list):
                    raise ValueError(f"Expected a JSON array of documents in {path}")
                with self.connection:
                    for row in rows:
                        row = dict(row)
                        ref_url, ref_title = (
                            row.pop("ref_url", None),
                            row.pop("ref_title", None),
                        )
                        try:
                            doc = Document.model_validate(row)
                        except ValidationError as exc:
                            rejected += 1
                            self.connection.execute(
                                "INSERT INTO errors(year, type, situation, title, url, message) VALUES (?, ?, ?, ?, ?, ?)",
                                (
                                    row.get("year"),
                                    row.get("type"),
                                    row.get("situation"),
                                    row.get("title"),
                                    row.get("document_url"),
                                    f"import {path}: {exc}",
                                ),
                            )
                            continue
                        self._insert(
                            doc,
                            ref_url or doc.document_url,
                            clean_title(ref_title) if ref_title else doc.title,
                            row.get("file_path"),
                        )
                        imported += 1
            return imported, rejected

        return await self._call(import_files)
