"""Transactional document storage and streaming archive export."""

import asyncio
import hashlib
import json
import os
import re
import shutil
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.core.document import Document

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
    errors INTEGER NOT NULL DEFAULT 0, llm_usage TEXT NOT NULL DEFAULT '{}'
);
"""


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
    ) -> None:
        if doc.raw is not None:
            if self.path == ":memory:":
                raise ValueError("Raw files require a filesystem store")
            ext = doc.raw_ext or ".bin"
            if not re.fullmatch("\\.[a-zA-Z0-9]+", ext):
                raise ValueError(f"Invalid raw extension: {ext!r}")
            identity = json.dumps([ref_url, ref_title], ensure_ascii=False).encode()
            # Keep the previous raw file intact if the SQL transaction fails.
            digest = hashlib.sha256(identity + b"\x00" + doc.raw).hexdigest()
            target = self.root / str(doc.year) / "docs" / f"{digest}{ext}"
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_suffix(ext + ".tmp")
            try:
                temporary.write_bytes(doc.raw)
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
            file_path = str(target)
        data = doc.model_dump()
        extra = {
            key: value
            for key, value in data.items()
            if key not in Document.model_fields
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

    def _save(
        self, doc: Document, ref_url: str, ref_title: str, file_path: str | None
    ) -> None:
        with self.connection:
            self._insert(doc, ref_url, ref_title, file_path)

    async def save_many(
        self, docs: list[Document], *, ref_url: str, ref_title: str
    ) -> None:
        def save() -> None:
            # Commit all outputs together so resume never skips a partial listing.
            with self.connection:
                for index, doc in enumerate(docs):
                    title = ref_title if index == 0 else f"{ref_title} [{index}]"
                    self._insert(doc, ref_url, title, None)

        await self._call(save)

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

    async def has(self, url: str, title: str) -> bool:
        return await self._call(
            lambda: (
                self.connection.execute(
                    "SELECT 1 FROM documents WHERE ref_url=? AND ref_title=?",
                    (url, title),
                ).fetchone()
                is not None
            )
        )

    async def reset_year(self, year: int) -> None:
        def reset() -> None:
            with self.connection:
                self.connection.execute("DELETE FROM documents WHERE year=?", (year,))
            folder = self.root / str(year) / "docs"
            if self.path != ":memory:" and folder.exists():
                shutil.rmtree(folder)

        await self._call(reset)

    async def start_run(self, scraper: str) -> int:
        def start() -> int:
            with self.connection:
                cursor = self.connection.execute(
                    "INSERT INTO runs(scraper) VALUES (?)", (scraper,)
                )
                return cursor.lastrowid

        return await self._call(start)

    async def finish_run(
        self, run_id: int, docs: int, errors: int, usage: dict
    ) -> None:
        def finish() -> None:
            with self.connection:
                self.connection.execute(
                    "UPDATE runs SET finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),\n                    docs=?, errors=?, llm_usage=? WHERE id=?",
                    (docs, errors, json.dumps(usage), run_id),
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
                            for key in ("ref_url", "ref_title", "scraped_at"):
                                data.pop(key)
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

    async def import_json(self, directory: str | Path) -> int:
        def import_files() -> int:
            count = 0
            root = Path(directory)
            paths = sorted(root.glob("[0-9]*/data.json")) + sorted(
                root.glob("[0-9]*/shards/*.json")
            )
            for path in paths:
                rows = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(rows, list):
                    raise ValueError(f"Expected JSON array in {path}")
                for row in rows:
                    doc = Document.model_validate(row)
                    self._save(
                        doc,
                        row.get("ref_url") or doc.document_url,
                        row.get("ref_title") or doc.title,
                        row.get("file_path"),
                    )
                    count += 1
            return count

        return await self._call(import_files)
