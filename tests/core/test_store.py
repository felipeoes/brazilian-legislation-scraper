import json
import sqlite3
import tracemalloc

import pytest

from src.core.document import Document
from src.core.store import Store


def document(**kwargs) -> Document:
    return Document(
        **{
            "year": 2024,
            "title": "Lei 1",
            "type": "Lei",
            "situation": "Vigente",
            "document_url": "https://example.org/1",
            "text_markdown": "Art. 1º Esta lei estabelece as disposições para a administração pública.",
            **kwargs,
        }
    )


async def test_long_titles_and_stable_listing_identity(tmp_path):
    async with Store(tmp_path / "documents.db") as store:
        for suffix in ("one", "two"):
            await store.save(
                document(title="a" * 300 + suffix, raw=suffix.encode(), raw_ext=".pdf"),
                ref_url="listing",
                ref_title=suffix,
            )
        assert await store.keys() == {("listing", "one"), ("listing", "two")}
        assert len(list((tmp_path / "2024/docs").glob("*.pdf"))) == 2
        for fmt in ("json", "jsonl"):
            assert await store.export(tmp_path / "export", fmt) == 2
            text = (tmp_path / f"export/2024/data.{fmt}").read_text()
            rows = (
                json.loads(text)
                if fmt == "json"
                else [json.loads(line) for line in text.splitlines()]
            )
            assert {row["document_url"] for row in rows} == {"https://example.org/1"}
            assert all("raw" not in row for row in rows)


async def test_failed_insert_preserves_existing_row_and_file(tmp_path):
    async with Store(tmp_path / "documents.db") as store:
        await store.save(document(raw=b"old", raw_ext=".pdf"))
        store.connection.execute(
            "CREATE TRIGGER fail BEFORE INSERT ON documents BEGIN SELECT RAISE(ABORT, 'crash'); END"
        )
        with pytest.raises(sqlite3.IntegrityError):
            await store.save(document(raw=b"new", raw_ext=".pdf"))
        row = store.connection.execute("SELECT file_path FROM documents").fetchone()
        from pathlib import Path

        assert Path(row[0]).read_bytes() == b"old"
        assert len(await store.keys()) == 1


async def test_summary_does_not_load_bodies(tmp_path):
    async with Store(tmp_path / "documents.db") as store:
        body = "x" * (1024 * 1024)
        for i in range(100):
            await store.save(document(title=str(i), text_markdown=body))
        tracemalloc.start()
        summary = await store.summary()
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert summary[0]["documents"] == 100
        assert peak < 1024 * 1024


async def test_import_fails_loudly_without_deleting_inputs(tmp_path):
    source = tmp_path / "2024/data.json"
    source.parent.mkdir()
    source.write_text("[broken")
    async with Store(tmp_path / "documents.db") as store:
        with pytest.raises(json.JSONDecodeError):
            await store.import_json(tmp_path)
        assert source.read_text() == "[broken"
        source.write_text(json.dumps([document(file_path="old.pdf").model_dump()]))
        assert await store.import_json(tmp_path) == 1
        assert (
            store.connection.execute("SELECT file_path FROM documents").fetchone()[0]
            == "old.pdf"
        )


async def test_reset_and_error_runs(tmp_path):
    async with Store(tmp_path / "documents.db") as store:
        run = await store.start_run("TEST")
        await store.record_error(run_id=run, message="failed", year=2024)
        await store.finish_run(run, 1, 1, {"test": {"requests": 1}})
        await store.save(document(raw=b"pdf", raw_ext=".pdf"))
        await store.reset_year(2024)
        assert await store.keys() == set()
        assert not (tmp_path / "2024/docs").exists()
        assert store.connection.execute("SELECT errors FROM runs").fetchone()[0] == 1


@pytest.mark.parametrize(
    "field", ["title", "type", "situation", "document_url", "text_markdown"]
)
def test_document_validates_required_text(field):
    with pytest.raises(ValueError):
        document(**{field: " "})


async def test_multiple_outputs_rollback_together(tmp_path):
    async with Store(tmp_path / "documents.db") as store:
        store.connection.execute(
            "CREATE TRIGGER fail BEFORE INSERT ON documents WHEN NEW.title = 'second' BEGIN SELECT RAISE(ABORT, 'crash'); END"
        )
        with pytest.raises(sqlite3.IntegrityError):
            await store.save_many(
                [document(title="first"), document(title="second")],
                ref_url="listing",
                ref_title="reference",
            )
        assert not await store.has("listing", "reference")
        assert await store.summary() == []


async def test_cancelled_context_entry_closes_open_connection():
    import asyncio

    store = Store(":memory:")
    original_open = store.open

    async def interrupted_open():
        await original_open()
        raise asyncio.CancelledError

    store.open = interrupted_open
    with pytest.raises(asyncio.CancelledError):
        async with store:
            pass
    assert store._connection is None
