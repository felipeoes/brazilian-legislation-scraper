import asyncio
from dataclasses import replace
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

import main
from src.core.scraper import BaseScraper
from src.registry import SCRAPERS, ScraperSpec


def test_selection_aliases_and_unknown_names():
    assert main.select_scrapers(["PB"])[0].cls.name == "PARAIBA"
    assert len(main.select_scrapers(["Conama", "CONAMASCRAPER"])) == 1
    with pytest.raises(ValueError, match="Unknown"):
        main.select_scrapers(["misspelled"])
    assert len(SCRAPERS) == 30


def test_registry_years_and_bahia_rate():
    bahia = main.select_scrapers(["BA"])[0]
    assert bahia.rps == 1  # the server returns 500s above 1 request per second
    assert bahia.years.start == 1891 and bahia.years.stop == datetime.now().year + 1


@pytest.mark.parametrize("argv", [[], ["run"], ["run", "--all", "-s", "AC"]])
def test_cli_requires_explicit_selection(argv):
    with pytest.raises(SystemExit) as exc:
        main.parse_args(argv)
    assert exc.value.code == 2


@pytest.mark.parametrize("failed", [False, True])
async def test_exit_code_reflects_scraper_failure(monkeypatch, failed):
    monkeypatch.setattr(main, "LLM_MODEL", "")
    monkeypatch.setattr(main, "run_scraper", AsyncMock(return_value=not failed))
    assert await main.main(["run", "-s", "AC"]) == int(failed)


async def test_cleanup_failure_sets_exit_one(tmp_path, monkeypatch):
    class BrokenCleanup(BaseScraper):
        name = "BROKEN"
        base_url = "https://example.org"

        async def list_documents(self, year):
            if False:
                yield {}

        async def __aexit__(self, *exc):
            await super().__aexit__(*exc)
            raise RuntimeError("cleanup failed")

    monkeypatch.setattr(main, "LLM_MODEL", "")
    monkeypatch.setattr(
        main,
        "SCRAPERS",
        [ScraperSpec(BrokenCleanup, ("bad",), 2024, save_dir=tmp_path)],
    )
    monkeypatch.setattr("src.core.scraper.LOG_DIR", tmp_path)
    assert await main.main(["run", "-s", "bad"]) == 1


async def test_import_and_export_cli(tmp_path, monkeypatch):
    import json

    from test_store import document

    spec = replace(SCRAPERS[3], save_dir=tmp_path)
    source = spec.database.parent / "2024/data.json"
    source.parent.mkdir(parents=True)
    source.write_text(json.dumps([document().model_dump()]))
    monkeypatch.setattr(main, "SCRAPERS", [spec])
    assert await main.main(["import-json", "-s", "AC"]) == 0
    out = tmp_path / "export"
    assert await main.main(["export", "-s", "AC", "--out", str(out)]) == 0
    assert (
        json.loads((out / "2024/data.jsonl").read_text())["title"] == document().title
    )
    assert source.exists()
    source.write_text(json.dumps([{**document().model_dump(), "situation": ""}]))
    assert await main.main(["import-json", "-s", "AC"]) == 1


class FakeProcess:
    def __init__(self, code: int | None) -> None:
        self.code, self.returncode = code, None
        self.stopped = asyncio.Event()

    async def wait(self) -> int:
        if self.code is None:  # runs until terminated
            await self.stopped.wait()
            return -15
        self.returncode = self.code
        return self.code

    def terminate(self) -> None:
        self.returncode = -15
        self.stopped.set()


def _three_specs(tmp_path):
    return [
        replace(SCRAPERS[3], save_dir=tmp_path, needs_ocr=True),
        replace(SCRAPERS[4], save_dir=tmp_path, needs_ocr=True),
        replace(SCRAPERS[5], save_dir=tmp_path, needs_ocr=False),
    ]


async def test_several_scrapers_run_in_child_processes(tmp_path, monkeypatch):
    specs = _three_specs(tmp_path)
    calls = []

    async def spawn(*argv, env):
        calls.append((argv, env))
        return FakeProcess(1 if specs[1].cls.name in argv else 0)

    monkeypatch.setattr(main, "SCRAPERS", specs)
    monkeypatch.setattr(main, "LLM_CONCURRENCY", 8)
    monkeypatch.setattr(main.asyncio, "create_subprocess_exec", spawn)
    code = await main.main(["run", "--all", "--year", "2012", "--overwrite"])
    assert code == 1  # one child failed
    assert sorted(argv[argv.index("-s") + 1] for argv, _ in calls) == sorted(
        spec.cls.name for spec in specs
    )
    for argv, env in calls:
        assert argv[-3:] == ("--year", "2012", "--overwrite")
        # An even split starved the one OCR-heavy source; the limit is per process.
        assert env.get("LLM_CONCURRENCY") == main.os.environ.get("LLM_CONCURRENCY")
        assert int(env["PDF_WORKERS"]) >= 1


async def test_cancelled_runner_terminates_its_children(tmp_path, monkeypatch):
    processes = []

    async def spawn(*argv, env):
        processes.append(FakeProcess(None))
        return processes[-1]

    monkeypatch.setattr(main, "SCRAPERS", _three_specs(tmp_path))
    monkeypatch.setattr(main.asyncio, "create_subprocess_exec", spawn)
    task = asyncio.create_task(main.main(["run", "--all", "--jobs", "2"]))
    while len(processes) < 2:
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(processes) == 2  # --jobs bounds concurrent children
    assert all(process.returncode == -15 for process in processes)


async def test_stats_command_prints_latest_run_per_archive(
    tmp_path, monkeypatch, capsys
):
    from src.core.store import Store

    spec = replace(SCRAPERS[3], save_dir=tmp_path)
    async with Store(spec.database) as store:
        old = await store.start_run(spec.cls.name)
        await store.finish_run(old, 1, 0, {}, {"seconds": 1.0, "docs": 1})
        run = await store.start_run(spec.cls.name)
        stats = {
            "seconds": 90.0,
            "listed": 166,
            "docs": 166,
            "docs_per_minute": 110.7,
            "fetch_p50": 0.4,
            "http": {"requests": 400, "retries": 2, "bytes": 5_000_000},
        }
        await store.finish_run(run, 166, 0, {"m": {"requests": 3}}, stats)
    monkeypatch.setattr(main, "SCRAPERS", [spec])
    assert await main.main(["stats"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("| Scraper |")
    row = next(line for line in out.splitlines() if spec.cls.name in line)
    assert (
        "| 166 |" in row and "| 90 |" in row and "| 110.7 |" in row and "| 4.8 |" in row
    )
