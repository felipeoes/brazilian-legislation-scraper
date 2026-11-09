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
