"""Safety-net tests for BaseScraper orchestration methods.

Covers the core orchestration surface that every scraper depends on:
- scrape()          — main entry point, year iteration, error propagation
- _scrape_year()    — dict types, list types, _iterate_situations mode
- _paginate_until_end() — first-page-only, multi-page, empty responses
- _gather_results() — success, partial failures, all failures
- _save_summary()   — no-crash with empty results
- _process_documents() — doc fetching + saving flow
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from conftest import make_base_scraper
from src.scraper.base.scraper import BaseScraper


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TYPES = {"Lei Ordinária": 1, "Decreto": 2}
SITUATIONS = {"Vigente": 1, "Revogada": 2}


def _make_scraper(**kwargs) -> BaseScraper:
    """Instantiate BaseScraper bypassing __init__."""
    defaults = dict(
        years=list(range(2023, 2025)),
        year_start=2023,
        year_end=2024,
        use_browser=False,
        max_workers=5,
        rps=10,
        _scrape_start=time.time(),
        _overwrite_reset_years=set(),
        _mhtml_browser=None,
        _cleaned_up=False,
    )
    defaults.update(kwargs)
    return make_base_scraper(
        BaseScraper,
        "https://example.com",
        "TEST",
        TYPES,
        situations=SITUATIONS,
        **defaults,
    )


# ---------------------------------------------------------------------------
# _gather_results
# ---------------------------------------------------------------------------


class TestGatherResults:
    """Tests for BaseScraper._gather_results — asyncio.gather wrapper."""

    @pytest.mark.asyncio
    async def test_all_succeed(self):
        scraper = _make_scraper()

        async def ok(n):
            return {"title": f"doc-{n}"}

        results = await scraper._gather_results([ok(1), ok(2), ok(3)])
        assert len(results) == 3
        assert all(isinstance(r, dict) for r in results)

    @pytest.mark.asyncio
    async def test_partial_failures_filtered(self):
        scraper = _make_scraper()

        async def ok():
            return {"title": "good"}

        async def fail():
            raise ValueError("boom")

        results = await scraper._gather_results([ok(), fail(), ok()])
        assert len(results) == 2
        assert scraper.error_count == 1

    @pytest.mark.asyncio
    async def test_all_failures(self):
        scraper = _make_scraper()

        async def fail():
            raise RuntimeError("fail")

        results = await scraper._gather_results([fail(), fail()])
        assert results == []
        assert scraper.error_count == 2

    @pytest.mark.asyncio
    async def test_none_results_filtered(self):
        scraper = _make_scraper()

        async def none_result():
            return None

        async def ok():
            return {"title": "good"}

        results = await scraper._gather_results([none_result(), ok(), none_result()])
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_empty_tasks_returns_empty(self):
        scraper = _make_scraper()
        results = await scraper._gather_results([])
        assert results == []


# ---------------------------------------------------------------------------
# _scrape_year
# ---------------------------------------------------------------------------


class TestScrapeYear:
    """Tests for BaseScraper._scrape_year — type/situation iteration."""

    @pytest.mark.asyncio
    async def test_dict_types(self):
        """_scrape_year creates one _scrape_type call per dict entry."""
        scraper = _make_scraper()
        scraper._scrape_type = AsyncMock(return_value=[])

        await scraper._scrape_year(2023)
        assert scraper._scrape_type.call_count == len(TYPES)
        called_types = {call.args[0] for call in scraper._scrape_type.call_args_list}
        assert called_types == set(TYPES.keys())

    @pytest.mark.asyncio
    async def test_list_types(self):
        """_scrape_year handles list-style types (no IDs)."""
        list_types = ["Lei", "Decreto", "Resolução"]
        scraper = _make_scraper()
        scraper.types = list_types
        scraper._scrape_type = AsyncMock(return_value=[])

        await scraper._scrape_year(2023)
        assert scraper._scrape_type.call_count == len(list_types)
        for call in scraper._scrape_type.call_args_list:
            assert call.args[1] is None  # norm_type_id should be None for list types

    @pytest.mark.asyncio
    async def test_iterate_situations_mode(self):
        """When _iterate_situations=True, creates cross-product of situations × types."""
        scraper = _make_scraper()
        scraper._iterate_situations = True
        scraper._scrape_situation_type = AsyncMock(return_value=[])

        await scraper._scrape_year(2023)
        expected_count = len(SITUATIONS) * len(TYPES)
        assert scraper._scrape_situation_type.call_count == expected_count

    @pytest.mark.asyncio
    async def test_returns_flattened_results(self):
        """_scrape_year flattens nested lists from _scrape_type."""
        scraper = _make_scraper()
        scraper._scrape_type = AsyncMock(return_value=[{"title": "a"}, {"title": "b"}])

        results = await scraper._scrape_year(2023)
        assert len(results) == len(TYPES) * 2


# ---------------------------------------------------------------------------
# _paginate_until_end
# ---------------------------------------------------------------------------


class TestPaginateUntilEnd:
    """Tests for BaseScraper._paginate_until_end — page-by-page fetching."""

    @pytest.mark.asyncio
    async def test_single_page(self):
        """Stops after first page when end signal is True."""
        scraper = _make_scraper()

        async def make_task(page):
            return [{"title": f"doc-{page}"}], True  # ended=True

        result = await scraper._paginate_until_end(
            make_task=make_task,
            context={"year": 2023, "type": "Lei", "situation": ""},
        )
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_multi_page(self):
        """Fetches multiple pages until end signal."""
        scraper = _make_scraper()
        call_count = 0

        async def make_task(page):
            nonlocal call_count
            call_count += 1
            ended = page >= 3
            return [{"title": f"doc-{page}"}], ended

        result = await scraper._paginate_until_end(
            make_task=make_task,
            context={"year": 2023, "type": "Lei", "situation": ""},
            initial_batch=1,
        )
        assert len(result) >= 3

    @pytest.mark.asyncio
    async def test_empty_first_page(self):
        """Returns empty list when first page has no docs."""
        scraper = _make_scraper()

        async def make_task(page):
            return [], True

        result = await scraper._paginate_until_end(
            make_task=make_task,
            context={"year": 2023, "type": "Lei", "situation": ""},
        )
        assert result == []


# ---------------------------------------------------------------------------
# scrape
# ---------------------------------------------------------------------------


class TestScrape:
    """Tests for BaseScraper.scrape — main entry point."""

    @pytest.mark.asyncio
    async def test_iterates_all_years(self):
        """scrape() calls _scrape_year for each year and tracks count."""
        scraper = _make_scraper()
        scraper.saver = MagicMock()
        scraper.saver.flush = AsyncMock()
        scraper.saver.flush_all = AsyncMock()
        scraper.saver.get_dataset_summary = AsyncMock(
            return_value={
                "year_start": 2023,
                "year_end": 2024,
                "total_documents": 0,
                "types_summary": {},
            }
        )
        scraper.saver.save_dir = "/fake"
        scraper._load_scraped_keys = AsyncMock()
        scraper._scrape_year = AsyncMock(return_value=[{"title": "d1", "type": "Lei"}])
        scraper._before_scrape = AsyncMock()
        scraper._save_summary = AsyncMock()

        total = await scraper.scrape()
        assert scraper._scrape_year.call_count == 2  # 2023, 2024
        assert total == 2  # 1 doc per year × 2 years

    @pytest.mark.asyncio
    async def test_raises_without_saver(self):
        """scrape() raises RuntimeError if saver not initialized."""
        scraper = _make_scraper()
        # saver is None by default from make_base_scraper
        with pytest.raises(RuntimeError, match="Saver is not initialized"):
            await scraper.scrape()

    @pytest.mark.asyncio
    async def test_scrape_year_error_propagates(self):
        """Errors in _scrape_year propagate through scrape()."""
        scraper = _make_scraper()
        scraper.saver = MagicMock()
        scraper.saver.flush = AsyncMock()
        scraper._load_scraped_keys = AsyncMock()
        scraper._before_scrape = AsyncMock()
        scraper._scrape_year = AsyncMock(side_effect=RuntimeError("network down"))

        with pytest.raises(RuntimeError, match="network down"):
            await scraper.scrape()


# ---------------------------------------------------------------------------
# _save_summary
# ---------------------------------------------------------------------------


class TestSaveSummary:
    """Tests for BaseScraper._save_summary — summary persistence."""

    @pytest.mark.asyncio
    async def test_empty_results_no_crash(self, tmp_path):
        """_save_summary doesn't crash with zero documents and no OCR service."""
        scraper = _make_scraper()
        scraper.saver = MagicMock()
        scraper.saver.flush_all = AsyncMock()
        scraper.saver.get_dataset_summary = AsyncMock(
            return_value={
                "year_start": 2023,
                "year_end": 2024,
                "total_documents": 0,
                "types_summary": {},
            }
        )
        scraper.saver.save_dir = str(tmp_path)

        await scraper._save_summary()

        scraper.saver.flush_all.assert_awaited_once()
        summary_file = tmp_path / "summary.json"
        assert summary_file.exists()


# ---------------------------------------------------------------------------
# _process_documents
# ---------------------------------------------------------------------------


class TestProcessDocuments:
    """Tests for BaseScraper._process_documents — doc fetch + save orchestration."""

    @pytest.mark.asyncio
    async def test_processes_all_documents(self):
        """_process_documents calls _get_doc_data for each doc, then saves."""
        scraper = _make_scraper()

        async def mock_get_doc_data(doc):
            return {
                "title": doc["title"],
                "text_markdown": "content",
                "document_url": f"https://example.com/{doc['title']}",
                "year": doc.get("year", 2023),
                "type": doc.get("type", "Lei"),
                "situation": doc.get("situation", ""),
            }

        scraper._get_doc_data = mock_get_doc_data
        scraper._save_doc_result = AsyncMock(side_effect=lambda doc: doc)

        documents = [
            {"title": "doc1", "url": "https://example.com/1"},
            {"title": "doc2", "url": "https://example.com/2"},
        ]

        results = await scraper._process_documents(
            documents, year=2023, norm_type="Lei"
        )
        assert len(results) == 2
        assert scraper._save_doc_result.call_count == 2

    @pytest.mark.asyncio
    async def test_empty_documents_returns_empty(self):
        """_process_documents with no docs returns empty list without errors."""
        scraper = _make_scraper()
        scraper._get_doc_data = AsyncMock()

        results = await scraper._process_documents([], year=2023, norm_type="Lei")
        assert results == []
        scraper._get_doc_data.assert_not_called()
