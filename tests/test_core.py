"""Tests for core infrastructure modules.

Covers:
- clean_md_tag utility
- RateLimiter concurrency primitive
- BaseScraper markdown utilities and helpers
- RequestService (FailedRequest, detect_content_info)
"""

import asyncio
import json
import tempfile
import fitz
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.utils import clean_md_tag
from src.utils.concurrency import RateLimiter
from src.services.request.service import FailedRequest, RequestService


# =========================================================================
# clean_md_tag utility
# =========================================================================


class TestCleanMdTag:
    def test_strip_markdown_fence(self):
        assert clean_md_tag("```markdown\n# Hello\n```") == "# Hello"

    def test_strip_md_fence(self):
        assert clean_md_tag("```md\n# Hello\n```") == "# Hello"

    def test_no_fence(self):
        assert clean_md_tag("# Hello") == "# Hello"

    def test_only_opening_fence(self):
        assert clean_md_tag("```markdown\n# Hello") == "# Hello"

    def test_only_closing_fence(self):
        assert clean_md_tag("# Hello\n```") == "# Hello"

    def test_empty_string(self):
        assert clean_md_tag("") == ""

    def test_preserves_inner_backticks(self):
        result = clean_md_tag("```markdown\nsome `code` here\n```")
        assert "`code`" in result


# =========================================================================
# RateLimiter
# =========================================================================


class TestRateLimiter:
    @pytest.mark.asyncio
    async def test_acquire_does_not_block_below_limit(self):
        limiter = RateLimiter(1000)
        start = asyncio.get_event_loop().time()
        for _ in range(5):
            await limiter.acquire()
        elapsed = asyncio.get_event_loop().time() - start
        # With 1000 rps, 5 requests should be nearly instant
        assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_acquire_rate_limits(self):
        limiter = RateLimiter(2)  # 2 requests per second
        start = asyncio.get_event_loop().time()
        for _ in range(3):
            await limiter.acquire()
        elapsed = asyncio.get_event_loop().time() - start
        # 3 requests at 2 rps should take at least 0.5 seconds
        assert elapsed >= 0.4

    @pytest.mark.asyncio
    async def test_fractional_rps(self):
        limiter = RateLimiter(0.5)  # 1 request every 2 seconds
        start = asyncio.get_event_loop().time()
        await limiter.acquire()
        await limiter.acquire()
        elapsed = asyncio.get_event_loop().time() - start
        # Second request should wait ~2 seconds
        assert elapsed >= 1.5

    def test_stores_requests_per_second(self):
        limiter = RateLimiter(42)
        assert limiter.requests_per_second == 42

    @pytest.mark.asyncio
    async def test_no_burst_even_spacing(self):
        """Verify requests are evenly spaced (no burst).

        With rps=5, each request should be spaced ~200ms apart.
        If burst were allowed, all 5 would fire instantly then wait 1s.
        """
        limiter = RateLimiter(5)
        timestamps = []
        for _ in range(4):
            await limiter.acquire()
            timestamps.append(asyncio.get_event_loop().time())

        gaps = [timestamps[i + 1] - timestamps[i] for i in range(len(timestamps) - 1)]
        expected_gap = 1.0 / 5  # 200ms
        for gap in gaps:
            # Each gap should be close to 200ms (allow ±100ms tolerance)
            assert gap >= expected_gap - 0.1, (
                f"Gap {gap:.3f}s too short (expected ~{expected_gap:.3f}s)"
            )

    @pytest.mark.asyncio
    async def test_concurrent_acquires_are_serialized(self):
        """Multiple concurrent acquire() calls should still be rate-limited."""
        limiter = RateLimiter(5)  # 1 request per 200ms
        timestamps = []

        async def worker():
            await limiter.acquire()
            timestamps.append(asyncio.get_event_loop().time())

        # Launch 4 workers concurrently
        await asyncio.gather(*[worker() for _ in range(4)])

        total_elapsed = timestamps[-1] - timestamps[0]
        # 4 requests at 5 rps → at least 3 gaps of ~200ms = ~600ms
        assert total_elapsed >= 0.4, (
            f"Elapsed {total_elapsed:.3f}s — concurrent requests were not serialized"
        )

    @pytest.mark.asyncio
    async def test_independent_limiters_do_not_interfere(self):
        """Two separate RateLimiter instances should not affect each other."""
        RateLimiter(1)  # 1 rps — exists to verify it doesn't affect `fast`
        fast = RateLimiter(100)  # 100 rps

        start = asyncio.get_event_loop().time()
        for _ in range(5):
            await fast.acquire()
        elapsed = asyncio.get_event_loop().time() - start

        # Fast limiter should not be slowed by the existence of the slow one
        assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_very_low_fractional_rps(self):
        """Verify rps=0.33 (~1 request every 3 seconds) works."""
        limiter = RateLimiter(0.33)
        start = asyncio.get_event_loop().time()
        await limiter.acquire()
        await limiter.acquire()
        elapsed = asyncio.get_event_loop().time() - start
        # Second request should wait ~3 seconds
        assert elapsed >= 2.5


# =========================================================================
# FailedRequest
# =========================================================================


class TestFailedRequest:
    def test_is_falsy(self):
        fr = FailedRequest(url="http://example.com")
        assert not fr
        assert bool(fr) is False

    def test_carries_diagnostics(self):
        fr = FailedRequest(url="http://example.com", status=404, reason="Not Found")
        assert fr.url == "http://example.com"
        assert fr.status == 404
        assert fr.reason == "Not Found"

    def test_repr(self):
        fr = FailedRequest(url="http://example.com", status=500)
        r = repr(fr)
        assert "FailedRequest" in r
        assert "500" in r


# =========================================================================
# RequestService.detect_content_info
# =========================================================================


class TestDetectContentInfo:
    def _mock_response(self, content_type="text/html", disposition=""):
        resp = MagicMock()
        resp.content_type = content_type
        resp.headers = {"Content-Disposition": disposition} if disposition else {}
        return resp

    def test_html(self):
        resp = self._mock_response("text/html")
        filename, ct = RequestService.detect_content_info(resp)
        assert "html" in filename
        assert "html" in ct

    def test_pdf(self):
        resp = self._mock_response("application/pdf")
        filename, ct = RequestService.detect_content_info(resp)
        assert "pdf" in filename
        assert "pdf" in ct

    def test_content_disposition(self):
        resp = self._mock_response(
            "application/pdf",
            'attachment; filename="lei_123.pdf"',
        )
        filename, ct = RequestService.detect_content_info(resp)
        assert "lei_123.pdf" in filename

    def test_unknown_type(self):
        resp = self._mock_response("application/octet-stream")
        filename, ct = RequestService.detect_content_info(resp)
        assert filename == "document"


# =========================================================================
# BaseScraper helpers
# =========================================================================


class TestBaseScraperHelpers:
    """Test BaseScraper static/class methods and utilities without full init."""

    def test_detect_extension_pdf(self):
        from src.scraper.base.converter import detect_extension

        assert detect_extension("application/pdf") == ".pdf"

    def test_detect_extension_html(self):
        from src.scraper.base.converter import detect_extension

        assert detect_extension("text/html") == ".html"

    def test_detect_extension_from_filename(self):
        from src.scraper.base.converter import detect_extension

        assert detect_extension("", "document.docx") == ".docx"

    def test_detect_extension_unknown(self):
        from src.scraper.base.converter import detect_extension

        assert detect_extension("application/octet-stream") == ".bin"

    def test_valid_markdown_empty(self):
        from src.scraper.base.converter import valid_markdown

        valid, reason = valid_markdown(None)
        assert not valid
        assert "None or empty" in reason

    def test_valid_markdown_too_short(self):
        from src.scraper.base.converter import valid_markdown

        valid, reason = valid_markdown("Short text", min_length=50)
        assert not valid
        assert "too short" in reason

    def test_merge_context_replaces_blank_year_with_context_year(self):
        from src.scraper.base.scraper import merge_context

        result = {
            "title": "RJ Doc",
            "year": "   ",
            "document_url": "http://example.com",
        }
        context = {"year": 2025, "type": "Lei", "situation": "Vigente"}

        merged = merge_context(result, context)

        assert merged["year"] == 2025

    @pytest.mark.asyncio
    async def test_is_already_scraped_skips_when_overwrite_disabled(self):
        from src.database.saver import FileSaver
        from src.scraper.base.persistence import PersistenceManager
        from src.scraper.base.scraper import BaseScraper

        with tempfile.TemporaryDirectory() as tmp:
            saver = FileSaver(save_dir=Path(tmp), flush_interval=100)
            await saver.save_document(
                {
                    "year": 2024,
                    "document_url": "http://example.com/doc1",
                    "title": "Test Doc",
                    "text_markdown": "content",
                    "type": "Lei",
                    "situation": "Vigente",
                }
            )
            await saver.flush(2024)

            scraper = BaseScraper.__new__(BaseScraper)
            scraper.saver = saver
            scraper.verbose = False
            scraper.overwrite = False
            scraper._scraped_keys = set()
            scraper._persister = PersistenceManager(scraper)

            await scraper._load_scraped_keys(2024)

            assert scraper._scraped_keys == {("http://example.com/doc1", "Test Doc")}
            assert scraper._is_already_scraped("http://example.com/doc1", "Test Doc")

    @pytest.mark.asyncio
    async def test_is_already_scraped_does_not_skip_when_overwrite_enabled(self):
        from src.database.saver import FileSaver
        from src.scraper.base.persistence import PersistenceManager
        from src.scraper.base.scraper import BaseScraper

        with tempfile.TemporaryDirectory() as tmp:
            saver = FileSaver(save_dir=Path(tmp), flush_interval=100)
            await saver.save_document(
                {
                    "year": 2024,
                    "document_url": "http://example.com/doc1",
                    "title": "Test Doc",
                    "text_markdown": "content",
                    "type": "Lei",
                    "situation": "Vigente",
                }
            )
            await saver.flush(2024)

            scraper = BaseScraper.__new__(BaseScraper)
            scraper.saver = saver
            scraper.verbose = False
            scraper.overwrite = True
            scraper._scraped_keys = set()
            scraper._persister = PersistenceManager(scraper)

            await scraper._load_scraped_keys(2024)

            # overwrite=True skips disk I/O — keys are empty
            assert scraper._scraped_keys == set()
            assert not scraper._is_already_scraped(
                "http://example.com/doc1", "Test Doc"
            )

    def test_valid_markdown_server_error(self):
        from src.scraper.base.converter import valid_markdown

        text = "The requested URL was not found on this server. " * 5
        valid, reason = valid_markdown(text)
        assert not valid
        assert "server error" in reason

    def test_valid_markdown_ok(self):
        from src.scraper.base.converter import valid_markdown

        text = "Art. 1º Esta lei dispõe sobre a organização básica do Estado. " * 3
        valid, reason = valid_markdown(text)
        assert valid
        assert reason == ""

    @pytest.mark.asyncio
    async def test_bytes_to_markdown_falls_back_to_pymupdf_for_pdf(self):
        from src.scraper.base.converter import MarkdownConverter
        from src.scraper.base.scraper import BaseScraper

        pdf = fitz.open()
        page = pdf.new_page()
        page.insert_text(
            (72, 72),
            "Art. 1º Esta lei dispõe sobre a organização administrativa do Estado. "
            * 4,
        )
        pdf_bytes = pdf.tobytes()
        pdf.close()

        scraper = BaseScraper.__new__(BaseScraper)
        scraper.ocr_service = None
        scraper._converter = MarkdownConverter(scraper)
        scraper._converter.convert_to_md = AsyncMock(
            side_effect=ValueError("markitdown returned empty content")
        )

        markdown = await scraper._bytes_to_markdown(
            pdf_bytes,
            filename="document.pdf",
            content_type="application/pdf",
        )

        assert "Art. 1º Esta lei dispõe" in markdown
        scraper._converter.convert_to_md.assert_awaited_once()

    def test_flatten_results(self):
        from src.scraper.base.scraper import flatten_results

        results = [
            {"a": 1},
            [{"b": 2}, {"c": 3}],
            None,
            {"d": 4},
        ]
        flat = flatten_results(results)
        assert len(flat) == 4
        assert flat[0] == {"a": 1}
        assert flat[1] == {"b": 2}
        assert flat[3] == {"d": 4}

    def test_merge_context(self):
        from src.scraper.base.scraper import merge_context

        result = {"title": "Doc", "text_markdown": "content"}
        context = {"year": 2024, "situation": "Vigente", "type": "Lei"}
        merged = merge_context(result, context)
        assert merged["year"] == 2024
        assert merged["situation"] == "Vigente"
        assert merged["title"] == "Doc"

    def test_merge_context_result_overrides_context(self):
        from src.scraper.base.scraper import merge_context

        result = {"year": 2025, "title": "New"}
        context = {"year": 2024, "situation": "Vigente"}
        merged = merge_context(result, context)
        assert merged["year"] == 2025  # result wins

    def test_merge_context_ignores_generic_type_placeholders(self):
        from src.scraper.base.scraper import merge_context

        result = {"title": "Doc", "type": ""}
        context = {"year": 2024, "type": "all", "situation": "NA"}

        merged = merge_context(result, context)

        assert "type" not in merged
        assert "situation" not in merged

    def test_merge_context_uses_meaningful_context_type_when_result_blank(self):
        from src.scraper.base.scraper import merge_context

        result = {"title": "Doc", "type": "   "}
        context = {"year": 2024, "type": "Lei Complementar"}

        merged = merge_context(result, context)

        assert merged["type"] == "Lei Complementar"

    def test_wrap_html(self):
        from src.scraper.base.converter import wrap_html

        assert wrap_html("<p>Hello</p>") == "<html><body><p>Hello</p></body></html>"

    def test_clean_markdown_strips_links(self):
        from src.scraper.base.converter import clean_markdown

        result = clean_markdown("[link text](http://example.com)")
        assert result == "link text"
        assert "http" not in result

    def test_clean_markdown_custom_replacements(self):
        from src.scraper.base.converter import clean_markdown

        result = clean_markdown("HEADER text content", replace=[("HEADER", "")])
        assert "HEADER" not in result
        assert "text content" in result

    @pytest.mark.asyncio
    async def test_runtime_logs_are_isolated_per_scraper(self, monkeypatch):
        import src.scraper.base.scraper as scraper_module

        from loguru import logger
        from src.scraper.base.scraper import BaseScraper

        class DummyLoggingScraper(BaseScraper):
            def __init__(self, name: str, docs_save_dir: Path):
                super().__init__(
                    base_url="http://example.com",
                    name=name,
                    types={"Lei": 1},
                    situations={},
                    year_start=2024,
                    year_end=2024,
                    docs_save_dir=docs_save_dir,
                    verbose=False,
                )

            async def _scrape_type(
                self, norm_type: str, norm_type_id, year: int
            ) -> list[dict]:
                logger.debug(f"debug from {self.name}")
                return []

        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "logs"
            monkeypatch.setattr(scraper_module, "LOG_DIR", log_dir)

            scraper_a = DummyLoggingScraper("SCRAPER_A", Path(tmp) / "docs")
            scraper_b = DummyLoggingScraper("SCRAPER_B", Path(tmp) / "docs")

            try:
                await asyncio.gather(scraper_a.scrape(), scraper_b.scrape())
            finally:
                await asyncio.gather(scraper_a.cleanup(), scraper_b.cleanup())

            assert scraper_a.saver is not None
            assert scraper_b.saver is not None
            assert scraper_a.saver.log_dir == log_dir / "SCRAPER_A"
            assert scraper_b.saver.log_dir == log_dir / "SCRAPER_B"

            log_a = (log_dir / "SCRAPER_A" / "runtime.log").read_text(encoding="utf-8")
            log_b = (log_dir / "SCRAPER_B" / "runtime.log").read_text(encoding="utf-8")

            assert "debug from SCRAPER_A" in log_a
            assert "debug from SCRAPER_B" not in log_a
            assert "debug from SCRAPER_B" in log_b
            assert "debug from SCRAPER_A" not in log_b

    @pytest.mark.asyncio
    async def test_runtime_log_is_overwritten_each_run(self, monkeypatch):
        import src.scraper.base.scraper as scraper_module

        from loguru import logger
        from src.scraper.base.scraper import BaseScraper

        class DummyLoggingScraper(BaseScraper):
            def __init__(self, name: str, docs_save_dir: Path, marker: str):
                self.marker = marker
                super().__init__(
                    base_url="http://example.com",
                    name=name,
                    types={"Lei": 1},
                    situations={},
                    year_start=2024,
                    year_end=2024,
                    docs_save_dir=docs_save_dir,
                    verbose=False,
                )

            async def _scrape_type(
                self, norm_type: str, norm_type_id, year: int
            ) -> list[dict]:
                logger.debug(self.marker)
                return []

        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "logs"
            monkeypatch.setattr(scraper_module, "LOG_DIR", log_dir)
            docs_dir = Path(tmp) / "docs"

            first_run = DummyLoggingScraper("SCRAPER_OVERWRITE", docs_dir, "first run")
            try:
                await first_run.scrape()
            finally:
                await first_run.cleanup()

            second_run = DummyLoggingScraper(
                "SCRAPER_OVERWRITE", docs_dir, "second run"
            )
            try:
                await second_run.scrape()
            finally:
                await second_run.cleanup()

            runtime_log = (log_dir / "SCRAPER_OVERWRITE" / "runtime.log").read_text(
                encoding="utf-8"
            )

            assert "second run" in runtime_log
            assert "first run" not in runtime_log

    @pytest.mark.asyncio
    async def test_save_doc_error_uses_scraper_log_dir(self, monkeypatch):
        import src.scraper.base.scraper as scraper_module

        from src.scraper.base.scraper import BaseScraper

        class DummyLoggingScraper(BaseScraper):
            def __init__(self, name: str, docs_save_dir: Path):
                super().__init__(
                    base_url="http://example.com",
                    name=name,
                    types={"Lei": 1},
                    situations={},
                    year_start=2024,
                    year_end=2024,
                    docs_save_dir=docs_save_dir,
                    verbose=False,
                )

            async def _scrape_type(
                self, norm_type: str, norm_type_id, year: int
            ) -> list[dict]:
                return []

        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "logs"
            monkeypatch.setattr(scraper_module, "LOG_DIR", log_dir)

            scraper = DummyLoggingScraper("SCRAPER_ERRORS", Path(tmp) / "docs")

            try:
                await scraper._save_doc_error(
                    title="Bad Doc",
                    year=2024,
                    situation="Vigente",
                    norm_type="Lei",
                    html_link="http://example.com/bad",
                    error_message="Parse failed",
                )
            finally:
                await scraper.cleanup()

            error_files = list((log_dir / "SCRAPER_ERRORS").rglob("*.json"))
            assert len(error_files) == 1

            error_data = json.loads(error_files[0].read_text(encoding="utf-8"))
            assert error_data["title"] == "Bad Doc"
            assert error_data["error_message"] == "Parse failed"


# =========================================================================
# ScrapedDocument pydantic validation
# =========================================================================


_VALID_DOC_KWARGS = dict(
    year=2024,
    title="Lei nº 1/2024",
    type="Lei Ordinária",
    situation="Não consta revogação expressa",
    summary="",
    text_markdown="# Art. 1º Fica criado o município.",
    document_url="https://example.com/lei/1",
)


class TestScrapedDocumentValidation:
    """Tests for ScrapedDocument field validators."""

    def test_valid_document_passes(self):
        from src.scraper.base.schemas import ScrapedDocument

        doc = ScrapedDocument(**_VALID_DOC_KWARGS)
        assert doc.year == 2024
        assert doc.title == "Lei nº 1/2024"

    def test_year_coerced_from_string(self):
        from src.scraper.base.schemas import ScrapedDocument

        doc = ScrapedDocument(**{**_VALID_DOC_KWARGS, "year": "2022"})
        assert doc.year == 2022
        assert isinstance(doc.year, int)

    def test_summary_may_be_empty_string(self):
        from src.scraper.base.schemas import ScrapedDocument

        doc = ScrapedDocument(**{**_VALID_DOC_KWARGS, "summary": ""})
        assert doc.summary == ""

    def test_extra_fields_allowed(self):
        from src.scraper.base.schemas import ScrapedDocument

        doc = ScrapedDocument(**_VALID_DOC_KWARGS, norm_number="42", date="2024-01-01")
        assert doc.model_extra["norm_number"] == "42"

    @pytest.mark.parametrize(
        "field",
        ["title", "type", "situation", "text_markdown", "document_url"],
    )
    def test_empty_required_string_raises(self, field):
        from pydantic import ValidationError

        from src.scraper.base.schemas import ScrapedDocument

        kwargs = {**_VALID_DOC_KWARGS, field: ""}
        with pytest.raises(ValidationError, match=field):
            ScrapedDocument(**kwargs)

    @pytest.mark.parametrize(
        "field",
        ["title", "type", "situation", "text_markdown", "document_url"],
    )
    def test_whitespace_only_required_string_raises(self, field):
        from pydantic import ValidationError

        from src.scraper.base.schemas import ScrapedDocument

        kwargs = {**_VALID_DOC_KWARGS, field: "   "}
        with pytest.raises(ValidationError, match=field):
            ScrapedDocument(**kwargs)

    @pytest.mark.parametrize(
        "field",
        ["title", "type", "situation", "text_markdown", "document_url"],
    )
    def test_leading_trailing_whitespace_is_stripped(self, field):
        from src.scraper.base.schemas import ScrapedDocument

        kwargs = {**_VALID_DOC_KWARGS, field: "  value  "}
        doc = ScrapedDocument(**kwargs)
        assert getattr(doc, field) == "value"


# =========================================================================
# SavedDocument pydantic validation
# =========================================================================


class TestSavedDocumentValidation:
    """Tests for SavedDocument (extends ScrapedDocument with file_path)."""

    def test_valid_saved_document_with_file_path_passes(self):
        from src.scraper.base.schemas import SavedDocument

        doc = SavedDocument(
            **_VALID_DOC_KWARGS, file_path="MYSTATE/2024/docs/lei_1.pdf"
        )
        assert doc.file_path == "MYSTATE/2024/docs/lei_1.pdf"

    def test_saved_document_without_file_path_is_allowed(self):
        """file_path is optional — not all saves produce a raw file on disk."""
        from src.scraper.base.schemas import SavedDocument

        doc = SavedDocument(**_VALID_DOC_KWARGS)
        assert doc.file_path is None

    def test_empty_file_path_raises(self):
        from pydantic import ValidationError

        from src.scraper.base.schemas import SavedDocument

        with pytest.raises(ValidationError):
            SavedDocument(**_VALID_DOC_KWARGS, file_path="")


# =========================================================================
# FileSaver document validation
# =========================================================================


class TestFileSaverValidation:
    """Tests for FileSaver._validate_data and save_document validation."""

    @pytest.mark.asyncio
    async def test_save_document_returns_none_for_missing_required_fields(self):
        from src.database.saver import FileSaver

        with tempfile.TemporaryDirectory() as tmp:
            saver = FileSaver(save_dir=Path(tmp))
            # Missing 'type' and 'situation'
            result = await saver.save_document(
                {
                    "year": 2024,
                    "document_url": "https://example.com/doc",
                    "title": "Test Doc",
                    "text_markdown": "content",
                }
            )
            assert result is None

    @pytest.mark.asyncio
    async def test_save_document_returns_none_for_empty_required_field(self):
        from src.database.saver import FileSaver

        with tempfile.TemporaryDirectory() as tmp:
            saver = FileSaver(save_dir=Path(tmp))
            result = await saver.save_document(
                {
                    "year": 2024,
                    "document_url": "https://example.com/doc",
                    "title": "Test Doc",
                    "text_markdown": "content",
                    "type": "",  # blank — must be rejected
                    "situation": "Vigente",
                }
            )
            assert result is None

    @pytest.mark.asyncio
    async def test_save_document_succeeds_with_all_valid_fields(self):
        from src.database.saver import FileSaver

        with tempfile.TemporaryDirectory() as tmp:
            saver = FileSaver(save_dir=Path(tmp))
            result = await saver.save_document(
                {
                    "year": 2024,
                    "document_url": "https://example.com/doc/valid",
                    "title": "Valid Doc",
                    "text_markdown": "# Art. 1º Content here.",
                    "type": "Lei Ordinária",
                    "situation": "Não consta revogação expressa",
                    "summary": "",
                },
                raw_content=b"fake-pdf-bytes",
                content_extension=".pdf",
            )
            assert result is not None
            assert result["title"] == "Valid Doc"
            assert result["year"] == 2024
            assert "file_path" in result

    @pytest.mark.asyncio
    async def test_validate_data_rejects_blank_type(self):
        from src.database.saver import FileSaver

        saver = FileSaver(save_dir=Path("/tmp/irrelevant"))
        data = {
            "year": 2024,
            "document_url": "https://example.com/doc",
            "title": "Doc",
            "text_markdown": "content",
            "type": "  ",  # whitespace only
            "situation": "Vigente",
        }
        assert saver._validate_data(data) is False

    def test_validate_data_passes_for_complete_data(self):
        from src.database.saver import FileSaver

        saver = FileSaver(save_dir=Path("/tmp/irrelevant"))
        data = {
            "year": 2024,
            "document_url": "https://example.com/doc",
            "title": "Doc",
            "text_markdown": "content",
            "type": "Lei",
            "situation": "Vigente",
        }
        assert saver._validate_data(data) is True
