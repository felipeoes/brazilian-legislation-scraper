"""Tests for core infrastructure modules.

Covers:
- clean_md_tag utility
- RateLimiter concurrency primitive
- BaseScraper markdown utilities and helpers
- RequestService (FailedRequest, detect_content_info)
"""

import asyncio
import tempfile
import fitz
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.utils import clean_md_tag
from src.scraper.base.concurrency import RateLimiter
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
        from src.scraper.base.scraper import BaseScraper

        assert BaseScraper._detect_extension("application/pdf") == ".pdf"

    def test_detect_extension_html(self):
        from src.scraper.base.scraper import BaseScraper

        assert BaseScraper._detect_extension("text/html") == ".html"

    def test_detect_extension_from_filename(self):
        from src.scraper.base.scraper import BaseScraper

        assert BaseScraper._detect_extension("", "document.docx") == ".docx"

    def test_detect_extension_unknown(self):
        from src.scraper.base.scraper import BaseScraper

        assert BaseScraper._detect_extension("application/octet-stream") == ".bin"

    def test_valid_markdown_empty(self):
        from src.scraper.base.scraper import BaseScraper

        scraper = MagicMock(spec=BaseScraper)
        # _SERVER_ERROR_PATTERNS is now module-level; no mock attribute needed
        valid, reason = BaseScraper._valid_markdown(scraper, None)
        assert not valid
        assert "None or empty" in reason

    def test_valid_markdown_too_short(self):
        from src.scraper.base.scraper import BaseScraper

        scraper = MagicMock(spec=BaseScraper)
        # _SERVER_ERROR_PATTERNS is now module-level; no mock attribute needed
        valid, reason = BaseScraper._valid_markdown(
            scraper, "Short text", min_length=50
        )
        assert not valid
        assert "too short" in reason

    def test_merge_context_replaces_blank_year_with_context_year(self):
        from src.scraper.base.scraper import BaseScraper

        result = {
            "title": "RJ Doc",
            "year": "   ",
            "document_url": "http://example.com",
        }
        context = {"year": 2025, "type": "Lei", "situation": "Vigente"}

        merged = BaseScraper._merge_context(result, context)

        assert merged["year"] == 2025

    @pytest.mark.asyncio
    async def test_is_already_scraped_skips_when_overwrite_disabled(self):
        from src.database.saver import FileSaver
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

            await scraper._load_scraped_keys(2024)

            assert scraper._scraped_keys == {("http://example.com/doc1", "Test Doc")}
            assert scraper._is_already_scraped("http://example.com/doc1", "Test Doc")

    @pytest.mark.asyncio
    async def test_is_already_scraped_does_not_skip_when_overwrite_enabled(self):
        from src.database.saver import FileSaver
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

            await scraper._load_scraped_keys(2024)

            assert scraper._scraped_keys == {("http://example.com/doc1", "Test Doc")}
            assert not scraper._is_already_scraped(
                "http://example.com/doc1", "Test Doc"
            )

    def test_valid_markdown_server_error(self):
        from src.scraper.base.scraper import BaseScraper

        scraper = MagicMock(spec=BaseScraper)
        # _SERVER_ERROR_PATTERNS is now module-level; no mock attribute needed
        text = "The requested URL was not found on this server. " * 5
        valid, reason = BaseScraper._valid_markdown(scraper, text)
        assert not valid
        assert "server error" in reason

    def test_valid_markdown_ok(self):
        from src.scraper.base.scraper import BaseScraper

        scraper = MagicMock(spec=BaseScraper)
        # _SERVER_ERROR_PATTERNS is now module-level; no mock attribute needed
        text = "Art. 1º Esta lei dispõe sobre a organização básica do Estado. " * 3
        valid, reason = BaseScraper._valid_markdown(scraper, text)
        assert valid
        assert reason == ""

    @pytest.mark.asyncio
    async def test_bytes_to_markdown_falls_back_to_pymupdf_for_pdf(self):
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
        # _SERVER_ERROR_PATTERNS is now module-level; no mock attribute needed
        scraper._convert_to_md = AsyncMock(
            side_effect=ValueError("markitdown returned empty content")
        )

        markdown = await BaseScraper._bytes_to_markdown(
            scraper,
            pdf_bytes,
            filename="document.pdf",
            content_type="application/pdf",
        )

        assert "Art. 1º Esta lei dispõe" in markdown
        scraper._convert_to_md.assert_awaited_once()

    def test_flatten_results(self):
        from src.scraper.base.scraper import BaseScraper

        results = [
            {"a": 1},
            [{"b": 2}, {"c": 3}],
            None,
            {"d": 4},
        ]
        flat = BaseScraper._flatten_results(results)
        assert len(flat) == 4
        assert flat[0] == {"a": 1}
        assert flat[1] == {"b": 2}
        assert flat[3] == {"d": 4}

    def test_merge_context(self):
        from src.scraper.base.scraper import BaseScraper

        result = {"title": "Doc", "text_markdown": "content"}
        context = {"year": 2024, "situation": "Vigente", "type": "Lei"}
        merged = BaseScraper._merge_context(result, context)
        assert merged["year"] == 2024
        assert merged["situation"] == "Vigente"
        assert merged["title"] == "Doc"

    def test_merge_context_result_overrides_context(self):
        from src.scraper.base.scraper import BaseScraper

        result = {"year": 2025, "title": "New"}
        context = {"year": 2024, "situation": "Vigente"}
        merged = BaseScraper._merge_context(result, context)
        assert merged["year"] == 2025  # result wins

    def test_wrap_html(self):
        from src.scraper.base.scraper import BaseScraper

        assert (
            BaseScraper._wrap_html("<p>Hello</p>")
            == "<html><body><p>Hello</p></body></html>"
        )

    def test_clean_markdown_strips_links(self):
        from src.scraper.base.scraper import BaseScraper

        scraper_cls = BaseScraper.__new__(BaseScraper)
        result = scraper_cls._clean_markdown("[link text](http://example.com)")
        assert result == "link text"
        assert "http" not in result

    def test_clean_markdown_custom_replacements(self):
        from src.scraper.base.scraper import BaseScraper

        scraper_cls = BaseScraper.__new__(BaseScraper)
        result = scraper_cls._clean_markdown(
            "HEADER text content", replace=[("HEADER", "")]
        )
        assert "HEADER" not in result
        assert "text content" in result
