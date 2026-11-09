"""Tests for core infrastructure modules.

Covers:
- clean_md_tag utility
- RateLimiter concurrency primitive
- BaseScraper markdown utilities and helpers
- RequestService (FailedRequest, detect_content_info)
"""

import asyncio

import pytest

from src.core.http import RateLimiter
from src.core.ocr import clean_md_tag


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


class TestRateLimiter:
    @pytest.mark.asyncio
    async def test_acquire_does_not_block_below_limit(self):
        limiter = RateLimiter(1000)
        start = asyncio.get_event_loop().time()
        for _ in range(5):
            await limiter.acquire()
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_acquire_rate_limits(self):
        limiter = RateLimiter(2)
        start = asyncio.get_event_loop().time()
        for _ in range(3):
            await limiter.acquire()
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed >= 0.4

    @pytest.mark.asyncio
    async def test_fractional_rps(self):
        limiter = RateLimiter(0.5)
        start = asyncio.get_event_loop().time()
        await limiter.acquire()
        await limiter.acquire()
        elapsed = asyncio.get_event_loop().time() - start
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
        expected_gap = 1.0 / 5
        for gap in gaps:
            assert gap >= expected_gap - 0.1, (
                f"Gap {gap:.3f}s too short (expected ~{expected_gap:.3f}s)"
            )

    @pytest.mark.asyncio
    async def test_concurrent_acquires_are_serialized(self):
        """Multiple concurrent acquire() calls should still be rate-limited."""
        limiter = RateLimiter(5)
        timestamps = []

        async def worker():
            await limiter.acquire()
            timestamps.append(asyncio.get_event_loop().time())

        await asyncio.gather(*[worker() for _ in range(4)])
        total_elapsed = timestamps[-1] - timestamps[0]
        assert total_elapsed >= 0.4, (
            f"Elapsed {total_elapsed:.3f}s — concurrent requests were not serialized"
        )

    @pytest.mark.asyncio
    async def test_independent_limiters_do_not_interfere(self):
        """Two separate RateLimiter instances should not affect each other."""
        RateLimiter(1)
        fast = RateLimiter(100)
        start = asyncio.get_event_loop().time()
        for _ in range(5):
            await fast.acquire()
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_very_low_fractional_rps(self):
        """Verify rps=0.33 (~1 request every 3 seconds) works."""
        limiter = RateLimiter(0.33)
        start = asyncio.get_event_loop().time()
        await limiter.acquire()
        await limiter.acquire()
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed >= 2.5


class TestBaseScraperHelpers:
    """Test BaseScraper static/class methods and utilities without full init."""

    def test_detect_extension_pdf(self):
        from src.core.convert import detect_extension

        assert detect_extension("application/pdf") == ".pdf"

    def test_detect_extension_html(self):
        from src.core.convert import detect_extension

        assert detect_extension("text/html") == ".html"

    def test_detect_extension_from_filename(self):
        from src.core.convert import detect_extension

        assert detect_extension("", "document.docx") == ".docx"

    def test_detect_extension_unknown(self):
        from src.core.convert import detect_extension

        assert detect_extension("application/octet-stream") == ".bin"

    def test_valid_markdown_empty(self):
        from src.core.convert import valid_markdown

        valid, reason = valid_markdown(None)
        assert not valid
        assert "None or empty" in reason

    def test_valid_markdown_too_short(self):
        from src.core.convert import valid_markdown

        valid, reason = valid_markdown("Short text", min_length=50)
        assert not valid
        assert "too short" in reason

    def test_valid_markdown_server_error(self):
        from src.core.convert import valid_markdown

        text = "The requested URL was not found on this server. " * 5
        valid, reason = valid_markdown(text)
        assert not valid
        assert "server error" in reason

    def test_valid_markdown_dotnet_error(self):
        from src.core.convert import valid_markdown

        text = "Object reference not set to an instance of an object\nNossa equipe resolverá o problema, você pode tentar mais tarde."
        valid, reason = valid_markdown(text)
        assert not valid
        assert "server error" in reason

    def test_valid_markdown_ok(self):
        from src.core.convert import valid_markdown

        text = "Art. 1º Esta lei dispõe sobre a organização básica do Estado. " * 3
        valid, reason = valid_markdown(text)
        assert valid
        assert reason == ""

    def test_wrap_html(self):
        from src.core.convert import wrap_html

        assert wrap_html("<p>Hello</p>") == "<html><body><p>Hello</p></body></html>"

    def test_clean_markdown_strips_links(self):
        from src.core.convert import clean_markdown

        result = clean_markdown("[link text](http://example.com)")
        assert result == "link text"
        assert "http" not in result

    def test_clean_markdown_custom_replacements(self):
        from src.core.convert import clean_markdown

        result = clean_markdown("HEADER text content", replace=[("HEADER", "")])
        assert "HEADER" not in result
        assert "text content" in result


_VALID_DOC_KWARGS = dict(
    year=2024,
    title="Lei nº 1/2024",
    type="Lei Ordinária",
    situation="Não consta revogação expressa",
    summary="",
    text_markdown="# Art. 1º Fica criado o município.",
    document_url="https://example.com/lei/1",
)
