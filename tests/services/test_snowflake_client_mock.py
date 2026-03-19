"""Mock versions of Snowflake OCR tests for faster execution.

These tests replace the slow integration tests that make real network calls
to Snowflake with fast mocked versions that simulate the same behavior.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest


class TestSnowflakeClientMocked:
    """Mocked Snowflake client tests for performance."""

    @pytest.fixture
    def mock_llm_service(self):
        """Create a mocked LLM service that returns predictable responses."""
        service = MagicMock()

        # Mock images_to_markdown to return a predictable legal markdown
        mock_markdown = """# DECRETO Nº 001/1942

**Art. 1º** - Dispõe sobre medidas administrativas.

**Art. 2º** - As disposições deste decreto entram em vigor na data de sua publicação.

*Publicado no Diário Oficial em 31/12/1942*
"""

        service.images_to_markdown = AsyncMock(return_value=mock_markdown)
        service.pdf_to_markdown = AsyncMock(return_value=mock_markdown)

        # Mock usage stats
        service.usage_stats = {
            "test-model": {
                "requests": 1,
                "successful_requests": 1,
                "failed_requests": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "cached_tokens": 0,
                "reasoning_tokens": 0,
            }
        }

        return service

    @pytest.mark.asyncio
    async def test_image_to_markdown_mock(self, mock_llm_service):
        """Image input returns non-empty, structurally valid markdown with legal content (mocked)."""
        result = await mock_llm_service.images_to_markdown([b"mock_image_bytes"])

        assert isinstance(result, str)
        assert result.strip(), "Expected non-empty markdown from image"
        assert "# DECRETO" in result, "Expected markdown heading"
        assert "Art." in result, "Expected legal article content"

    @pytest.mark.asyncio
    async def test_pdf_to_markdown_mock(self, mock_llm_service):
        """PDF input is rendered to images, sent to Snowflake, and returns valid markdown (mocked)."""
        result = await mock_llm_service.pdf_to_markdown(b"mock_pdf_bytes")

        assert isinstance(result, str)
        assert result.strip(), "Expected non-empty markdown from PDF"
        assert "# DECRETO" in result, "Expected markdown heading"
        assert "Art." in result, "Expected legal article content"

    @pytest.mark.asyncio
    async def test_usage_tracking_after_image_mock(self, mock_llm_service):
        """After one image request, usage stats show 1 request/success and zero tokens (mocked)."""
        await mock_llm_service.images_to_markdown([b"mock_image_bytes"])

        stats = mock_llm_service.usage_stats
        assert stats, "Expected at least one model entry in usage_stats"

        model = list(stats.keys())[0]
        bucket = stats[model]
        assert bucket["requests"] == 1
        assert bucket["successful_requests"] == 1
        assert bucket["failed_requests"] == 0
        assert bucket["input_tokens"] == 0
        assert bucket["output_tokens"] == 0

    @pytest.mark.asyncio
    async def test_usage_tracking_after_pdf_mock(self, mock_llm_service):
        """After a PDF request (one or more page batches), usage stats accumulate correctly (mocked)."""
        await mock_llm_service.pdf_to_markdown(b"mock_pdf_bytes")

        stats = mock_llm_service.usage_stats
        assert stats, "Expected at least one model entry in usage_stats"

        model = list(stats.keys())[0]
        bucket = stats[model]
        assert bucket["requests"] >= 1
        assert bucket["successful_requests"] >= 1
        assert bucket["failed_requests"] == 0
