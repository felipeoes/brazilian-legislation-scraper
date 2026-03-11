"""Integration tests for SnowflakeClient and LLMOCRService with Snowflake provider.

These tests make real network calls to Snowflake and require the following
environment variables to be set:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, LLM_API_KEY (as token),
    SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_STAGE, LLM_MODEL

Run with:
    uv run pytest tests/test_snowflake_client.py -m integration -v
"""

import os
from pathlib import Path

import pytest

TEST_DATA_DIR = Path(__file__).parent.parent / "scripts" / "debug" / "test_data"
PDF_PATH = TEST_DATA_DIR / "output_test.pdf"
IMAGE_PATH = TEST_DATA_DIR / "decreto_test.png"

# Output files written by integration tests for manual inspection
IMAGE_OUTPUT_PATH = TEST_DATA_DIR / "image_output.md"
PDF_OUTPUT_PATH = TEST_DATA_DIR / "pdf_output.md"

# Portuguese legal keywords expected in OCR output of Brazilian legislation
_LEGAL_KEYWORDS = [
    "art",  # "Art." appears in virtually every Brazilian legal document
    "decreto",
    "lei",
    "norma",
    "estado",
    "federal",
    "municipal",
    "parágrafo",
    "§",
    "inciso",
    "alínea",
    "seção",
    "capítulo",
]


def _has_legal_content(text: str) -> bool:
    """Return True if the text contains at least one Portuguese legal keyword."""
    lower = text.lower()
    return any(kw in lower for kw in _LEGAL_KEYWORDS)


def _has_markdown_structure(text: str) -> bool:
    """Return True if the text has at least one Markdown heading or list marker."""
    for line in text.splitlines():
        stripped = line.lstrip()
        if (
            stripped.startswith("#")
            or stripped.startswith("- ")
            or stripped.startswith("* ")
        ):
            return True
    return False


@pytest.fixture
async def snowflake_client():
    from src.services.ocr.clients import SnowflakeClient

    client = SnowflakeClient(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        token=os.environ["LLM_API_KEY"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
        stage=os.environ["SNOWFLAKE_STAGE"],
    )
    yield client
    await client.close()


@pytest.fixture
def llm_service(snowflake_client):
    from src.services.ocr.config import LLMConfig
    from src.services.ocr.llm import LLMOCRService
    from src.scraper.base.scraper import DEFAULT_LLM_PROMPT

    model = os.environ["LLM_MODEL"]
    # Snowflake AI_COMPLETE cap is 600 RPM = 10 RPS
    config = LLMConfig(client=snowflake_client, model=model, rps=10)
    return LLMOCRService(prompt=DEFAULT_LLM_PROMPT, llm_config=config)


@pytest.mark.integration
@pytest.mark.skip(
    reason="Use mock versions in test_snowflake_client_mock.py for faster execution"
)
class TestSnowflakeClientIntegration:
    @pytest.mark.asyncio
    async def test_image_to_markdown(self, llm_service):
        """Image input returns non-empty, structurally valid markdown with legal content."""
        img_bytes = IMAGE_PATH.read_bytes()
        result = await llm_service.images_to_markdown([img_bytes])

        assert isinstance(result, str)
        assert result.strip(), "Expected non-empty markdown from image"
        assert _has_markdown_structure(result), (
            "Expected at least one Markdown heading or list marker in image output"
        )
        assert _has_legal_content(result), (
            "Expected Portuguese legal keywords in image OCR output"
        )

        IMAGE_OUTPUT_PATH.write_text(result, encoding="utf-8")

    @pytest.mark.asyncio
    async def test_pdf_to_markdown(self, llm_service):
        """PDF input is rendered to images, sent to Snowflake, and returns valid markdown."""
        pdf_bytes = PDF_PATH.read_bytes()
        result = await llm_service.pdf_to_markdown(pdf_bytes)

        assert isinstance(result, str)
        assert result.strip(), "Expected non-empty markdown from PDF"
        assert _has_markdown_structure(result), (
            "Expected at least one Markdown heading or list marker in PDF output"
        )
        assert _has_legal_content(result), (
            "Expected Portuguese legal keywords in PDF OCR output"
        )

        PDF_OUTPUT_PATH.write_text(result, encoding="utf-8")

    @pytest.mark.asyncio
    async def test_usage_tracking_after_image(self, llm_service):
        """After one image request, usage stats show 1 request/success and zero tokens."""
        img_bytes = IMAGE_PATH.read_bytes()
        await llm_service.images_to_markdown([img_bytes])

        stats = llm_service.usage_stats
        assert stats, "Expected at least one model entry in usage_stats"

        model = list(stats.keys())[0]
        bucket = stats[model]
        assert bucket["requests"] == 1
        assert bucket["successful_requests"] == 1
        assert bucket["failed_requests"] == 0
        # Snowflake AI_COMPLETE does not return token counts
        assert bucket["input_tokens"] == 0
        assert bucket["output_tokens"] == 0
        assert bucket["cached_tokens"] == 0
        assert bucket["reasoning_tokens"] == 0

    @pytest.mark.asyncio
    async def test_usage_tracking_after_pdf(self, llm_service):
        """After a PDF request (one or more page batches), usage stats accumulate correctly."""
        pdf_bytes = PDF_PATH.read_bytes()
        await llm_service.pdf_to_markdown(pdf_bytes)

        stats = llm_service.usage_stats
        assert stats, "Expected at least one model entry in usage_stats"

        model = list(stats.keys())[0]
        bucket = stats[model]
        assert bucket["requests"] >= 1
        assert bucket["successful_requests"] >= 1
        assert bucket["failed_requests"] == 0
        # Snowflake AI_COMPLETE does not return token counts
        assert bucket["input_tokens"] == 0
        assert bucket["output_tokens"] == 0
        assert bucket["cached_tokens"] == 0
        assert bucket["reasoning_tokens"] == 0
