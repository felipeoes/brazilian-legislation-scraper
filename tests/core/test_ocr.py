import asyncio

import fitz
import pytest
from pydantic_ai.messages import ModelResponse, ToolCallPart
from pydantic_ai.models.function import FunctionModel

from src.core.ocr import OCR, OCRError, OCROutput


def pdf_bytes():
    with fitz.open() as doc:
        for _ in range(3):
            doc.new_page()
        return doc.tobytes()


@pytest.mark.parametrize("pdf_input", [False, True])
@pytest.mark.parametrize("failed_batch", [0, 1, 2])
async def test_any_failed_batch_rejects_entire_pdf(pdf_input, failed_batch):
    ocr = OCR("test", batch_size=1, concurrency=1, pdf_input=pdf_input)
    calls = 0

    async def respond(messages, info):
        nonlocal calls
        current = calls
        calls += 1
        if current == failed_batch:
            raise ValueError("batch failed")
        return ModelResponse(
            parts=[ToolCallPart(info.output_tools[0].name, {"text_markdown": "page"})]
        )

    with ocr.agent.override(model=FunctionModel(respond)):
        with pytest.raises(OCRError):
            await ocr.pdf(pdf_bytes())


async def test_shared_concurrency_and_usage():
    ocr = OCR("test", batch_size=1, concurrency=2)
    active = peak = 0

    async def respond(messages, info):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return ModelResponse(
            parts=[ToolCallPart(info.output_tools[0].name, {"text_markdown": "page"})]
        )

    with ocr.agent.override(model=FunctionModel(respond)):
        texts = await asyncio.gather(*(ocr.pdf(pdf_bytes()) for _ in range(3)))
    assert texts == ["page\n\npage\n\npage"] * 3
    assert peak == 2
    assert sum(usage.requests for usage in ocr.usage.values()) == 9


async def test_successful_blank_batch_is_not_a_failure():
    ocr = OCR("test", batch_size=1, concurrency=1)
    calls = 0

    async def respond(messages, info):
        nonlocal calls
        calls += 1
        return ModelResponse(
            parts=[
                ToolCallPart(
                    info.output_tools[0].name,
                    {"text_markdown": "" if calls == 2 else "page"},
                )
            ]
        )

    with ocr.agent.override(model=FunctionModel(respond)):
        assert await ocr.pdf(pdf_bytes()) == "page\n\n\n\npage"
    assert calls == 3


async def test_live_usage_cost_can_be_saved_to_sqlite() -> None:
    import json
    from decimal import Decimal

    from pydantic_ai.usage import RunUsage

    from src.core.store import Store

    ocr = OCR("test")
    ocr._scoped_usage["live"]["claude-haiku-5-5"] = RunUsage(
        input_tokens=42, output_tokens=10, requests=1, cost=Decimal("0.000123")
    )
    async with Store(":memory:") as store:
        run_id = await store.start_run("OCR_TEST")
        await store.finish_run(run_id, 1, 0, ocr.usage_for("live"))
        saved = json.loads(
            store.connection.execute("SELECT llm_usage FROM runs").fetchone()[0]
        )
    assert saved["claude-haiku-5-5"]["input_tokens"] == 42
    assert saved["claude-haiku-5-5"]["cost"] == "0.000123"


@pytest.mark.parametrize(
    "payload", [{}, {"text_markdown": None}, {"text_markdown": 42}]
)
def test_ocr_output_requires_a_string(payload: dict) -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        OCROutput.model_validate(payload)
