"""OpenAI-compatible LLM client adapter.

Wraps ``AsyncOpenAI`` into the ``LLMClient`` protocol so that
``LLMOCRService`` can treat all backends uniformly.
"""

from __future__ import annotations

from openai import AsyncOpenAI


class OpenAIClient:
    """Thin adapter from ``AsyncOpenAI`` to the ``LLMClient`` protocol."""

    def __init__(self, client: AsyncOpenAI, **kwargs) -> None:
        self._client = client
        self._kwargs = kwargs

    async def generate(
        self, messages: list[dict], model_id: str, timeout: int | None = None
    ) -> str:
        kwargs = self._kwargs.copy()
        if timeout is not None:
            kwargs["timeout"] = timeout

        is_stream = kwargs.pop("stream", False)

        response = await self._client.chat.completions.create(
            model=model_id, messages=messages, stream=is_stream, **kwargs
        )

        if is_stream:
            content: list[str] = []
            async for chunk in response:
                if chunk.choices and chunk.choices[0].delta.content:
                    content.append(chunk.choices[0].delta.content)
            return "".join(content)

        return response.choices[0].message.content
