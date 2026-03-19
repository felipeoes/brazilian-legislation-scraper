"""Amazon Bedrock Converse API client for LLM-based OCR.

Translates OpenAI-style messages into Bedrock Converse content blocks and
makes HTTP requests via the shared ``RequestService``.
"""

from __future__ import annotations

from urllib.parse import quote

from loguru import logger

from src.services.request.service import RequestService

from ..protocol import LLMUsage
from ..utils import parse_base64_data_uri


class BedrockClient:
    """Async client for the Amazon Bedrock Converse API.

    Uses ``RequestService`` for all HTTP traffic so that rate-limiting,
    retries, and proxy support are inherited automatically.

    Args:
        base_url: Bedrock runtime base URL, e.g.
            ``https://bedrock-runtime.us-east-2.amazonaws.com``.
        model_id: Full model ARN or inference-profile ARN (will be
            URL-encoded automatically).
        api_key: Bearer token for ``Authorization`` header.
        request_service: Shared ``RequestService`` instance.
        inference_config: Dict forwarded as ``inferenceConfig``, e.g.
            ``{"maxTokens": 32000, "stopSequences": []}``.
        additional_request_fields: Dict forwarded as
            ``additionalModelRequestFields``, e.g. ``{"top_k": 250}``.
        performance_config: Dict forwarded as ``performanceConfig``,
            e.g. ``{"latency": "standard"}``.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        request_service: RequestService,
        inference_config: dict | None = None,
        additional_request_fields: dict | None = None,
        performance_config: dict | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.request_service = request_service

        # Default inference config
        self.inference_config = inference_config or {
            "maxTokens": 65536,
            "stopSequences": [],
        }
        self.additional_request_fields = additional_request_fields or {}
        self.performance_config = performance_config or {"latency": "standard"}

    # ------------------------------------------------------------------
    # Message translation: OpenAI format → Bedrock Converse format
    # ------------------------------------------------------------------

    @staticmethod
    def _translate_content_block(block: dict) -> dict:
        """Convert a single OpenAI content block to a Bedrock content block.

        If the block is already in Bedrock format, it is returned as-is.

        OpenAI format examples:
            {"type": "text", "text": "hello"}
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,..."}}
            {"type": "image_url", "image_url": {"url": "data:application/pdf;base64,...", "detail": "high"}}

        Bedrock format examples:
            {"text": "hello"}
            {"image": {"format": "png", "source": {"bytes": "<base64>"}}}
            {"document": {"name": "document", "format": "pdf", "source": {"bytes": "<base64>"}}}
        """
        if "type" not in block and any(
            k in block
            for k in (
                "text",
                "image",
                "document",
                "toolUse",
                "toolResult",
                "guardContent",
            )
        ):
            return block

        block_type = block.get("type", "")

        if block_type == "text":
            return {"text": block["text"]}

        if block_type == "image_url":
            data_url = block["image_url"]["url"]
            fmt, b64 = parse_base64_data_uri(data_url)
            if fmt == "pdf":
                return {
                    "document": {
                        "name": "document",
                        "format": "pdf",
                        "source": {"bytes": b64},
                    }
                }
            return {
                "image": {
                    "format": fmt,
                    "source": {"bytes": b64},
                }
            }

        if block_type == "document":
            doc_data = block["document"]
            return {
                "document": {
                    "name": doc_data.get("name", "document"),
                    "format": doc_data.get("format", "pdf"),
                    "source": {"bytes": doc_data["source"]["bytes"]},
                }
            }

        # Unknown block type — pass text if available
        if "text" in block:
            return {"text": block["text"]}

        raise ValueError(f"Unsupported content block type: {block_type}")

    @classmethod
    def _translate_messages(cls, messages: list[dict]) -> list[dict]:
        """Convert a list of OpenAI-format messages to Bedrock Converse format.

        Handles both:
        - Simple messages: {"role": "user", "content": "hello"}
        - Multimodal messages: {"role": "user", "content": [{"type": "text", ...}, ...]}
        """
        bedrock_messages = []
        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")

            if isinstance(content, str):
                # Simple text message
                bedrock_messages.append({"role": role, "content": [{"text": content}]})
            elif isinstance(content, list):
                # Multimodal content blocks
                bedrock_content = [
                    cls._translate_content_block(block) for block in content
                ]
                bedrock_messages.append({"role": role, "content": bedrock_content})
            else:
                raise ValueError(f"Unsupported content type: {type(content)}")

        return bedrock_messages

    # ------------------------------------------------------------------
    # API call
    # ------------------------------------------------------------------

    async def generate(
        self, messages: list[dict], model_id: str, timeout: int | None = None
    ) -> tuple[str, LLMUsage]:
        """Send messages to the Bedrock Converse API and return the text response.

        Args:
            messages: List of OpenAI-format message dicts.
            model_id: Full model ARN or inference-profile ARN.

        Returns:
            A tuple of (text response, token usage).

        Raises:
            RuntimeError: If the API call fails or the response is malformed.
        """
        encoded_model_id = quote(model_id, safe=":")
        endpoint_url = f"{self.base_url}/model/{encoded_model_id}/converse"

        bedrock_messages = self._translate_messages(messages)

        payload = {
            "messages": bedrock_messages,
            "inferenceConfig": self.inference_config,
        }

        if self.additional_request_fields:
            payload["additionalModelRequestFields"] = self.additional_request_fields

        if self.performance_config:
            payload["performanceConfig"] = self.performance_config

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

        kwargs = {
            "url": endpoint_url,
            "method": "POST",
            "json": payload,
            "headers": headers,
        }
        if timeout is not None:
            kwargs["timeout"] = timeout

        response = await self.request_service.make_request(**kwargs)

        if not response:
            reason = response.reason
            raise RuntimeError(
                f"Bedrock Converse API returned no response for {endpoint_url}: {reason}"
            )

        if response.status != 200:
            body = await response.text()
            raise RuntimeError(f"Bedrock Converse API error {response.status}: {body}")

        data = await response.json()
        text = self._extract_text(data)
        raw = data.get("usage", {})
        usage = LLMUsage(
            input_tokens=raw.get("inputTokens", 0),
            cached_tokens=raw.get("cacheReadInputTokens", 0),
            output_tokens=raw.get("outputTokens", 0),
            reasoning_tokens=0,
        )
        return text, usage

    async def close(self) -> None:
        """Close the shared HTTP request service."""
        await self.request_service.cleanup()

    @staticmethod
    def _extract_text(data: dict) -> str:
        """Extract the text content from a Bedrock Converse response.

        Response structure:
            {
                "output": {
                    "message": {
                        "role": "assistant",
                        "content": [{"text": "..."}]
                    }
                },
                ...
            }
        """
        try:
            message = data["output"]["message"]
            content_blocks = message.get("content", [])
            text_parts = [block["text"] for block in content_blocks if "text" in block]
            return "\n".join(text_parts)
        except (KeyError, TypeError) as e:
            logger.error(f"Failed to parse Bedrock response: {e} | Data: {data}")
            raise RuntimeError(f"Malformed Bedrock Converse response: {e}") from e
