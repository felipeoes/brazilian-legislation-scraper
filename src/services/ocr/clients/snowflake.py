"""Snowflake Cortex AI client for LLM-based OCR.

Uploads images to a Snowflake stage and calls AI_COMPLETE via
``snowflake.connector``.  All blocking connector calls are wrapped
in ``asyncio.to_thread``.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import shutil
import tempfile
import uuid

import snowflake.connector
from loguru import logger

from ..protocol import LLMUsage
from ..utils import parse_base64_data_uri


class SnowflakeClient:
    """Async client for Snowflake Cortex AI (AI_COMPLETE).

    Lifecycle per ``generate()`` call:
    1. Extract PNG images from OpenAI-format messages.
    2. Save them to a temporary directory on disk.
    3. Upload to the configured Snowflake stage and execute an
       ``AI_COMPLETE`` SQL query (blocking → ``asyncio.to_thread``).
    4. Schedule fire-and-forget cleanup of local temp files + stage files.
    """

    def __init__(
        self,
        account: str,
        user: str,
        token: str,
        database: str,
        schema: str,
        stage: str,
        pool_size: int = 20,
        **_kwargs,
    ) -> None:
        self.account = account
        self.user = user
        self.token = token
        self.database = database
        self.schema = schema
        self.stage = stage
        self._pool: asyncio.Queue[snowflake.connector.SnowflakeConnection] = (
            asyncio.Queue()
        )
        self._pool_size = pool_size
        self._pool_initialized = False

    # ------------------------------------------------------------------
    # Connection pool
    # ------------------------------------------------------------------

    def _create_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Create a Snowflake connection using password auth."""
        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.token,
            database=self.database,
            schema=self.schema,
        )

    async def _ensure_pool(self) -> None:
        """Lazily populate the connection pool on first use (connections created in parallel)."""
        if self._pool_initialized:
            return
        self._pool_initialized = True
        connections = await asyncio.gather(
            *[
                asyncio.to_thread(self._create_connection)
                for _ in range(self._pool_size)
            ]
        )
        for conn in connections:
            self._pool.put_nowait(conn)

    async def _acquire_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Get a connection from the pool, reconnecting if stale."""
        await self._ensure_pool()
        conn = await self._pool.get()
        try:
            if conn.is_closed():
                conn = await asyncio.to_thread(self._create_connection)
        except Exception as e:
            logger.debug(f"Reconnecting stale Snowflake connection: {e}")
            conn = await asyncio.to_thread(self._create_connection)
        return conn

    def _release_connection(
        self, conn: snowflake.connector.SnowflakeConnection
    ) -> None:
        """Return a connection to the pool."""
        try:
            self._pool.put_nowait(conn)
        except asyncio.QueueFull:
            try:
                conn.close()
            except Exception as e:
                logger.debug(f"Error closing excess Snowflake connection: {e}")

    # ------------------------------------------------------------------
    # Public API (matches LLMClient protocol)
    # ------------------------------------------------------------------

    async def generate(
        self,
        messages: list[dict],
        model_id: str,
        timeout: int | None = None,
    ) -> tuple[str, LLMUsage]:
        """Send images to Snowflake Cortex AI and return the text response."""
        prompt, image_bytes_list = self._extract_content(messages)

        if not image_bytes_list:
            raise RuntimeError("No images found in messages for Snowflake AI call.")

        batch_id = uuid.uuid4().hex[:12]
        tmp_dir = os.path.join(tempfile.gettempdir(), "snowflake_ocr", batch_id)
        os.makedirs(tmp_dir, exist_ok=True)

        filenames: list[str] = []
        local_paths: list[str] = []
        for i, img_bytes in enumerate(image_bytes_list):
            fname = f"{batch_id}_img{i}.png"
            fpath = os.path.join(tmp_dir, fname)
            with open(fpath, "wb") as f:
                f.write(img_bytes)
            filenames.append(fname)
            local_paths.append(fpath)

        conn = await self._acquire_connection()
        try:
            response_text = await asyncio.to_thread(
                self._upload_and_query_sync,
                conn,
                local_paths,
                filenames,
                prompt,
                model_id,
            )
        finally:
            self._release_connection(conn)

        self._schedule_cleanup(filenames, tmp_dir)

        return response_text, LLMUsage()  # no token data from AI_COMPLETE

    # ------------------------------------------------------------------
    # Content extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_content(messages: list[dict]) -> tuple[str, list[bytes]]:
        """Extract the text prompt and image bytes from OpenAI-format messages."""
        prompt = ""
        images: list[bytes] = []

        for msg in messages:
            content = msg.get("content", "")
            if isinstance(content, str):
                prompt = content
            elif isinstance(content, list):
                for block in content:
                    block_type = block.get("type", "")
                    if block_type == "text":
                        prompt = block["text"]
                    elif block_type == "image_url":
                        data_url = block["image_url"]["url"]
                        _, img_b64 = parse_base64_data_uri(data_url)
                        if img_b64:
                            images.append(base64.standard_b64decode(img_b64))

        return prompt, images

    # ------------------------------------------------------------------
    # Stage + query operations (blocking — run via asyncio.to_thread)
    # ------------------------------------------------------------------

    def _upload_and_query_sync(
        self,
        conn: snowflake.connector.SnowflakeConnection,
        local_paths: list[str],
        filenames: list[str],
        prompt: str,
        model_id: str,
    ) -> str:
        """Upload images to stage and run AI_COMPLETE in one connection (blocking).

        Snowflake's ``AI_COMPLETE`` returns a VARIANT column; the Python
        connector delivers it as a **JSON-encoded string** (e.g.
        ``'"text\\\\nwith newline"'``).  We JSON-decode the raw value so that
        callers receive a plain Python string with real newline characters and
        no surrounding quotes.
        """
        cursor = conn.cursor()
        try:
            for local_path in local_paths:
                abs_path = os.path.abspath(local_path).replace("\\", "/")
                put_query = (
                    f"PUT file://{abs_path} @{self.stage} "
                    f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                )
                cursor.execute(put_query)

            sql_query = self._build_ai_query(filenames, prompt, model_id)
            cursor.execute(sql_query)
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("AI_COMPLETE returned no rows.")

            result = row[0]
            if isinstance(result, str):
                try:
                    decoded = json.loads(result)
                    if isinstance(decoded, str):
                        return decoded
                except (json.JSONDecodeError, ValueError):
                    pass
            return result
        finally:
            cursor.close()

    def _cleanup_stage_sync(
        self,
        filenames: list[str],
        tmp_dir: str,
    ) -> None:
        """Remove local temp files (blocking)."""
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception as e:
            logger.warning(f"Local temp cleanup failed: {e}")

    # ------------------------------------------------------------------
    # SQL query builder
    # ------------------------------------------------------------------

    _SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9._-]+$")

    def _build_ai_query(self, filenames: list[str], prompt: str, model_id: str) -> str:
        """Build the AI_COMPLETE SQL query with TO_FILE references."""
        if not self._SAFE_IDENTIFIER_RE.match(model_id):
            raise ValueError(f"model_id contains disallowed characters: {model_id!r}")

        to_file_refs = ", ".join(
            f"TO_FILE('@{self.stage}', '{fname}')" for fname in filenames
        )

        safe_prompt = prompt.replace("'", "''")

        placeholders = ", ".join(f"{{{i}}}" for i in range(len(filenames)))
        return (
            f"SELECT AI_COMPLETE("
            f"'{model_id}', "
            f"PROMPT('{safe_prompt} {placeholders}', {to_file_refs})"
            f") AS AI_RESPONSE"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _schedule_cleanup(
        self,
        filenames: list[str],
        tmp_dir: str,
    ) -> None:
        """Schedule a fire-and-forget cleanup task."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(
                asyncio.to_thread(self._cleanup_stage_sync, filenames, tmp_dir)
            )
        except RuntimeError:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    async def close(self) -> None:
        """Drain and close all pooled connections."""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                await asyncio.to_thread(conn.close)
            except Exception as e:
                logger.debug(f"Error closing Snowflake connection during cleanup: {e}")
        self._pool_initialized = False
