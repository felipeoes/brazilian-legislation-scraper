"""Centralized configuration from environment variables."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
LLM_MODEL = os.environ.get("LLM_MODEL", "")
LLM_PDF_INPUT = os.environ.get("LLM_PDF_INPUT", "false").lower() in {"1", "true", "yes"}
LLM_BATCH_SIZE = int(os.environ.get("LLM_BATCH_SIZE", "5"))
LLM_CONCURRENCY = int(os.environ.get("LLM_CONCURRENCY", "8"))
SAVE_DIR = Path(os.environ.get("SAVE_DIR", "outputs/legislation"))
_raw_state_dir = os.environ.get("STATE_LEGISLATION_SAVE_DIR")
STATE_LEGISLATION_SAVE_DIR: Path | None = (
    Path(_raw_state_dir) if _raw_state_dir else None
)
_raw_specific_dir = os.environ.get("SPECIFIC_LEGISLATION_SAVE_DIR")
SPECIFIC_LEGISLATION_SAVE_DIR: Path | None = (
    Path(_raw_specific_dir) if _raw_specific_dir else None
)
LOG_DIR = Path(
    os.environ.get("LOG_DIR", os.environ.get("ERROR_LOG_DIR", "logs/legislation"))
)
PROXY_FILE_PATH = os.environ.get("PROXY_FILE_PATH")
PROXY_ENDPOINT = os.environ.get("PROXY_ENDPOINT")


def build_proxy_config() -> dict | None:
    """Build proxy config dict from environment variables, or return None."""
    if not (PROXY_FILE_PATH or PROXY_ENDPOINT):
        return None
    config: dict = {}
    if PROXY_FILE_PATH:
        config["file_path"] = PROXY_FILE_PATH
    if PROXY_ENDPOINT:
        config["endpoint"] = PROXY_ENDPOINT
    return config
