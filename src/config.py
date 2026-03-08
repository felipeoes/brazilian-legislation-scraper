"""Centralized configuration from environment variables."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

SAVE_DIR = Path(os.environ.get("SAVE_DIR", "outputs/legislation"))

_raw_state_dir = os.environ.get("STATE_LEGISLATION_SAVE_DIR")
STATE_LEGISLATION_SAVE_DIR: Path | None = (
    Path(_raw_state_dir) if _raw_state_dir else None
)

_raw_specific_dir = os.environ.get("SPECIFIC_LEGISLATION_SAVE_DIR")
SPECIFIC_LEGISLATION_SAVE_DIR: Path | None = (
    Path(_raw_specific_dir) if _raw_specific_dir else None
)

ERROR_LOG_DIR = Path(os.environ.get("ERROR_LOG_DIR", "logs/legislation"))

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
