import asyncio
import json
import random
import time

import aiofiles
import aiohttp
from loguru import logger


def _parse_proxy_list(content: str) -> list[str]:
    """Parse proxy list from text content (JSON with 'proxies' key or newline-separated)."""
    try:
        data = json.loads(content)
        if isinstance(data, dict) and "proxies" in data:
            return [p.strip() for p in data["proxies"] if p.strip()]
    except (ValueError, TypeError):
        pass
    return [p.strip() for p in content.splitlines() if p.strip()]


class ProxyService:
    """Service to load and provide proxies from a file or an endpoint."""

    def __init__(self, config: dict, verbose: bool = False):
        """
        Initialize the ProxyService.

        Args:
            config: A dictionary containing either 'file_path' or 'endpoint'.
            verbose: If True, prints extra loading info.
        """
        self.config = config
        self.verbose = verbose
        self.proxies: list[str] = []
        self._loaded = False
        self._last_loaded = 0.0
        self._load_lock = asyncio.Lock()
        self._session: aiohttp.ClientSession | None = None

    async def _load_proxies(self):
        """Internal method to load proxies from the configured source(s).

        When both 'file_path' and 'endpoint' are configured, proxies from both
        sources are combined and deduplicated.
        """
        current_time = time.time()
        ttl = self.config.get("ttl", 60)  # default 1 minute

        if self._loaded and (current_time - self._last_loaded) < ttl:
            return

        async with self._load_lock:
            # Re-check after acquiring lock (another coroutine may have loaded)
            current_time = time.time()
            if self._loaded and (current_time - self._last_loaded) < ttl:
                return

            logger.debug("Reloading proxies...")

            file_path = self.config.get("file_path")
            endpoint = self.config.get("endpoint")

            all_proxies: list[str] = []
            sources: list[str] = []

            # --- Load from file ---
            if file_path:
                try:
                    async with aiofiles.open(file_path, "r") as f:
                        content = await f.read()
                        file_proxies = [
                            p.strip() for p in content.splitlines() if p.strip()
                        ]
                        all_proxies.extend(file_proxies)
                        sources.append(f"file ({len(file_proxies)})")
                except Exception as e:
                    logger.error(f"Failed to load proxies from {file_path}: {e}")

            # --- Load from endpoint ---
            if endpoint:
                try:
                    if self._session is None or self._session.closed:
                        self._session = aiohttp.ClientSession()
                    async with self._session.get(endpoint) as response:
                        if response.status == 200:
                            content = await response.text()
                            endpoint_proxies = _parse_proxy_list(content)
                            all_proxies.extend(endpoint_proxies)
                            sources.append(f"endpoint ({len(endpoint_proxies)})")
                        else:
                            logger.error(
                                f"Failed to load proxies from endpoint {endpoint}: HTTP {response.status}"
                            )
                except Exception as e:
                    logger.error(f"Failed to load proxies from {endpoint}: {e}")

            if not file_path and not endpoint:
                logger.warning("No 'file_path' or 'endpoint' provided in proxy_config.")

            # Deduplicate while preserving order
            seen: set[str] = set()
            unique_proxies: list[str] = []
            for p in all_proxies:
                if p not in seen:
                    seen.add(p)
                    unique_proxies.append(p)
            self.proxies = unique_proxies

            if self.proxies:
                source_str = " + ".join(sources)
                logger.info(
                    f"Loaded {len(self.proxies)} unique proxies from {source_str}."
                )
            else:
                logger.warning("No proxies loaded.")

            self._loaded = True
            self._last_loaded = current_time

    async def get_proxy(self) -> str | None:
        """
        Get a random proxy from the loaded list.

        Returns:
            A string formatted proxy (e.g. 'http://ip:port') or None if not available.
        """
        await self._load_proxies()

        if not self.proxies:
            return None

        proxy = random.choice(self.proxies)

        # Ensure proxy has a scheme (fallback to http://)
        if "://" not in proxy:
            proxy = f"http://{proxy}"

        return proxy

    async def cleanup(self) -> None:
        """Close the reusable HTTP session used for endpoint reloads."""
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None
