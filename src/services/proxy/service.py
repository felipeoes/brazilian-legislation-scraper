import json
import random
import time
import aiohttp
import aiofiles
from typing import Optional
from loguru import logger


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

    async def _load_proxies(self):
        """Internal method to load proxies from the configured source."""
        current_time = time.time()
        ttl = self.config.get("ttl", 60)  # default 1 minute

        if self._loaded and (current_time - self._last_loaded) < ttl:
            return

        if self.verbose:
            logger.info("Reloading proxies...")

        file_path = self.config.get("file_path")
        endpoint = self.config.get("endpoint")

        if file_path:
            try:
                async with aiofiles.open(file_path, "r") as f:
                    content = await f.read()
                    self.proxies = [
                        p.strip() for p in content.splitlines() if p.strip()
                    ]
            except Exception as e:
                logger.error(f"Failed to load proxies from {file_path}: {e}")

        elif endpoint:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(endpoint) as response:
                        if response.status == 200:
                            content = await response.text()
                            try:
                                data = json.loads(content)
                                if isinstance(data, dict) and "proxies" in data:
                                    self.proxies = [
                                        p.strip() for p in data["proxies"] if p.strip()
                                    ]
                                else:
                                    self.proxies = [
                                        p.strip()
                                        for p in content.splitlines()
                                        if p.strip()
                                    ]
                            except ValueError:
                                self.proxies = [
                                    p.strip() for p in content.splitlines() if p.strip()
                                ]
                        else:
                            logger.error(
                                f"Failed to load proxies from endpoint {endpoint}: HTTP {response.status}"
                            )
            except Exception as e:
                logger.error(f"Failed to load proxies from {endpoint}: {e}")

        else:
            logger.warning("No 'file_path' or 'endpoint' provided in proxy_config.")

        if self.proxies:
            logger.info(
                f"Loaded {len(self.proxies)} proxies from {file_path or endpoint}."
            )
        else:
            logger.warning("No proxies loaded.")

        self._loaded = True
        self._last_loaded = current_time

    async def get_proxy(self) -> Optional[str]:
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
