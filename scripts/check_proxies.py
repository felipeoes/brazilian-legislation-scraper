"""Write the proxies that can reach Paraná: one per exit IP, fastest first.

    uv run scripts/check_proxies.py proxies.txt proxies.parana.txt --count 20

Free proxy lists are mostly dead (~12% alive) and churn within minutes. The scraper
re-reads PROXY_FILE_PATH every 60 s, so re-running this during a run refreshes the
pool. Prints counts only, never proxy addresses.
"""

import argparse
import asyncio
import ssl
import sys
import time
from pathlib import Path

import aiohttp
from aiohttp_socks import ProxyConnector

sys.path.insert(0, str(Path(__file__).parents[1]))
from src.core.http import HttpClient  # noqa: E402  (needs the repo root on the path)
from src.core.proxy import _with_scheme  # noqa: E402

ECHO_URL = "https://api.ipify.org"
SITE_URL = "https://www.legislacao.pr.gov.br/legislacao/pesquisarAto.do?action=exibir&codAto=342000"
BLOCKED = b"Acesso temporariamente bloqueado"


async def _get(proxy: str | None, url: str, timeout: float) -> tuple[bytes, float]:
    # Always verify TLS: an untrusted proxy must not be able to alter what it relays.
    ctx = ssl.create_default_context()
    connector = (
        ProxyConnector.from_url(proxy, ssl=ctx)
        if proxy
        else aiohttp.TCPConnector(ssl=ctx)
    )
    start = time.monotonic()
    async with aiohttp.ClientSession(
        connector=connector, headers=HttpClient().headers
    ) as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            r.raise_for_status()
            return await r.read(), time.monotonic() - start


async def _exit_ip(proxy: str, sem: asyncio.Semaphore) -> tuple[str, str | None, float]:
    async with sem:
        try:
            body, seconds = await _get(proxy, ECHO_URL, 15)
            return proxy, body.decode().strip(), seconds
        except Exception:
            return proxy, None, 0.0


async def _reaches_site(proxy: str) -> bool:
    try:
        body, _ = await _get(proxy, SITE_URL, 60)
    except Exception:
        return False
    return b"Paran" in body and BLOCKED not in body


def candidates(alive: list[tuple[str, str | None, float]], own_ip: str) -> list[str]:
    """Live proxies, one per exit IP other than ours, fastest first."""
    seen = {own_ip}
    picked = []
    for proxy, ip, _ in sorted(alive, key=lambda row: row[2]):
        if ip and ip not in seen:
            seen.add(ip)
            picked.append(proxy)
    return picked


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("source", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument("--count", type=int, default=20)
    args = parser.parse_args()

    proxies = list(
        dict.fromkeys(
            _with_scheme(line.strip())
            for line in args.source.read_text().splitlines()
            if line.strip()
        )
    )
    own_ip = (await _get(None, ECHO_URL, 30))[0].decode().strip()
    sem = asyncio.Semaphore(400)
    alive = await asyncio.gather(*(_exit_ip(p, sem) for p in proxies))
    pool = candidates(alive, own_ip)
    print(f"{len(proxies)} proxies, {len(pool)} alive with distinct exit IPs")

    # One page per proxy, a few at a time, until enough reach the site.
    working: list[str] = []
    for i in range(0, len(pool), 8):
        if len(working) >= args.count:
            break
        batch = pool[i : i + 8]
        ok = await asyncio.gather(*(_reaches_site(p) for p in batch))
        working += [p for p, good in zip(batch, ok, strict=True) if good]
    working = working[: args.count]
    args.out.write_text("".join(f"{p}\n" for p in working))
    print(f"{len(working)} reach the site; wrote {args.out}")


if __name__ == "__main__":
    asyncio.run(main())
