import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest
from aiohttp_socks import ProxyConnectionError

from src.core.browser import BrowserPool
from src.core.http import HttpClient, HttpError, Response


def test_response_encoding_filename_and_json():
    response = Response(
        "url",
        200,
        {
            "content-type": "text/html; charset=iso-8859-1",
            "content-disposition": 'attachment; filename="lei.pdf"',
        },
        b"Legisla\xe7\xe3o",
    )
    assert response.text == "Legislação"
    assert response.filename == "lei.pdf"
    assert response.soup().text == "Legislação"
    assert Response("url", 200, {}, b'{"n":1}').json() == {"n": 1}


@pytest.mark.parametrize("label", ["iso-8859-1", "latin-1", "us-ascii"])
def test_latin1_labels_decode_as_windows_1252_like_browsers(label):
    # Ceará labels windows-1252 pages ISO-8859-1; \x96 is an en dash there.
    response = Response(
        "url", 200, {"content-type": f"text/html; charset={label}"}, b"Lei 1 \x96 Art."
    )
    assert response.text == "Lei 1 – Art."
    assert response.soup().text == "Lei 1 – Art."


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        # RS link pages: no charset anywhere; chardet guessed windows-1250 ("Resoluçăo").
        (
            b"<html><a href='Resolu\xe7\xe3o 001.pdf'>Texto</a></html>",
            "Resolução 001.pdf",
        ),
        (
            "<html><a href='Resolução 001.pdf'>x</a></html>".encode(),
            "Resolução 001.pdf",
        ),
        (
            b'<html><head><meta charset="windows-1250"></head>'
            b"<a href='Resolu\xe7\xe3o 001.pdf'>x</a></html>",
            "Resoluçăo 001.pdf",
        ),
    ],
    ids=["undeclared-latin", "undeclared-utf8", "meta-declared"],
)
@pytest.mark.parametrize("headers", [{"content-type": "text/html"}, {}])
def test_undeclared_charset_decodes_like_browsers(body, expected, headers):
    response = Response("url", 200, headers, body)
    assert response.soup().a["href"] == expected
    assert expected in response.text


def test_json_ignores_meta_charset_inside_strings():
    """Goiás embeds whole HTML documents (with <meta charset>) in UTF-8 JSON."""
    payload = """{"conteudo": "<meta charset='windows-1252'>Resolução"}""".encode()
    response = Response("url", 200, {"content-type": "application/json"}, payload)
    assert response.json()["conteudo"].endswith("Resolução")


@pytest.mark.parametrize("status", [400, 404, 503])
async def test_http_errors_raise_after_retry_budget(status):
    http = HttpClient(max_retries=1)
    response = MagicMock(status=status, reason="unavailable", url="url", headers={})
    response.read = AsyncMock(return_value=b"body")
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=response)
    context.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.request.return_value = context
    http._ensure_session = AsyncMock(return_value=session)
    with pytest.raises(HttpError) as exc:
        await http.request("url")
    assert exc.value.status == status
    context.__aexit__.assert_awaited_once()
    accepted = await http.request("url", ok_statuses=(status,))
    assert accepted.content == b"body"


async def test_body_read_is_inside_retry_and_response_is_released(monkeypatch):
    from tenacity import wait_none

    import src.core.http as module

    monkeypatch.setattr(module, "wait_exponential", lambda **kw: wait_none())
    http = HttpClient(rps=1000, max_retries=2)
    response = MagicMock(status=200, url="url", headers={})
    response.read = AsyncMock(
        side_effect=[aiohttp.ClientPayloadError("truncated"), b"complete"]
    )
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=response)
    context.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.request.return_value = context
    http._ensure_session = AsyncMock(return_value=session)
    assert (await http.request("url")).content == b"complete"
    assert context.__aexit__.await_count == 2
    assert http.stats == {"requests": 2, "retries": 1, "bytes": 8}


async def test_block_pause_holds_every_request_until_it_ends(monkeypatch):
    """Paraná answers 403 to a client that went too fast and blocks it for ~30 min."""
    from tenacity import wait_none

    import src.core.http as module

    monkeypatch.setattr(module, "wait_exponential", lambda **kw: wait_none())
    # 10 rps: the second request is still waiting on the limiter when the 403 lands.
    http = HttpClient(rps=10, max_retries=2, pause_on_403=0.3)
    sent: list[float] = []

    def respond(method, url, **kwargs):
        sent.append(asyncio.get_running_loop().time())
        status = 403 if len(sent) == 1 else 200
        response = MagicMock(status=status, reason="blocked", url=url, headers={})
        response.read = AsyncMock(return_value=b"ok")

        async def arrive() -> MagicMock:
            await asyncio.sleep(0.05 if status == 403 else 0)
            return response

        context = MagicMock()
        context.__aenter__ = AsyncMock(side_effect=arrive)
        context.__aexit__ = AsyncMock(return_value=False)
        return context

    session = MagicMock()
    session.request.side_effect = respond
    http._ensure_session = AsyncMock(return_value=session)

    await asyncio.gather(http.request("blocked"), http.request("other"))
    assert len(sent) == 3
    assert min(sent[1:]) - sent[0] >= 0.3


async def test_pool_never_exceeds_limit_and_replaces_closed_pages():
    pool = BrowserPool(max_pages=2, limiter=MagicMock())
    created = []

    async def new_page():
        page = MagicMock()
        page.is_closed.return_value = False
        created.append(page)
        return page

    context = MagicMock()
    context.new_page = AsyncMock(side_effect=new_page)
    pool._context = context
    active = peak = 0

    async def use_page():
        nonlocal active, peak
        async with pool.page():
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.005)
            active -= 1

    await asyncio.gather(*(use_page() for _ in range(25)))
    assert peak == 2 and len(created) == 2
    pool._idle[-1].is_closed.return_value = True
    async with pool.page() as page:
        assert page is created[-1]
    assert len(created) == 3
    assert sum(not p.is_closed() for p in created) == 2
    await pool.close()


async def test_navigation_uses_shared_limiter():
    limiter = MagicMock()
    limiter.acquire = AsyncMock()
    pool = BrowserPool(limiter=limiter)
    page = MagicMock()
    page.goto = AsyncMock(return_value=MagicMock(status=200))
    await pool.goto(page, "https://example.org")
    limiter.acquire.assert_awaited_once()
    await pool.close()


async def test_cancelled_page_wait_does_not_create_overflow():
    pool = BrowserPool(max_pages=1, limiter=MagicMock())
    context = MagicMock()
    context.new_page = AsyncMock(return_value=MagicMock(is_closed=lambda: False))
    pool._context = context
    async with pool.page():
        task = asyncio.create_task(pool.page().__aenter__())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    async with pool.page():
        pass
    assert context.new_page.await_count == 1
    await pool.close()


def _proxy_service(tmp_path, *proxies: str):
    from src.core.proxy import ProxyService

    path = tmp_path / "proxies.txt"
    path.write_text("\n".join(proxies))
    return ProxyService({"file_path": str(path)})


def _session(status: int = 200, error: Exception | None = None) -> MagicMock:
    response = MagicMock(status=status, reason="r", url="url", headers={})
    response.read = AsyncMock(return_value=b"ok")
    context = MagicMock()
    context.__aenter__ = AsyncMock(side_effect=error, return_value=response)
    context.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.request.return_value = context
    return session


async def test_proxies_rotate_and_failed_ones_rest_before_returning(tmp_path):
    service = _proxy_service(tmp_path, "http://a:1", "socks5://b:2", "c:3")
    assert [await service.get_proxy() for _ in range(4)] == [
        "http://a:1",
        "socks5://b:2",
        "http://c:3",
        "http://a:1",
    ]
    service.suspend("socks5://b:2", 0.1, "timeout")
    assert {await service.get_proxy() for _ in range(4)} == {"http://a:1", "http://c:3"}
    service.suspend("http://a:1", 60, "HTTP 403")
    service.suspend("http://c:3", 60, "HTTP 403")
    # With every proxy resting, requests go direct.
    assert await service.get_proxy() is None
    # Free proxies flap: a failed one comes back once its rest ends.
    await asyncio.sleep(0.15)
    assert await service.get_proxy() == "socks5://b:2"


async def test_each_proxy_has_its_own_rate_limit(tmp_path):
    """Paraná limits each IP; N proxies at 0.3 rps each fetch N times faster."""
    http = HttpClient(
        rps=1, proxy_service=_proxy_service(tmp_path, "http://a:1", "http://b:2")
    )
    http._ensure_session = AsyncMock(return_value=_session())
    loop = asyncio.get_running_loop()
    start = loop.time()
    await asyncio.gather(http.request("u1"), http.request("u2"))
    assert loop.time() - start < 0.5
    assert [c.args[0] for c in http._ensure_session.await_args_list] == [
        "http://a:1",
        "http://b:2",
    ]


@pytest.mark.parametrize(
    "failing",
    [
        _session(status=403),
        _session(error=ProxyConnectionError(111, "refused")),
        # Seen live: a proxy that hangs up mid SOCKS handshake.
        _session(error=asyncio.IncompleteReadError(b"", 2)),
    ],
    ids=["blocked", "dead", "hung-up"],
)
async def test_failed_proxy_rests_and_the_retry_uses_the_next(
    tmp_path, monkeypatch, failing
):
    from tenacity import wait_none

    import src.core.http as module

    monkeypatch.setattr(module, "wait_exponential", lambda **kw: wait_none())
    service = _proxy_service(tmp_path, "http://a:1", "http://b:2")
    http = HttpClient(rps=1000, max_retries=2, pause_on_403=600, proxy_service=service)
    sessions = {"http://a:1": failing, "http://b:2": _session(), None: _session()}
    http._ensure_session = AsyncMock(side_effect=lambda proxy: sessions[proxy])
    assert (await http.request("url")).content == b"ok"
    assert {await service.get_proxy() for _ in range(3)} == {"http://b:2"}
    # A 403 rests the proxy through the site's block; other failures more briefly.
    assert service._resting["http://a:1"] - time.monotonic() > 100
    # A proxy's block is not the client's: direct requests carry on without a pause.
    await asyncio.wait_for(http.request("url", use_proxy=False), 1)


async def test_direct_requests_skip_the_proxies(tmp_path):
    http = HttpClient(rps=1000, proxy_service=_proxy_service(tmp_path, "http://a:1"))
    http._ensure_session = AsyncMock(return_value=_session())
    await http.request("url", use_proxy=False)
    assert http._ensure_session.await_args.args == (None,)


async def test_proxied_connections_verify_certificates(monkeypatch):
    """Free proxies are untrusted; without verification one could rewrite documents."""
    import ssl

    import src.core.http as module

    from_url = MagicMock(side_effect=lambda url, **kw: aiohttp.TCPConnector())
    monkeypatch.setattr(module.ProxyConnector, "from_url", from_url)
    http = HttpClient(verify_ssl=False)
    await http._ensure_session("socks5://a:1")
    assert from_url.call_args.kwargs["ssl"].verify_mode == ssl.CERT_REQUIRED
    await http.close()
