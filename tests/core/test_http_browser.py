import asyncio
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

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
