"""Pagination helpers for BaseScraper.

Extracted from BaseScraper; access via ``self.paginator`` on any BaseScraper subclass.
"""

from __future__ import annotations

from typing import Any, Callable, Coroutine


def _flatten(results: list) -> list:
    """Flatten a list of lists/items, filtering out None."""
    flat: list = []
    for item in results:
        if isinstance(item, list):
            flat.extend(item)
        elif item is not None:
            flat.append(item)
    return flat


class PaginationHelper:
    """Pagination utilities composed into BaseScraper as ``self.paginator``.

    ``gather_fn`` must be ``BaseScraper._gather_results`` (bound method).
    """

    def __init__(self, gather_fn, max_workers: int):
        self._gather = gather_fn
        self.max_workers = max_workers

    def calc_pages(self, total: int, per_page: int) -> int:
        """Number of pages needed for *total* items at *per_page* each."""
        if total <= 0 or per_page <= 0:
            return 0
        return (total + per_page - 1) // per_page

    async def fetch_all_pages(
        self,
        make_task: Callable[[int], Coroutine],
        total_pages: int,
        *,
        start_page: int = 2,
        context: dict | None = None,
        desc: str = "",
    ) -> list:
        """Fetch pages ``start_page``..``total_pages`` concurrently and flatten.

        Typical usage — call after fetching and parsing page 1 yourself::

            docs = first_page_docs
            extra = await self.paginator.fetch_all_pages(
                lambda p: self._get_docs_links(self._build_url(year, p)),
                total_pages,
                context=ctx,
                desc="SCRAPER | year | get_docs_links",
            )
            docs.extend(extra)

        Returns a flat list of all items gathered from the extra pages.
        """
        if total_pages < start_page:
            return []
        tasks = [make_task(page) for page in range(start_page, total_pages + 1)]
        results = await self._gather(tasks, context=context, desc=desc)
        return _flatten(results)

    async def collect_paginated_listing(
        self,
        first_page_docs: list[dict],
        *,
        total_pages: int,
        make_page_task: Callable[[int], Coroutine],
        context: dict | None = None,
        start_page: int = 2,
        desc: str = "",
    ) -> list[dict]:
        """Return first-page docs plus every remaining page in one flat list."""
        documents = list(first_page_docs)
        documents.extend(
            await self.fetch_all_pages(
                make_page_task,
                total_pages,
                start_page=start_page,
                context=context,
                desc=desc,
            )
        )
        return documents

    async def paginate_until_end(
        self,
        *,
        make_task: Callable[[int], Coroutine[Any, Any, tuple[list[dict], bool]]],
        context: dict,
        desc: str = "",
        initial_batch: int = 1,
        batch_growth: int | None = None,
        max_batch: int | None = None,
    ) -> list[dict]:
        """Fetch pages in growing batches until a page signals end-of-results.

        ``make_task(page_number)`` must return ``(docs, reached_end)``.
        """
        batch = initial_batch
        growth = batch_growth if batch_growth is not None else self.max_workers
        cap = max_batch or self.max_workers
        page = 1
        all_docs: list[dict] = []

        while True:
            tasks = [make_task(p) for p in range(page, page + batch)]
            results = await self._gather(tasks, context=context, desc=desc)

            reached_end = False
            batch_docs: list[dict] = []
            for docs, ended in results:
                if ended:
                    reached_end = True
                if docs:
                    batch_docs.extend(docs)

            all_docs.extend(batch_docs)
            if reached_end or not batch_docs:
                break

            page += batch
            batch = min(batch + growth, cap)

        return all_docs
