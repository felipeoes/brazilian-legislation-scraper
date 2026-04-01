"""Pagination mixin for BaseScraper.

Provides ``_gather_results``, ``_process_documents``, ``_fetch_all_pages``,
``_paginate_until_end``, ``_with_save``, and ``_save_gather_errors``.
Mixed into ``BaseScraper`` via MRO — expects ``self.verbose``, ``self.saver``,
``self.error_count``, ``self.max_workers``, ``self.name``, and
``self._save_doc_result`` from the host class.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Callable, Coroutine

from loguru import logger
from tqdm import tqdm

from src.scraper.base.summary_utils import flatten_results, merge_context

if TYPE_CHECKING:
    pass


class PaginationMixin:
    """Pagination and gathering helpers mixed into BaseScraper."""

    async def _with_save(self, coro, context: dict):
        result = await coro
        if result is None:
            return None

        is_list = isinstance(result, list)
        items = result if is_list else [result]
        saved = []
        for r in items:
            doc = merge_context(r, context)
            s = await self._save_doc_result(doc)
            if s is None:
                logger.warning(
                    f"Save failed for '{doc.get('title', '?')}', discarding result"
                )
                continue
            saved.append(s)
        if not saved:
            return None
        return saved if is_list else saved[0]

    async def _save_gather_errors(
        self,
        results: list,
        context: dict,
        desc: str = "",
    ) -> list:
        ctx = {"year": "", "type": "", "situation": "", **context}
        valid = []
        for result in results:
            if isinstance(result, Exception):
                self.error_count += 1
                logger.error(f"{desc} | Error: {result}")
                if self.saver:
                    error_data = {
                        "title": desc or "Unknown",
                        "html_link": "",
                        **ctx,
                    }
                    await self.saver.save_error(error_data, error_message=str(result))
                continue
            if result is None:
                continue
            valid.append(result)
        return valid

    async def _gather_results(
        self,
        tasks: list,
        context: dict | None = None,
        desc: str = "",
    ) -> list:
        if not tasks:
            return []

        if self.verbose:
            progress = tqdm(total=len(tasks), desc=desc or "Gathering")
            wrapped_tasks = [asyncio.create_task(task) for task in tasks]
            for task in wrapped_tasks:
                task.add_done_callback(lambda _: progress.update())
            try:
                results = await asyncio.gather(*wrapped_tasks, return_exceptions=True)
            finally:
                progress.close()
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        return await self._save_gather_errors(results, context or {}, desc)

    async def _process_documents(
        self,
        documents: list,
        *,
        year: int,
        norm_type: str,
        situation: str = "",
        desc: str = "",
        doc_data_fn=None,
        doc_data_kwargs: dict | None = None,
    ) -> list[dict]:
        """Wrap each document through _get_doc_data -> _with_save -> _gather_results."""
        ctx = {"year": year, "type": norm_type, "situation": situation}
        fn = doc_data_fn or self._get_doc_data
        kw = doc_data_kwargs or {}
        # Enrich each doc with context fields (year/type/situation) so that
        # ScrapedDocument validation succeeds when _get_doc_data calls
        # _process_pdf_doc/_process_html_doc.  Doc-specific values take
        # precedence over context defaults (dict merge order: ctx first, then doc).
        enriched = [{**ctx, **doc} for doc in documents]
        tasks = [self._with_save(fn(doc, **kw), ctx) for doc in enriched]
        results = await self._gather_results(
            tasks,
            context=ctx,
            desc=desc or f"{self.name} | {norm_type}",
        )
        logger.debug(
            f"Finished scraping for Year: {year} | Type: {norm_type} "
            f"| Situation: {situation} | Results: {len(results)}"
        )
        return results

    async def _fetch_all_pages(
        self,
        make_task: Callable[[int], Coroutine],
        total_pages: int,
        *,
        start_page: int = 2,
        context: dict | None = None,
        desc: str = "",
    ) -> list:
        """Fetch pages ``start_page``..``total_pages`` concurrently and flatten."""
        if total_pages < start_page:
            return []
        tasks = [make_task(page) for page in range(start_page, total_pages + 1)]
        results = await self._gather_results(tasks, context=context, desc=desc)
        return flatten_results(results)

    async def _paginate_until_end(
        self,
        *,
        make_task: Callable[[int], Coroutine[Any, Any, tuple[list[dict], bool]]],
        context: dict,
        desc: str = "",
        initial_batch: int = 1,
        batch_growth: int | None = None,
        max_batch: int | None = None,
        max_iterations: int = 1000,
    ) -> list[dict]:
        """Fetch pages in growing batches until a page signals end-of-results."""
        batch = initial_batch
        growth = batch_growth if batch_growth is not None else self.max_workers
        cap = max_batch or self.max_workers
        page = 1
        all_docs: list[dict] = []
        iterations = 0

        while iterations < max_iterations:
            tasks = [make_task(p) for p in range(page, page + batch)]
            results = await self._gather_results(tasks, context=context, desc=desc)

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
            iterations += 1
        else:
            logger.warning(
                f"_paginate_until_end: reached max_iterations={max_iterations} "
                f"without end signal — stopping ({desc})"
            )

        return all_docs
