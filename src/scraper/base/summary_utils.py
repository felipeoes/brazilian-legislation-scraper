from __future__ import annotations

from typing import Any

from src.scraper.base.persistence import _normalize_year
from src.scraper.base.schemas import ScrapedDocument


def _format_duration(seconds: float) -> str:
    """Format seconds into a human-readable string."""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    parts = []
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)


def _llm_usage_totals(llm_usage: dict[str, dict]) -> dict[str, int]:
    """Aggregate per-model LLM usage into a single totals dict."""
    totals = {
        "requests": 0,
        "successful_requests": 0,
        "failed_requests": 0,
        "input_tokens": 0,
        "cached_tokens": 0,
        "output_tokens": 0,
        "reasoning_tokens": 0,
    }
    for usage in llm_usage.values():
        requests = int(usage.get("requests", 0) or 0)
        failed_requests = int(usage.get("failed_requests", 0) or 0)
        successful_requests = usage.get("successful_requests")
        if successful_requests is None:
            successful_requests = max(requests - failed_requests, 0)
        else:
            successful_requests = int(successful_requests or 0)
        totals["requests"] += requests
        totals["successful_requests"] += successful_requests
        totals["failed_requests"] += failed_requests
        for key in totals:
            if key in {"requests", "successful_requests", "failed_requests"}:
                continue
            totals[key] += int(usage.get(key, 0) or 0)
    return totals


def _format_llm_usage(llm_usage: dict[str, dict]) -> str:
    """Build a compact human-readable LLM usage string with per-model details."""

    def _fmt(usage: dict[str, int]) -> str:
        requests = int(usage.get("requests", 0) or 0)
        failed_requests = int(usage.get("failed_requests", 0) or 0)
        successful_requests = usage.get("successful_requests")
        if successful_requests is None:
            successful_requests = max(requests - failed_requests, 0)
        else:
            successful_requests = int(successful_requests or 0)
        return (
            f"{requests} reqs ({successful_requests} ok, {failed_requests} failed), "
            f"{int(usage.get('input_tokens', 0) or 0)} input, "
            f"{int(usage.get('cached_tokens', 0) or 0)} cached, "
            f"{int(usage.get('output_tokens', 0) or 0)} output, "
            f"{int(usage.get('reasoning_tokens', 0) or 0)} reasoning"
        )

    totals = _llm_usage_totals(llm_usage)
    model_breakdown = "; ".join(
        f"{model}: {_fmt(usage)}" for model, usage in sorted(llm_usage.items())
    )
    summary = f"LLM total {_fmt(totals)}"
    if model_breakdown:
        summary += f" | {model_breakdown}"
    return summary


def _build_llm_usage_summary(llm_usage_by_model: dict[str, dict]) -> dict[str, Any]:
    """Build structured LLM usage summary for a run."""
    return {
        "models": llm_usage_by_model,
        "totals": _llm_usage_totals(llm_usage_by_model),
        "human": _format_llm_usage(llm_usage_by_model),
    }


def _build_run_summary(
    *,
    scraper: str,
    year_start: int,
    year_end: int,
    total_documents: int,
    total_errors: int,
    elapsed_seconds: float,
    completed_at: str,
    types_summary: dict[str, dict],
    llm_usage: dict[str, Any],
) -> dict[str, Any]:
    """Build a single-run summary snapshot."""
    rounded_elapsed = round(elapsed_seconds, 2)
    return {
        "scraper": scraper,
        "year_start": year_start,
        "year_end": year_end,
        "total_documents": total_documents,
        "total_errors": total_errors,
        "elapsed_seconds": rounded_elapsed,
        "elapsed_human": _format_duration(rounded_elapsed),
        "completed_at": completed_at,
        "types_summary": types_summary,
        "llm_usage": llm_usage,
    }


def _empty_llm_usage_summary() -> dict[str, Any]:
    """Return an empty structured LLM usage summary."""
    return _build_llm_usage_summary({})


def _coerce_summary_runs(summary_data: Any) -> list[dict[str, Any]]:
    """Extract historical run snapshots from existing summary data."""
    if not isinstance(summary_data, dict):
        return []

    existing_runs = summary_data.get("runs")
    if isinstance(existing_runs, list):
        return [run for run in existing_runs if isinstance(run, dict)]

    if "completed_at" not in summary_data:
        return []

    return [
        {
            "scraper": summary_data.get("scraper", ""),
            "year_start": summary_data.get("year_start"),
            "year_end": summary_data.get("year_end"),
            "total_documents": int(summary_data.get("total_documents", 0) or 0),
            "total_errors": int(summary_data.get("total_errors", 0) or 0),
            "elapsed_seconds": round(
                float(summary_data.get("elapsed_seconds", 0) or 0), 2
            ),
            "elapsed_human": str(summary_data.get("elapsed_human", "0s") or "0s"),
            "completed_at": str(summary_data.get("completed_at", "")),
            "types_summary": summary_data.get("types_summary", {}) or {},
            "llm_usage": summary_data.get("llm_usage") or _empty_llm_usage_summary(),
        }
    ]


def _meaningful_context_value(value: Any) -> str | None:
    """Return a cleaned context value unless it is a generic placeholder."""
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if normalized.casefold() in {"na", "n/a", "all"}:
        return None
    return normalized


def merge_context(result: dict | ScrapedDocument, context: dict) -> dict:
    """Merge a document result dict with its scraping context."""
    if isinstance(result, ScrapedDocument):
        # Convert to dict but preserve raw_content/content_extension.
        # Use underscore-prefixed keys so save_doc_result's dict branch can
        # find them via pop("_raw_content") / pop("_content_extension").
        # Writing these after model_dump() also overwrites any stale underscore
        # extras that scrapers (e.g. Goiás) may have left on the ScrapedDocument.
        res_dict = result.model_dump()
        if result.raw_content is not None:
            res_dict["_raw_content"] = result.raw_content
        if result.content_extension is not None:
            res_dict["_content_extension"] = result.content_extension
    else:
        res_dict = result

    doc = {**context, **res_dict}

    result_type = _meaningful_context_value(res_dict.get("type"))
    context_type = _meaningful_context_value(context.get("type"))
    if result_type:
        doc["type"] = result_type
    elif context_type:
        doc["type"] = context_type
    else:
        doc["type"] = ""

    result_situation = _meaningful_context_value(res_dict.get("situation"))
    context_situation = _meaningful_context_value(context.get("situation"))
    if result_situation:
        doc["situation"] = result_situation
    elif context_situation:
        doc["situation"] = context_situation
    else:
        doc["situation"] = ""

    year = _normalize_year(doc.get("year"))
    ctx_year = _normalize_year(context.get("year"))
    doc["year"] = year if year is not None else ctx_year
    return doc


def flatten_results(results: list) -> list[dict | ScrapedDocument]:
    """Flatten a list of results (some of which may be sub-lists) into a single list."""
    flat: list[dict | ScrapedDocument] = []
    for item in results:
        if isinstance(item, list):
            flat.extend(item)
        elif item is not None:
            flat.append(item)
    return flat
