"""
Tier 1 base interpreter — runs on every job, always.

Responsibilities:
1. Fetch historical sketch records from storage
2. Build a baseline sketch via the merge engine
3. Compute drift signals (current vs baseline)
4. Call the LLM for a fast structured interpretation
5. Return a populated InterpretationResult

This is intentionally cheap: single LLM call, structured JSON output,
no tool use, no multi-turn. The agent (Tier 2) handles deep investigation.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from lakesense.core.plugin import StorageBackend
from lakesense.core.result import DriftSignals, InterpretationResult, Severity
from lakesense.interpreter.providers import LLMProvider
from lakesense.sketches.compute import SketchRecord
from lakesense.sketches.merge import BaselineConfig, BaselineStrategy, build_baseline
from lakesense.sketches.signals import aggregate_signals, compute_signals

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are a data quality analyst for an ML platform.
You receive sketch-based drift signals for a dataset and produce a concise assessment.

Respond ONLY with a valid JSON object — no preamble, no markdown fences:
{
  "severity": "ok" | "warn" | "alert",
  "summary":  "<one sentence, plain language>",
  "reasoning": "<2-3 sentences explaining the key signals>"
}

Severity guidelines:
  ok    — signals within normal bounds, no action needed
  warn  — notable drift worth monitoring; investigate if it persists
  alert — significant drift; likely impacting model quality; investigate now

Be concise. Do not speculate beyond the provided signals.\
"""


def _build_prompt(
    dataset_id: str,
    job_id: str,
    signals: DriftSignals,
    baseline_config: dict[str, Any],
    history: list[InterpretationResult],
) -> str:
    history_summary = [
        {"run_ts": r.run_ts.isoformat(), "severity": r.severity.value, "summary": r.summary}
        for r in history[-5:]  # last 5 runs for trend context
    ]
    return json.dumps(
        {
            "dataset_id": dataset_id,
            "job_id": job_id,
            "baseline_config": baseline_config,
            "drift_signals": {
                "jaccard_delta": signals.jaccard_delta,
                "cardinality_ratio": signals.cardinality_ratio,
                "quantile_shifts": signals.quantile_shifts,
                "null_rate": signals.null_rate,
                "null_delta": signals.null_delta,
                "worst_signal": signals.worst_signal(),
            },
            "recent_history": history_summary,
        },
        indent=2,
    )


def _parse_llm_response(text: str) -> tuple[Severity, str, str]:
    """Parse LLM JSON response. Returns (severity, summary, reasoning)."""
    try:
        parsed = json.loads(text.strip())
        return (
            Severity(parsed.get("severity", "ok")),
            parsed.get("summary", ""),
            parsed.get("reasoning", ""),
        )
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("failed to parse LLM response: %s — raw: %s", e, text[:200])
        return Severity.WARN, "interpretation parse error", text[:500]


async def base_interpret(
    job: dict[str, Any],
    storage: StorageBackend,
) -> InterpretationResult:
    """
    Tier 1 interpretation pipeline.

    Args:
        job: dict with keys:
            dataset_id    (required)
            job_id        (required)
            sketch_records (optional) list[SketchRecord] — pre-computed sketches
            baseline_config (optional) BaselineConfig — defaults to 7-day rolling window

        storage: StorageBackend instance

    Returns:
        InterpretationResult with severity, summary, and drift_signals populated.
    """
    dataset_id = job["dataset_id"]
    job_id = job["job_id"]

    # data_interval_end is the authoritative timestamp — the period this data covers.
    # For backfills, this is set to the historical date being reprocessed.
    # Falls back to wall clock only for ad-hoc runs with no explicit interval.
    # executed_at captures when the job actually ran (metadata only).
    executed_at = datetime.now(timezone.utc)
    run_ts = job.get("data_interval_end") or executed_at

    # --- Step 1: load current sketch records ---
    current_records: list[SketchRecord] = job.get("sketch_records", [])

    if not current_records:
        logger.warning("no sketch records for %s/%s — skipping", dataset_id, job_id)
        return InterpretationResult(
            dataset_id=dataset_id,
            job_id=job_id,
            run_ts=run_ts,
            executed_at=executed_at,
            severity=Severity.OK,
            summary="no sketch records available for this run",
        )

    # --- Step 2: load historical records for baseline ---
    baseline_cfg: BaselineConfig = job.get(
        "baseline_config",
        BaselineConfig(
            dataset_id=dataset_id,
            strategy=BaselineStrategy.ROLLING_WINDOW,
            window_days=7,
        ),
    )
    after_ts = (run_ts - timedelta(days=baseline_cfg.window_days + 1)).isoformat()
    historical = await storage.read_sketches(
        dataset_id=dataset_id,
        after_ts=after_ts,
        before_ts=run_ts.isoformat(),
    )

    from lakesense.sketches.profile import sketch_record_to_profile
    from lakesense.sketches.signals import compute_profile_signals

    # --- Step 3: build baselines and compute signals per column ---
    all_signals: list[DriftSignals] = []

    # split records by type
    sketch_records = [r for r in current_records if r.sketch_type != "profile"]
    profile_records = [r for r in current_records if r.sketch_type == "profile"]

    for rec in sketch_records:
        baseline = build_baseline(
            records=[r for r in historical if r.sketch_type != "profile"],
            config=baseline_cfg,
            sketch_type=rec.sketch_type,
            column=rec.column,
        )
        if baseline is None:
            logger.debug("no baseline for %s/%s — first run?", rec.column, rec.sketch_type)
            continue
        signals = compute_signals(current=rec, baseline=baseline)
        all_signals.append(signals)

    # profile-based signals — compare current profiles to baseline profiles
    if profile_records:
        hist_profile_records = [r for r in historical if r.sketch_type == "profile"]
        if hist_profile_records:
            current_profiles = [sketch_record_to_profile(r) for r in profile_records]
            baseline_profiles = [sketch_record_to_profile(r) for r in hist_profile_records]
            profile_signals = compute_profile_signals(current_profiles, baseline_profiles)
            all_signals.append(profile_signals)

    if not all_signals:
        return InterpretationResult(
            dataset_id=dataset_id,
            job_id=job_id,
            run_ts=run_ts,
            executed_at=executed_at,
            severity=Severity.OK,
            summary="first run — baseline established, no comparison available",
            baseline_config=baseline_cfg.to_dict(),
        )

    agg_signals = aggregate_signals(all_signals)

    # --- Step 4: fetch history for LLM trend context ---
    history = await storage.read_interpretation_history(dataset_id=dataset_id, limit=5)

    # --- Step 5: LLM call ---
    # --- Step 5: LLM call ---
    llm_provider: LLMProvider | None = job.get("llm_provider")

    if not llm_provider:
        # Auto-resolve from environment if absent
        if "ANTHROPIC_API_KEY" in os.environ:
            from lakesense.interpreter.providers.anthropic import AnthropicProvider

            llm_provider = AnthropicProvider()
        elif "OPENAI_API_KEY" in os.environ:
            from lakesense.interpreter.providers.openai import OpenAIProvider

            llm_provider = OpenAIProvider()

    if not llm_provider:
        severity = _heuristic_severity(agg_signals)
        return InterpretationResult(
            dataset_id=dataset_id,
            job_id=job_id,
            run_ts=run_ts,
            executed_at=executed_at,
            severity=severity,
            summary=f"heuristic (no API key): {agg_signals.worst_signal()}",
            drift_signals=agg_signals,
            baseline_config=baseline_cfg.to_dict(),
        )

    prompt = _build_prompt(dataset_id, job_id, agg_signals, baseline_cfg.to_dict(), history)

    try:
        raw = await llm_provider.analyze(prompt=prompt, system_prompt=_SYSTEM_PROMPT)
        severity, summary, reasoning = _parse_llm_response(raw)
    except Exception as e:
        logger.error("LLM evaluation failed: %s", e)
        severity = _heuristic_severity(agg_signals)
        summary = f"LLM parsing error fallback: {agg_signals.worst_signal()}"
        reasoning = str(e)

    return InterpretationResult(
        dataset_id=dataset_id,
        job_id=job_id,
        run_ts=run_ts,
        executed_at=executed_at,
        severity=severity,
        summary=summary,
        drift_signals=agg_signals,
        baseline_config=baseline_cfg.to_dict(),
        metadata={"reasoning": reasoning},
    )


def _heuristic_severity(signals: DriftSignals) -> Severity:
    """Fallback severity when no LLM API key is configured."""
    # 1. Schema / Structural drift
    if signals.missing_columns:
        return Severity.ALERT

    # 2. Probability Distribution Drift
    if signals.ks_test_divergent:
        return Severity.WARN

    # 3. Massive Volume Drops
    if signals.row_count_delta is not None:
        if signals.row_count_delta < 0.1 or signals.row_count_delta > 5.0:
            return Severity.ALERT
        if signals.row_count_delta < 0.5 or signals.row_count_delta > 2.0:
            return Severity.WARN

    # 4. Null Explosion
    if signals.max_null_rate_delta is not None:
        if signals.max_null_rate_delta > 0.5:
            return Severity.ALERT
        if signals.max_null_rate_delta > 0.1:
            return Severity.WARN

    # 5. Jaccard & Cardinality
    if signals.jaccard_delta is not None:
        if signals.jaccard_delta < -0.3:
            return Severity.ALERT
        if signals.jaccard_delta < -0.1:
            return Severity.WARN
    if signals.cardinality_ratio is not None:
        ratio = signals.cardinality_ratio
        if ratio < 0.5 or ratio > 2.0:
            return Severity.ALERT
        if ratio < 0.7 or ratio > 1.5:
            return Severity.WARN

    return Severity.OK
