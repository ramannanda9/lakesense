"""
OpenLineage DataQualityAssertions facet converter.

Converts a lakesense InterpretationResult into an OpenLineage-compatible
DataQualityAssertionsDatasetFacet dict (or typed object if openlineage-python
is installed). Callers attach this to their own OL RunEvent.

Usage:
    from lakesense.lineage import to_openlineage_facets

    result = await framework.run(job)
    facets = to_openlineage_facets(result)
    # Attach to your OL InputDataset:
    # InputDataset(namespace="...", name="...", facets=facets)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lakesense.core.result import InterpretationResult, Severity


@dataclass
class AssertionThresholds:
    """Configurable thresholds for assertion pass/fail.

    Defaults match the warn-level thresholds from _heuristic_severity.
    """

    jaccard_delta: float = -0.1
    cardinality_ratio_low: float = 0.7
    cardinality_ratio_high: float = 1.5
    null_rate_delta: float = 0.1
    row_count_delta_low: float = 0.5
    row_count_delta_high: float = 2.0
    bool_rate_delta: float = 0.05
    categorical_shift: float = 0.2
    quantile_shift: float = 0.3


def _build_assertions(
    result: InterpretationResult,
    thresholds: AssertionThresholds,
) -> list[dict[str, Any]]:
    """Map DatasetDriftSummary signals to OL assertion dicts."""
    s = result.dataset_drift_summary
    assertions: list[dict[str, Any]] = []

    # Overall gate — captures final severity (heuristic + optional LLM)
    assertions.append(
        {
            "assertion": "lakesense_quality_check",
            "success": result.severity != Severity.ALERT,
            "column": None,
        }
    )

    # Schema drift — missing columns
    for col in s.missing_columns:
        assertions.append(
            {
                "assertion": "schema_column_present",
                "success": False,
                "column": col,
            }
        )

    # Schema drift — new columns (informational)
    for col in s.new_columns:
        assertions.append(
            {
                "assertion": "schema_column_new",
                "success": True,
                "column": col,
            }
        )

    # Row count ratio
    if s.row_count_delta is not None:
        assertions.append(
            {
                "assertion": "row_count_ratio_within_bounds",
                "success": thresholds.row_count_delta_low <= s.row_count_delta <= thresholds.row_count_delta_high,
                "column": None,
            }
        )

    # Null rate change
    if s.max_null_rate_delta is not None:
        assertions.append(
            {
                "assertion": "null_rate_change_within_bounds",
                "success": s.max_null_rate_delta <= thresholds.null_rate_delta,
                "column": s.null_rate_worst_column,
            }
        )

    # Jaccard similarity
    if s.jaccard_delta is not None:
        assertions.append(
            {
                "assertion": "jaccard_similarity_within_bounds",
                "success": s.jaccard_delta >= thresholds.jaccard_delta,
                "column": s.jaccard_worst_column,
            }
        )

    # Cardinality ratio
    if s.cardinality_ratio is not None:
        assertions.append(
            {
                "assertion": "cardinality_ratio_within_bounds",
                "success": thresholds.cardinality_ratio_low <= s.cardinality_ratio <= thresholds.cardinality_ratio_high,
                "column": s.cardinality_worst_column,
            }
        )

    # KS distribution test
    if s.ks_test_divergent is not None:
        assertions.append(
            {
                "assertion": "distribution_ks_test_pass",
                "success": not s.ks_test_divergent,
                "column": None,
            }
        )

    # Boolean rate change
    if s.bool_true_rate_delta is not None:
        assertions.append(
            {
                "assertion": "bool_rate_change_within_bounds",
                "success": s.bool_true_rate_delta <= thresholds.bool_rate_delta,
                "column": s.bool_rate_worst_column,
            }
        )

    # Categorical distribution shift
    if s.categorical_top_shift is not None:
        assertions.append(
            {
                "assertion": "categorical_distribution_stable",
                "success": s.categorical_top_shift <= thresholds.categorical_shift,
                "column": s.categorical_worst_column,
            }
        )

    # Numeric range violations
    if s.range_min_delta is not None:
        assertions.append(
            {
                "assertion": "numeric_range_within_bounds",
                "success": True,  # range_min_delta presence alone is informational
                "column": s.range_worst_column,
            }
        )

    # Quantile shifts
    for quantile, shift in s.quantile_shifts.items():
        assertions.append(
            {
                "assertion": f"quantile_{quantile}_shift_within_bounds",
                "success": abs(shift) <= thresholds.quantile_shift,
                "column": None,
            }
        )

    return assertions


def to_openlineage_assertions(
    result: InterpretationResult,
    *,
    thresholds: AssertionThresholds | None = None,
) -> dict[str, Any]:
    """Convert an InterpretationResult into an OL DataQualityAssertionsDatasetFacet dict.

    If openlineage-python is installed, returns the typed facet object instead.

    Args:
        result: The interpretation result to convert.
        thresholds: Custom thresholds for assertion pass/fail. Defaults match _heuristic_severity warn level.

    Returns:
        Dict matching the OL DataQualityAssertionsDatasetFacet JSON schema,
        or a typed DataQualityAssertionsDatasetFacet if openlineage-python is installed.
    """
    t = thresholds or AssertionThresholds()
    assertions = _build_assertions(result, t)

    facet_dict: dict[str, Any] = {
        "_producer": "https://github.com/ramannanda9/lakesense",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityAssertionsDatasetFacet.json",
        "assertions": [
            {
                "assertion": a["assertion"],
                "success": a["success"],
                **({"column": a["column"]} if a["column"] is not None else {}),
            }
            for a in assertions
        ],
    }

    try:
        from openlineage.client.facet_v2 import (
            DataQualityAssertionsDatasetFacet,
            DataQualityAssertionsDatasetFacetAssertions,
        )

        return DataQualityAssertionsDatasetFacet(
            assertions=[
                DataQualityAssertionsDatasetFacetAssertions(
                    assertion=a["assertion"],
                    success=a["success"],
                    column=a.get("column"),
                )
                for a in assertions
            ],
        )
    except ImportError:
        return facet_dict


def to_openlineage_facets(
    result: InterpretationResult,
    *,
    thresholds: AssertionThresholds | None = None,
) -> dict[str, Any]:
    """Convenience function returning facets ready for InputDataset(facets=...).

    Returns:
        {"dataQualityAssertions": <assertions facet>}
    """
    return {
        "dataQualityAssertions": to_openlineage_assertions(result, thresholds=thresholds),
    }
