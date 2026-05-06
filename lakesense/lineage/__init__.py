"""
OpenLineage integration for lakesense.

Converts InterpretationResult into OpenLineage DataQualityAssertions facets
that callers attach to their own OL RunEvents for WAP gating.

No extra dependencies required — output is plain dicts matching the OL JSON schema.
"""

from lakesense.lineage.openlineage import AssertionThresholds, to_openlineage_assertions, to_openlineage_facets

__all__ = [
    "AssertionThresholds",
    "to_openlineage_assertions",
    "to_openlineage_facets",
]
