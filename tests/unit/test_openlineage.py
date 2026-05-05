"""Unit tests for lakesense.lineage.openlineage"""

from lakesense.core.result import DatasetDriftSummary, InterpretationResult, Severity
from lakesense.lineage.openlineage import AssertionThresholds, to_openlineage_assertions, to_openlineage_facets


def _result(severity=Severity.OK, **summary_kwargs) -> InterpretationResult:
    return InterpretationResult(
        dataset_id="ds1",
        job_id="j1",
        severity=severity,
        dataset_drift_summary=DatasetDriftSummary(**summary_kwargs),
    )


class TestOverallGateAssertion:
    def test_ok_passes(self):
        facet = to_openlineage_assertions(_result(Severity.OK))
        gate = next(a for a in facet["assertions"] if a["assertion"] == "lakesense_quality_check")
        assert gate["success"] is True
        assert gate["actual"] == "ok"

    def test_warn_passes(self):
        facet = to_openlineage_assertions(_result(Severity.WARN))
        gate = next(a for a in facet["assertions"] if a["assertion"] == "lakesense_quality_check")
        assert gate["success"] is True
        assert gate["actual"] == "warn"

    def test_alert_fails(self):
        facet = to_openlineage_assertions(_result(Severity.ALERT))
        gate = next(a for a in facet["assertions"] if a["assertion"] == "lakesense_quality_check")
        assert gate["success"] is False
        assert gate["actual"] == "alert"


class TestSchemaAssertions:
    def test_missing_columns(self):
        facet = to_openlineage_assertions(_result(missing_columns=["user_id", "email"]))
        schema_asserts = [a for a in facet["assertions"] if a["assertion"] == "schema_column_present"]
        assert len(schema_asserts) == 2
        assert all(a["success"] is False for a in schema_asserts)
        cols = {a["column"] for a in schema_asserts}
        assert cols == {"user_id", "email"}
        assert all(a["actual"] == "missing" for a in schema_asserts)

    def test_new_columns(self):
        facet = to_openlineage_assertions(_result(new_columns=["new_feat"]))
        new_asserts = [a for a in facet["assertions"] if a["assertion"] == "schema_column_new"]
        assert len(new_asserts) == 1
        assert new_asserts[0]["success"] is True
        assert new_asserts[0]["column"] == "new_feat"


class TestSignalAssertions:
    def test_jaccard_pass(self):
        facet = to_openlineage_assertions(_result(jaccard_delta=-0.05, jaccard_worst_column="desc"))
        a = next(a for a in facet["assertions"] if a["assertion"] == "jaccard_similarity_within_bounds")
        assert a["success"] is True
        assert a["column"] == "desc"
        assert a["expected"] == ">= -0.1"
        assert a["actual"] == "-0.050"

    def test_jaccard_fail(self):
        facet = to_openlineage_assertions(_result(jaccard_delta=-0.2, jaccard_worst_column="desc"))
        a = next(a for a in facet["assertions"] if a["assertion"] == "jaccard_similarity_within_bounds")
        assert a["success"] is False

    def test_cardinality_pass(self):
        facet = to_openlineage_assertions(_result(cardinality_ratio=1.0, cardinality_worst_column="uid"))
        a = next(a for a in facet["assertions"] if a["assertion"] == "cardinality_ratio_within_bounds")
        assert a["success"] is True

    def test_cardinality_fail_high(self):
        facet = to_openlineage_assertions(_result(cardinality_ratio=3.0, cardinality_worst_column="uid"))
        a = next(a for a in facet["assertions"] if a["assertion"] == "cardinality_ratio_within_bounds")
        assert a["success"] is False
        assert a["actual"] == "3.00"

    def test_null_rate_pass(self):
        facet = to_openlineage_assertions(_result(max_null_rate_delta=0.05, null_rate_worst_column="score"))
        a = next(a for a in facet["assertions"] if a["assertion"] == "null_rate_change_within_bounds")
        assert a["success"] is True
        assert a["column"] == "score"

    def test_null_rate_fail(self):
        facet = to_openlineage_assertions(_result(max_null_rate_delta=0.3, null_rate_worst_column="score"))
        a = next(a for a in facet["assertions"] if a["assertion"] == "null_rate_change_within_bounds")
        assert a["success"] is False

    def test_row_count_pass(self):
        facet = to_openlineage_assertions(_result(row_count_delta=1.1))
        a = next(a for a in facet["assertions"] if a["assertion"] == "row_count_ratio_within_bounds")
        assert a["success"] is True

    def test_row_count_fail_low(self):
        facet = to_openlineage_assertions(_result(row_count_delta=0.05))
        a = next(a for a in facet["assertions"] if a["assertion"] == "row_count_ratio_within_bounds")
        assert a["success"] is False

    def test_ks_divergent_fail(self):
        facet = to_openlineage_assertions(_result(ks_test_divergent=True))
        a = next(a for a in facet["assertions"] if a["assertion"] == "distribution_ks_test_pass")
        assert a["success"] is False
        assert a["actual"] == "divergent"

    def test_ks_not_divergent_pass(self):
        facet = to_openlineage_assertions(_result(ks_test_divergent=False))
        a = next(a for a in facet["assertions"] if a["assertion"] == "distribution_ks_test_pass")
        assert a["success"] is True
        assert a["actual"] == "not divergent"

    def test_bool_rate_fail(self):
        facet = to_openlineage_assertions(_result(bool_true_rate_delta=0.3, bool_rate_worst_column="is_active"))
        a = next(a for a in facet["assertions"] if a["assertion"] == "bool_rate_change_within_bounds")
        assert a["success"] is False
        assert a["column"] == "is_active"

    def test_categorical_fail(self):
        facet = to_openlineage_assertions(_result(categorical_top_shift=0.8, categorical_worst_column="status"))
        a = next(a for a in facet["assertions"] if a["assertion"] == "categorical_distribution_stable")
        assert a["success"] is False

    def test_range_min_delta_fails(self):
        facet = to_openlineage_assertions(_result(range_min_delta=-5.0, range_worst_column="amount"))
        a = next(a for a in facet["assertions"] if a["assertion"] == "numeric_range_within_bounds")
        assert a["success"] is False
        assert a["column"] == "amount"

    def test_quantile_shifts(self):
        facet = to_openlineage_assertions(_result(quantile_shifts={"p50": 0.1, "p99": 0.5}))
        q50 = next(a for a in facet["assertions"] if a["assertion"] == "quantile_p50_shift_within_bounds")
        q99 = next(a for a in facet["assertions"] if a["assertion"] == "quantile_p99_shift_within_bounds")
        assert q50["success"] is True
        assert q99["success"] is False


class TestEmptySummary:
    def test_no_signals_only_gate(self):
        facet = to_openlineage_assertions(_result())
        assert len(facet["assertions"]) == 1
        assert facet["assertions"][0]["assertion"] == "lakesense_quality_check"
        assert facet["assertions"][0]["success"] is True

    def test_none_drift_summary_does_not_raise(self):
        """First-run / no-sketch results have dataset_drift_summary=None."""
        result = InterpretationResult(
            dataset_id="ds1",
            job_id="j1",
            severity=Severity.OK,
            dataset_drift_summary=None,
        )
        facet = to_openlineage_assertions(result)
        assert len(facet["assertions"]) == 1
        assert facet["assertions"][0]["assertion"] == "lakesense_quality_check"


class TestCustomThresholds:
    def test_tighter_jaccard(self):
        t = AssertionThresholds(jaccard_delta=-0.01)
        facet = to_openlineage_assertions(_result(jaccard_delta=-0.05), thresholds=t)
        a = next(a for a in facet["assertions"] if a["assertion"] == "jaccard_similarity_within_bounds")
        assert a["success"] is False  # would pass with default -0.1
        assert a["expected"] == ">= -0.01"

    def test_looser_null_rate(self):
        t = AssertionThresholds(null_rate_delta=0.5)
        facet = to_openlineage_assertions(
            _result(max_null_rate_delta=0.3, null_rate_worst_column="x"),
            thresholds=t,
        )
        a = next(a for a in facet["assertions"] if a["assertion"] == "null_rate_change_within_bounds")
        assert a["success"] is True  # would fail with default 0.1


class TestFacetStructure:
    def test_has_schema_url(self):
        facet = to_openlineage_assertions(_result())
        assert "_schemaURL" in facet
        assert "DataQualityAssertions" in facet["_schemaURL"]

    def test_has_producer(self):
        facet = to_openlineage_assertions(_result())
        assert "_producer" in facet
        assert "lakesense" in facet["_producer"]

    def test_column_omitted_when_none(self):
        facet = to_openlineage_assertions(_result(row_count_delta=1.0))
        row_assert = next(a for a in facet["assertions"] if a["assertion"] == "row_count_ratio_within_bounds")
        assert "column" not in row_assert

    def test_expected_and_actual_present(self):
        facet = to_openlineage_assertions(_result(jaccard_delta=-0.5, jaccard_worst_column="col"))
        a = next(a for a in facet["assertions"] if a["assertion"] == "jaccard_similarity_within_bounds")
        assert "expected" in a
        assert "actual" in a


class TestConvenienceWrapper:
    def test_to_openlineage_facets_structure(self):
        facets = to_openlineage_facets(_result(Severity.WARN, jaccard_delta=-0.5))
        assert "dataQualityAssertions" in facets
        inner = facets["dataQualityAssertions"]
        assert "assertions" in inner
