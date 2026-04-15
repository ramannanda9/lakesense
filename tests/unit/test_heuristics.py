from lakesense.core.result import DatasetDriftSummary, Severity
from lakesense.interpreter.base import _heuristic_severity


def test_missing_columns_alert():
    sig = DatasetDriftSummary(missing_columns=["user_id", "email"])
    assert _heuristic_severity(sig) == Severity.ALERT


def test_schema_intact_ok():
    sig = DatasetDriftSummary(missing_columns=[])
    assert _heuristic_severity(sig) == Severity.OK


def test_ks_divergent_warn():
    sig = DatasetDriftSummary(ks_test_divergent=True)
    assert _heuristic_severity(sig) == Severity.WARN


def test_row_count_drop_alert():
    # 95% drop -> 0.05
    sig = DatasetDriftSummary(row_count_delta=0.05)
    assert _heuristic_severity(sig) == Severity.ALERT


def test_row_count_spike_warn():
    # 150% increase -> 2.5
    sig = DatasetDriftSummary(row_count_delta=2.5)
    assert _heuristic_severity(sig) == Severity.WARN


def test_row_count_stable_ok():
    sig = DatasetDriftSummary(row_count_delta=1.0)
    assert _heuristic_severity(sig) == Severity.OK


def test_null_rate_explosion_alert():
    # 60% increase in nulls
    sig = DatasetDriftSummary(max_null_rate_delta=0.6)
    assert _heuristic_severity(sig) == Severity.ALERT


def test_null_rate_creep_warn():
    # 20% increase in nulls
    sig = DatasetDriftSummary(max_null_rate_delta=0.2)
    assert _heuristic_severity(sig) == Severity.WARN


def test_jaccard_decay():
    sig = DatasetDriftSummary(jaccard_delta=-0.4)
    assert _heuristic_severity(sig) == Severity.ALERT

    sig2 = DatasetDriftSummary(jaccard_delta=-0.15)
    assert _heuristic_severity(sig2) == Severity.WARN


def test_cardinality_explosion():
    sig = DatasetDriftSummary(cardinality_ratio=3.0)
    assert _heuristic_severity(sig) == Severity.ALERT
