import pytest
from lakesense.core.result import DriftSignals, Severity
from lakesense.interpreter.base import _heuristic_severity

def test_missing_columns_alert():
    sig = DriftSignals(missing_columns=["user_id", "email"])
    assert _heuristic_severity(sig) == Severity.ALERT
    
def test_schema_intact_ok():
    sig = DriftSignals(missing_columns=[])
    assert _heuristic_severity(sig) == Severity.OK

def test_ks_divergent_warn():
    sig = DriftSignals(ks_test_divergent=True)
    assert _heuristic_severity(sig) == Severity.WARN

def test_row_count_drop_alert():
    # 95% drop -> 0.05
    sig = DriftSignals(row_count_delta=0.05)
    assert _heuristic_severity(sig) == Severity.ALERT

def test_row_count_spike_warn():
    # 150% increase -> 2.5
    sig = DriftSignals(row_count_delta=2.5)
    assert _heuristic_severity(sig) == Severity.WARN

def test_row_count_stable_ok():
    sig = DriftSignals(row_count_delta=1.0)
    assert _heuristic_severity(sig) == Severity.OK

def test_null_rate_explosion_alert():
    # 60% increase in nulls
    sig = DriftSignals(max_null_rate_delta=0.6)
    assert _heuristic_severity(sig) == Severity.ALERT

def test_null_rate_creep_warn():
    # 20% increase in nulls
    sig = DriftSignals(max_null_rate_delta=0.2)
    assert _heuristic_severity(sig) == Severity.WARN

def test_jaccard_decay():
    sig = DriftSignals(jaccard_delta=-0.4)
    assert _heuristic_severity(sig) == Severity.ALERT
    
    sig2 = DriftSignals(jaccard_delta=-0.15)
    assert _heuristic_severity(sig2) == Severity.WARN

def test_cardinality_explosion():
    sig = DriftSignals(cardinality_ratio=3.0)
    assert _heuristic_severity(sig) == Severity.ALERT
