"""Unit tests for lakesense.core"""

from datetime import datetime, timedelta, timezone

import pytest

from lakesense.core.plugin import SketchPlugin
from lakesense.core.result import DriftSignals, InterpretationResult, Severity


class TestSeverity:
    def test_is_actionable(self):
        assert Severity.OK.is_actionable() is False
        assert Severity.WARN.is_actionable() is True
        assert Severity.ALERT.is_actionable() is True

    def test_string_value(self):
        assert Severity.OK == "ok"
        assert Severity.ALERT == "alert"


class TestDriftSignals:
    def test_worst_signal_jaccard(self):
        s = DriftSignals(jaccard_delta=-0.4)
        assert "jaccard_delta" in s.worst_signal()

    def test_worst_signal_empty(self):
        s = DriftSignals()
        assert s.worst_signal() == "no signals"

    def test_null_rate(self):
        s = DriftSignals()
        assert s.null_rate is None


class TestInterpretationResult:
    def _make(self, **kwargs) -> InterpretationResult:
        defaults = dict(dataset_id="ds1", job_id="job1")
        defaults.update(kwargs)
        return InterpretationResult(**defaults)

    def test_defaults(self):
        r = self._make()
        assert r.severity == Severity.OK
        assert r.is_actionable() is False
        assert r.is_agent_enriched() is False

    def test_round_trip(self):
        r = self._make(
            severity=Severity.ALERT,
            summary="test summary",
            drift_signals=DriftSignals(jaccard_delta=-0.5, cardinality_ratio=0.4),
            root_cause="schema change",
            owners=["alice", "bob"],
        )
        d = r.to_dict()
        restored = InterpretationResult.from_dict(d)
        assert restored.severity == Severity.ALERT
        assert restored.summary == "test summary"
        assert restored.root_cause == "schema change"
        assert restored.owners == ["alice", "bob"]
        assert restored.drift_signals.jaccard_delta == pytest.approx(-0.5)

    def test_is_agent_enriched(self):
        r = self._make(root_cause="upstream schema drop")
        assert r.is_agent_enriched() is True

    def test_backfill_round_trip(self):
        """executed_at and run_ts survive a to_dict/from_dict round-trip correctly."""
        data_ts = datetime(2024, 1, 7, tzinfo=timezone.utc)
        exec_ts = datetime(2024, 1, 14, tzinfo=timezone.utc)  # backfill ran a week later

        r = InterpretationResult(
            dataset_id="ds",
            job_id="backfill_job",
            run_ts=data_ts,
            executed_at=exec_ts,
            severity=Severity.WARN,
            summary="backfill test",
        )
        assert r.run_ts == data_ts
        assert r.executed_at == exec_ts
        assert r.executed_at - r.run_ts == timedelta(days=7)

        restored = InterpretationResult.from_dict(r.to_dict())
        assert restored.run_ts == data_ts
        assert restored.executed_at == exec_ts


class TestSketchPluginABC:
    def test_cannot_instantiate_abstract(self):
        with pytest.raises(TypeError):
            SketchPlugin()  # type: ignore

    def test_concrete_subclass(self):
        class MyPlugin(SketchPlugin):
            def should_run(self, result):
                return result.severity == Severity.ALERT

            async def run(self, result):
                result.metadata["ran"] = True
                return result

        p = MyPlugin()
        assert p.name == "MyPlugin"

        ok_result = InterpretationResult(dataset_id="d", job_id="j")
        alert_result = InterpretationResult(dataset_id="d", job_id="j", severity=Severity.ALERT)

        assert p.should_run(ok_result) is False
        assert p.should_run(alert_result) is True


class TestFromDictDatetimeCompat:
    """Verify from_dict handles both ISO strings and datetime objects."""

    def test_from_dict_with_iso_strings(self):
        d = {
            "dataset_id": "ds1",
            "job_id": "j1",
            "run_ts": "2026-03-30T12:00:00+00:00",
            "executed_at": "2026-03-30T12:00:00+00:00",
            "severity": "ok",
            "summary": "test",
        }
        r = InterpretationResult.from_dict(d)
        assert r.run_ts == datetime(2026, 3, 30, 12, 0, tzinfo=timezone.utc)

    def test_from_dict_with_datetime_objects(self):
        """Arrow timestamp columns return datetime objects, not strings."""
        ts = datetime(2026, 3, 30, 12, 0, tzinfo=timezone.utc)
        d = {
            "dataset_id": "ds1",
            "job_id": "j1",
            "run_ts": ts,
            "executed_at": ts,
            "severity": "warn",
            "summary": "test",
        }
        r = InterpretationResult.from_dict(d)
        assert r.run_ts == ts
        assert r.executed_at == ts

    def test_from_dict_with_naive_datetime_gets_utc(self):
        """Naive datetimes (no tzinfo) should be assumed UTC."""
        ts = datetime(2026, 3, 30, 12, 0)  # no tz
        d = {
            "dataset_id": "ds1",
            "job_id": "j1",
            "run_ts": ts,
            "executed_at": ts,
            "severity": "ok",
            "summary": "test",
        }
        r = InterpretationResult.from_dict(d)
        assert r.run_ts.tzinfo == timezone.utc
