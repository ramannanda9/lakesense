"""Unit tests for storage backends."""


import pytest

from lakesense.core.result import DriftSignals, InterpretationResult, Severity
from lakesense.sketches.compute import SketchRecord, compute_minhash
from lakesense.storage.parquet import ParquetBackend


def _sketch_record(dataset_id="ds1", job_id="job1", column="col") -> SketchRecord:
    blob, _ = compute_minhash(["hello", "world"])
    return SketchRecord(
        dataset_id=dataset_id,
        job_id=job_id,
        column=column,
        sketch_type="minhash",
        sketch_blob=blob,
        num_perm=128,
        num_rows=1000,
        null_count=10,
    )


def _interp_result(dataset_id="ds1", severity=Severity.OK) -> InterpretationResult:
    return InterpretationResult(
        dataset_id=dataset_id,
        job_id="job1",
        severity=severity,
        summary="test summary",
        drift_signals=DriftSignals(jaccard_delta=-0.1),
    )


class TestParquetBackend:
    @pytest.fixture
    def backend(self, tmp_path):
        return ParquetBackend(tmp_path)

    @pytest.mark.asyncio
    async def test_write_and_read_sketch(self, backend):
        rec = _sketch_record()
        await backend.write_sketches([rec])

        from_ts = (rec.run_ts.replace(hour=0, minute=0, second=0)).isoformat()
        results = await backend.read_sketches("ds1", after_ts=from_ts)
        assert len(results) == 1
        assert results[0].dataset_id == "ds1"
        assert results[0].sketch_blob == rec.sketch_blob

    @pytest.mark.asyncio
    async def test_write_and_read_interpretation(self, backend):
        result = _interp_result()
        await backend.write_interpretation(result)

        history = await backend.read_interpretation_history("ds1", limit=5)
        assert len(history) == 1
        assert history[0].severity == Severity.OK
        assert history[0].summary == "test summary"

    @pytest.mark.asyncio
    async def test_read_empty_returns_empty_list(self, backend):
        results = await backend.read_sketches("nonexistent", after_ts="2020-01-01T00:00:00+00:00")
        assert results == []

        history = await backend.read_interpretation_history("nonexistent")
        assert history == []

    @pytest.mark.asyncio
    async def test_multiple_writes_accumulate(self, backend):
        for _i in range(3):
            r = _interp_result(severity=Severity.WARN)
            await backend.write_interpretation(r)

        history = await backend.read_interpretation_history("ds1", limit=10)
        assert len(history) == 3

    @pytest.mark.asyncio
    async def test_sketch_roundtrip_preserves_blob(self, backend):
        rec = _sketch_record()
        await backend.write_sketches([rec])

        from_ts = (rec.run_ts.replace(hour=0, minute=0, second=0)).isoformat()
        [restored] = await backend.read_sketches("ds1", after_ts=from_ts)
        assert restored.sketch_blob == rec.sketch_blob
        assert restored.num_rows == 1000
        assert restored.null_count == 10


class TestDuckDBBackend:
    @pytest.fixture
    def backend(self, tmp_path):
        pytest.importorskip("duckdb")
        from lakesense.storage.duckdb import DuckDBBackend
        return DuckDBBackend(tmp_path)

    @pytest.mark.asyncio
    async def test_query_interpretations(self, backend):
        for sev in [Severity.OK, Severity.WARN, Severity.ALERT]:
            await backend.write_interpretation(_interp_result(severity=sev))

        df = backend.query(
            "SELECT severity, COUNT(*) as n FROM interpretations "
            "GROUP BY severity ORDER BY severity"
        ).df()
        assert len(df) == 3

    @pytest.mark.asyncio
    async def test_summary(self, backend):
        await backend.write_interpretation(_interp_result(severity=Severity.ALERT))
        df = backend.summary()
        assert "ds1" in df["dataset_id"].values
