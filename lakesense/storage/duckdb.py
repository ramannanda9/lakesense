"""
DuckDBBackend — extends ParquetBackend with SQL query capabilities.

DuckDB reads the same Parquet files written by ParquetBackend,
adding fast analytical queries over interpretation history and sketch metrics.

Requires: pip install lakesense[duckdb]

Usage:
    backend = DuckDBBackend("./lakesense_data")

    # same interface as ParquetBackend
    await backend.write_sketch(record)

    # plus SQL access
    df = backend.query("SELECT dataset_id, severity, COUNT(*) FROM interpretations GROUP BY 1,2")
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from lakesense.core.result import InterpretationResult
from lakesense.storage.parquet import ParquetBackend


class DuckDBBackend(ParquetBackend):
    """
    DuckDB-powered storage backend.

    Inherits all read/write from ParquetBackend.
    Adds .query() for ad-hoc SQL over the sketch and interpretation tables.

    The DuckDB connection is lazy — only created on first .query() call.
    This keeps startup fast and avoids the optional dependency at import time.
    """

    def __init__(self, base_path: str | Path) -> None:
        super().__init__(base_path)
        self._conn: Any = None  # duckdb.DuckDBPyConnection, typed as Any to avoid hard import

    def _get_conn(self) -> Any:
        if self._conn is None:
            try:
                import duckdb
            except ImportError as e:
                raise ImportError("DuckDBBackend requires duckdb. Install with: pip install lakesense[duckdb]") from e
            self._conn = duckdb.connect()
            self._register_views()
        return self._conn

    def _register_views(self) -> None:
        """Register Parquet glob paths as DuckDB views, skipping missing directories."""
        conn = self._conn
        sketches_glob = str(self._sketches_root / "**" / "*.parquet")
        interp_glob = str(self._interp_root / "**" / "*.parquet")

        # sketches view — only register if files exist
        if list(self._sketches_root.rglob("*.parquet")):
            conn.execute(f"""
                CREATE OR REPLACE VIEW sketches AS
                SELECT * FROM read_parquet('{sketches_glob}', hive_partitioning=true)
            """)
        else:
            conn.execute("""
                CREATE OR REPLACE VIEW sketches AS
                SELECT NULL::VARCHAR as dataset_id WHERE FALSE
            """)

        # interpretations view — only register if files exist
        if list(self._interp_root.rglob("*.parquet")):
            conn.execute(f"""
                CREATE OR REPLACE VIEW interpretations AS
                SELECT * FROM read_parquet('{interp_glob}', hive_partitioning=true)
            """)
        else:
            conn.execute("""
                CREATE OR REPLACE VIEW interpretations AS
                SELECT
                    NULL::VARCHAR  as dataset_id,
                    NULL::VARCHAR  as job_id,
                    NULL::VARCHAR  as run_ts,
                    NULL::VARCHAR  as severity,
                    NULL::VARCHAR  as summary,
                    NULL::DOUBLE   as jaccard_delta,
                    NULL::DOUBLE   as cardinality_ratio,
                    NULL::DOUBLE   as null_delta,
                    NULL::VARCHAR  as root_cause,
                    NULL::VARCHAR  as affected_urns,
                    NULL::VARCHAR  as owners,
                    NULL::VARCHAR  as baseline_config,
                    NULL::VARCHAR  as metadata
                WHERE FALSE
            """)

    def query(self, sql: str) -> Any:
        """
        Run an arbitrary SQL query over sketch and interpretation data.

        Available tables/views:
            sketches        — all SketchRecords
            interpretations — all InterpretationResults

        Returns:
            duckdb.DuckDBPyRelation (call .df() for pandas, .arrow() for pyarrow)

        Examples:
            # severity distribution by dataset
            backend.query(
                "SELECT dataset_id, severity, COUNT(*) as n "
                "FROM interpretations GROUP BY 1, 2 ORDER BY 1, 2"
            ).df()

            # recent alerts
            backend.query(
                "SELECT * FROM interpretations "
                "WHERE severity = 'alert' ORDER BY run_ts DESC LIMIT 20"
            ).df()

            # Jaccard trend for a dataset
            backend.query(
                "SELECT run_ts, jaccard_delta FROM interpretations "
                "WHERE dataset_id = 'user_features' ORDER BY run_ts"
            ).df()
        """
        conn = self._get_conn()
        self._register_views()  # refresh in case new files were written
        return conn.execute(sql)

    async def read_interpretation_history(
        self,
        dataset_id: str,
        limit: int = 10,
    ) -> list[InterpretationResult]:
        """Override to use DuckDB for efficient filtered queries."""
        try:
            conn = self._get_conn()
            self._register_views()
            rows = (
                conn.execute(
                    """
                SELECT * FROM interpretations
                WHERE dataset_id = ?
                ORDER BY run_ts DESC
                LIMIT ?
                """,
                    [dataset_id, limit],
                )
                .fetchdf()
                .to_dict(orient="records")
            )
            return [InterpretationResult.from_dict(r) for r in rows]
        except Exception:
            # fall back to Parquet scan if DuckDB fails (e.g. no files yet)
            return await super().read_interpretation_history(dataset_id, limit)

    def summary(self) -> Any:
        """
        Quick health summary across all datasets.
        Returns a pandas DataFrame.
        """
        return self.query("""
            SELECT
                dataset_id,
                COUNT(*)                                           AS total_runs,
                SUM(CASE WHEN severity = 'alert' THEN 1 ELSE 0 END) AS alerts,
                SUM(CASE WHEN severity = 'warn'  THEN 1 ELSE 0 END) AS warnings,
                MIN(run_ts)                                        AS first_run,
                MAX(run_ts)                                        AS last_run,
                AVG(jaccard_delta)                                 AS avg_jaccard_delta
            FROM interpretations
            GROUP BY dataset_id
            ORDER BY alerts DESC, warnings DESC
        """).df()
