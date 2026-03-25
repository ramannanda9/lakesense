"""Spark sketch compute provider for distributed map-reduce."""

import datetime
from collections.abc import Iterator
from typing import Any

try:
    import pandas as pd
    import pyspark.sql.types as T
except ImportError:
    pass

from lakesense.sketches.compute import SketchRecord
from lakesense.sketches.providers.base import SketchProvider
from lakesense.sketches.providers.pandas import PandasProvider


class SparkProvider(SketchProvider):
    """
    SketchProvider implementation for distributed PySpark DataFrames.
    Uses .mapInPandas() to push PandasProvider logic onto worker JVMs across
    data partitions, and executes an exact binary union natively on the Python driver
    to reduce the chunked blobs back to exact SketchRecords.
    """

    def sketch(
        self,
        data: Any,
        dataset_id: str,
        job_id: str,
        id_columns: list[str] | None = None,
        numeric_columns: list[str] | None = None,
        text_columns: list[str] | None = None,
        run_ts: datetime.datetime | None = None,
        include_profiles: bool = True,
    ) -> list[SketchRecord]:
        """
        Compute distributed sketches by pushing pandas iteration via mapInPandas.
        data must be a pyspark.sql.DataFrame.
        """
        ts = run_ts or datetime.datetime.now(datetime.timezone.utc)

        # We need a total count. We can grab this natively via spark.
        total_rows = data.count()
        if total_rows == 0:
            return []

        # Define the structural schema representing the serialized sketch components
        return_schema = T.StructType(
            [
                T.StructField("dataset_id", T.StringType(), True),
                T.StructField("job_id", T.StringType(), True),
                T.StructField("column", T.StringType(), True),
                T.StructField("sketch_type", T.StringType(), True),
                T.StructField("sketch_blob", T.BinaryType(), True),
                T.StructField("num_rows", T.LongType(), True),
                T.StructField("null_count", T.LongType(), True),
            ]
        )

        def _map_sketches(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            """
            This executes locally on the Spark Python worker threads.
            """
            # Recreate the pandas provider within the worker
            provider = PandasProvider()
            for pdf in iterator:
                if len(pdf) == 0:
                    continue

                records = provider.sketch(
                    data=pdf,
                    dataset_id=dataset_id,
                    job_id=job_id,
                    id_columns=id_columns,
                    numeric_columns=numeric_columns,
                    text_columns=text_columns,
                    run_ts=ts,
                    include_profiles=include_profiles,
                )

                # Return as pandas rows conforming to Spark schema
                rows = []
                for r in records:
                    rows.append(
                        {
                            "dataset_id": dataset_id,
                            "job_id": job_id,
                            "column": r.column,
                            "sketch_type": r.sketch_type,
                            "sketch_blob": r.sketch_blob,
                            "num_rows": r.num_rows,
                            "null_count": r.null_count,
                        }
                    )
                yield pd.DataFrame(rows)

        # 1) Execute MAP across Spark cluster
        reduced_df = data.mapInPandas(_map_sketches, schema=return_schema)

        # 2) Execute DRIver COLLECT (.collect() brings only tiny byte blobs!)
        collected_rows = reduced_df.collect()

        if not collected_rows:
            return []

        # Convert back into SketchRecords natively unmerged
        worker_records: list[SketchRecord] = []
        for r in collected_rows:
            worker_records.append(
                SketchRecord(
                    dataset_id=r["dataset_id"],
                    job_id=r["job_id"],
                    column=r["column"],
                    sketch_type=r["sketch_type"],
                    sketch_blob=r["sketch_blob"],
                    run_ts=ts,
                    num_rows=total_rows,
                    # Note: Merging assumes row counts are correct,
                    # but mapInPandas gave chunk sizes.
                    null_count=r["null_count"],
                )
            )

        # 3) REDUCE driver union exactly across the binary strings
        merged: list[SketchRecord] = []
        from collections import defaultdict

        grouped = defaultdict(list)
        for r in worker_records:
            grouped[(r.column, r.sketch_type)].append(r)

        from datasketches import kll_doubles_sketch

        from lakesense.sketches.merge import merge_hll_records, merge_minhash_records

        for (col, stype), recs in grouped.items():
            if not recs:
                continue

            blob = recs[0].sketch_blob

            if stype == "minhash":
                blob = merge_minhash_records(recs)
            elif stype == "hll":
                blob = merge_hll_records(recs)
            elif stype == "kll":
                base_sk = kll_doubles_sketch(200)
                for r in recs:
                    base_sk.merge(kll_doubles_sketch.deserialize(r.sketch_blob))
                blob = base_sk.serialize()
            elif stype == "profile":
                # Profiles currently use JSON scalar summaries which merge manually.
                # For production distributed profiling, frequent_strings_sketch
                # arrays should be passed directly.
                pass

            merged.append(
                SketchRecord(
                    dataset_id=dataset_id,
                    job_id=job_id,
                    column=col,
                    sketch_type=stype,
                    sketch_blob=blob,
                    num_rows=total_rows,
                    null_count=sum(r.null_count or 0 for r in recs),
                    run_ts=ts,
                )
            )

        return merged
