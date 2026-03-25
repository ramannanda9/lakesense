"""
Abstract Sketch Provider interface.

Providers are responsible for taking a dataframe (local or distributed)
and computing its sketch signatures (Theta, HLL, KLL, Profile) in an
engine-specific manner.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from lakesense.sketches.compute import SketchRecord


class SketchProvider(ABC):
    """
    Abstract interface for computing dataset sketches.
    Each engine (Streaming, Pandas, Spark) MUST implement this interface to generate
    SketchRecords compliant with lakesense exact binary formats.
    """

    @abstractmethod
    def sketch(
        self,
        data: Any,
        dataset_id: str,
        job_id: str,
        id_columns: list[str] | None = None,
        numeric_columns: list[str] | None = None,
        text_columns: list[str] | None = None,
        run_ts: datetime | None = None,
        include_profiles: bool = True,
    ) -> list[SketchRecord]:
        """
        Execute the sketch computation profiling across the provided dataframe.

        Args:
            data: The input dataframe or iterable mapping, dependent on the Provider wrapper.
            dataset_id: Logical grouping of the dataset.
            job_id: The specific orchestration run producing this profile.
            id_columns: List of columns mapped to Theta/HLL sketches.
            text_columns: List of columns mapped to Theta/HLL sketches.
            run_ts: Observation timestamp (defaults to UTC now).
            include_profiles: Whether to also calculate exact univariate scalar profiles.

        Returns:
            List of binary-serialized SketchRecords.
        """
        pass
