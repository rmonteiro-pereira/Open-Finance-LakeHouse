"""Pandera contracts for the lakehouse, validated on Polars frames at ingest time."""

from __future__ import annotations

import pandera.polars as pa
import polars as pl


#: Structural contract: dtypes, non-null keys, one row per (series_id, date).
#: Extra lineage columns (source, ingested_at, load_id) are allowed.
observation_schema = pa.DataFrameSchema(
    {
        "series_id": pa.Column(pl.String, nullable=False),
        "date": pa.Column(pl.Date, nullable=False),
        "value": pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
    unique=["series_id", "date"],
)


def validate_observation(df: pl.DataFrame, *, max_value: float | None = None) -> pl.DataFrame:
    """Validate ``df`` against the observation contract; raises on violation.

    Structure (dtype/null/uniqueness) is enforced by Pandera; the optional
    per-series upper bound is a direct Polars check.
    """
    observation_schema.validate(df)
    if max_value is not None:
        breaches = df.filter(pl.col("value") > max_value).height
        if breaches:
            raise ValueError(f"{breaches} value(s) exceed the max bound {max_value}")
    return df
