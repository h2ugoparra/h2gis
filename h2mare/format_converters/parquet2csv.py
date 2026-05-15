from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Literal

import pandas as pd
import polars as pl
from loguru import logger


def parquet2csv(
    parquet_root: Path | str,
    csv_root: Path | str,
    start_date: pd.Timestamp | str,
    end_date: pd.Timestamp | str,
    freq: Literal["daily", "monthly", "yearly"] = "daily",
    n_workers: int = 8,
) -> None:
    """
    Convert parquet data into daily, monthly, or yearly CSV files.

    Parameters
    ----------
    parquet_root : Path or str
        Directory or file containing Parquet data.
    csv_root : Path or str
        Output directory for CSVs.
    start_date, end_date : str or pd.Timestamp
        Time range to extract.
    freq : str
        Aggregation level: "daily", "monthly", or "yearly".
    n_workers : int
        Number of threads for parallel writing.
    """
    if freq not in ("daily", "monthly", "yearly"):
        raise ValueError("freq must be 'daily', 'monthly', or 'yearly'")

    parquet_root = Path(parquet_root)
    csv_root = Path(csv_root)
    start_dt = pd.Timestamp(start_date).to_pydatetime()
    end_dt = pd.Timestamp(end_date).to_pydatetime()

    fmt = {"daily": "%Y-%m-%d", "monthly": "%Y-%m", "yearly": "%Y"}[freq]

    logger.info(f"Converting parquet to {freq} CSV files: {start_date} -> {end_date}")

    lf = pl.scan_parquet(parquet_root, missing_columns="insert")
    cols_to_drop = [c for c in ("year", "month") if c in lf.collect_schema().names()]

    lf = (
        lf.filter(pl.col("time").is_between(pl.lit(start_dt), pl.lit(end_dt)))
        .with_columns(pl.col("time").dt.truncate("1d"))
        .drop(cols_to_drop)
        .with_columns(pl.col(pl.Float64).cast(pl.Float32))
    )

    df = lf.collect(engine="streaming")
    df = df.filter(~pl.all_horizontal(pl.exclude(["time", "lat", "lon"]).is_nan()))
    df = df.with_columns(pl.col("time").dt.strftime(fmt).alias("date_key"))

    date_keys = df["date_key"].unique().to_list()

    def write_group(date_key: str) -> None:
        year_dir = csv_root / date_key[:4]
        year_dir.mkdir(parents=True, exist_ok=True)
        (
            df.filter(pl.col("date_key") == date_key)
            .drop("date_key")
            .with_columns(pl.col("time").dt.strftime("%Y-%m-%d"))
            .write_csv(year_dir / f"{date_key}.csv")
        )

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        executor.map(write_group, date_keys)

    logger.success(
        f"Finished exporting {len(date_keys)} {freq} CSV files to {csv_root}"
    )
