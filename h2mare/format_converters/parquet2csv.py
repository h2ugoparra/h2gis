from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import polars as pl
from loguru import logger

from h2mare.config import settings

# Supress scientific notation in polars
pl.Config.set_fmt_float("full")


def del_temp_folder(temp_path: Path):
    """Delete files from temp folder"""
    if temp_path.exists():
        for file in temp_path.glob("*"):
            file.unlink()


def parquet2csv(
    parquet_root: Path | str,
    csv_root: Path | str,
    start_date: pd.Timestamp | str,
    end_date: pd.Timestamp | str,
    freq: str = "daily",
    n_workers: int = 8,
) -> None:
    """
    Convert parquet data into daily, monthly, or yearly CSV files.

    Parameters
    ----------
    start_date, end_date : str or pd.Timestamp
        Time range to extract.
    parquet_root : Path or str
        Directory or file containing Parquet data.
    csv_root : Path or str
        Output directory for CSVs.
    freq : str
        Aggregation level: "daily", "monthly", or "yearly".
    n_workers : int
        Number of threads for parallel writing.
    """
    parquet_root = Path(parquet_root)
    csv_root = Path(csv_root)

    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)

    logger.info(
        f"Converting parquet to {freq} csv files for the period {start_date} -> {end_date}"
    )

    # missing_columns='insert' insert null values when var is not present in all years
    df_pl = pl.scan_parquet(parquet_root, missing_columns="insert")

    df_pl = df_pl.filter(
        (pl.col("time") >= pd.to_datetime(start_date))
        & (pl.col("time") <= pd.to_datetime(end_date))
    ).with_columns(
        [
            pl.col("time").dt.truncate("1d").alias("time"),  # standardize to date
        ]
    )

    # Remove year and month columns
    col_to_remove = {"year", "month"}
    cols_present = col_to_remove.intersection(df_pl.collect_schema().names())
    if cols_present:
        df_pl = df_pl.drop(list(col_to_remove))

    # Collect (use small batches if needed)
    df = df_pl.collect(engine="streaming")

    # Drop rows with all NaNs except coordinates/time
    df = df.filter(~pl.all_horizontal(pl.exclude(["time", "lat", "lon"]).is_nan()))

    float64_cols = [col for col, dtype in df.schema.items() if dtype == pl.Float64]
    if float64_cols:
        df = df.with_columns([pl.col(col).cast(pl.Float32) for col in float64_cols])

    # Define grouping key based on frequency
    if freq == "daily":
        df = df.with_columns(pl.col("time").dt.strftime("%Y-%m-%d").alias("date_key"))
    elif freq == "monthly":
        df = df.with_columns(pl.col("time").dt.strftime("%Y-%m").alias("date_key"))
    elif freq == "yearly":
        df = df.with_columns(pl.col("time").dt.strftime("%Y").alias("date_key"))
    else:
        raise ValueError("freq must be 'daily', 'monthly', or 'yearly'")

    # Group unique date keys
    date_keys = df.select("date_key").unique().to_series().to_list()

    # Function to write each group
    def write_group(date_key: str) -> None:
        year_dir = csv_root / date_key[:4]
        year_dir.mkdir(parents=True, exist_ok=True)

        out_file = year_dir / f"{date_key}.csv"
        (
            df.filter(pl.col("date_key") == date_key)
            .drop("date_key")
            .with_columns(pl.col("time").dt.strftime("%Y-%m-%d"))
            .write_csv(out_file)
        )

    # Parallel export
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        executor.map(write_group, date_keys)

    logger.success(
        f"Finished exporting {len(date_keys)} {freq} CSV files to {csv_root}"
    )


if __name__ == "__main__":

    log_path = settings.LOGS_DIR / f"{Path(__file__).stem}.log"
    logger.add(log_path, level="INFO")

    parquet_dir = settings.PARQUET_DIR / "h2mare_compiled-data-0.25deg-P1D_80W-10E-0-70N"
    dt_ini, dt_fin = "2023-01-01", "2023-12-31"
    parquet2csv(parquet_dir, settings.EXTERNAL_DIR, dt_ini, dt_fin)

    # Get list of files for the selected year
    # ds1_files = sorted((base_path / f"{year}").glob("*.nc"))

    # Create a directory for the selected year
    # output_path = base_path / file_format
    # output_path.mkdir(parents=True, exist_ok=True)

    # Create a list of tuples, each containing a file path and the year_path
    # args_list = [(file, conf.tmp_path) for file in ds1_files]

    # with WorkerPool(n_jobs=6) as pool:
    #    results = pool.map(
    #        process_csv, args_list,
    #        progress_bar=True
    #        )
    # return [r for r in results if r is not None]

    # if year_agg:
    #        agg_year(base_path, year)
    # else:
    #    year_path = output_path / str(year)
    #    year_path.mkdir(parents=True, exist_ok=True)
    #    move_files_to_folder(conf.tmp_path, year_path, file_extension=file_format)
    #    logger.info(f"Files moved from {conf.tmp_path} to {year_path}")
#
#    return None
#
