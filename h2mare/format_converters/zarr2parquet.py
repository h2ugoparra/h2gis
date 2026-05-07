"""
Convert yearly zarr to parquet files - Only used for h2ds

"""

from __future__ import annotations

import gc
from pathlib import Path

import pandas as pd
import polars as pl
from loguru import logger

from h2mare.storage import ZarrCatalog
from h2mare.storage.coverage import split_time_range
from h2mare.storage.parquet_indexer import ParquetIndexer
from h2mare.types import DateRange, TimeResolution
from h2mare.utils.datetime_utils import more_than_one_year


class Zarr2Parquet:
    """Convert yearly zarr to parquet files

    Args:
        ZarrCatalog (_type_): repository to get zarr data
    """

    def __init__(self, var_key: str, parquet_root: Path | str):
        """
        Running Zarr2Parquet() it checks data range available in h2ds, compares to existing parquet files and convert what is missing.

        Args:
            parquet_root (Path | str, optional): Folder with parquet files.
        """
        self.var_key = var_key
        self.parquet_root = Path(parquet_root)

        # ---- Get ZARR repository info ----
        self.zarr_repo = ZarrCatalog(self.var_key)
        repo_dates = self.zarr_repo.get_time_coverage()
        if repo_dates:
            self.repo_start, self.repo_end = repo_dates.start, repo_dates.end

        self.indexer = ParquetIndexer(self.parquet_root)

    # ------------------------------------
    # Main processing
    # -------------------------------------
    def process(
        self,
        start_date: str | pd.Timestamp,
        end_date: str | pd.Timestamp,
        time_resolution: TimeResolution = TimeResolution.MONTH,
    ):
        """
        Convert zarr data to parquet for the specified date range.
        If no dates provided, uses missing range from parquet folder.

        Args:
            start_date, end_date (str | pd.Timestamp): Start and end date to process files. If None, it get's dates from existing files
            time_resolution (Literal['monthly', 'yearly'], optional): Frequency to split writing process. Defaults to 'monthly'.

        Raises:
            ValueError: If repo data is not available for the specified dates.
        """
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)

        date_range = DateRange(start_date, end_date)

        if more_than_one_year(start_date, end_date):
            splited_periods = split_time_range(date_range, time_resolution)
        else:
            splited_periods = [DateRange(start_date, end_date)]

        for periods in splited_periods:
            dt_ini, dt_end = periods.start, periods.end
            logger.info(
                f"Converting var_key {self.var_key} zarr data to parquet for the period: {dt_ini} -> {dt_end}"
            )

            # Load dataset
            try:
                ds = self.zarr_repo.open_dataset(start_date=dt_ini, end_date=dt_end)
                if not ds:
                    raise ValueError(
                        f"No {self.var_key} dataset found for time range {dt_ini} -> {dt_end}"
                    )

                ddf_new = ds.to_dataframe().reset_index()
                ds.close()

                ddf_new = pl.from_pandas(ddf_new)
                self.indexer.add_data(ddf_new)
            except Exception as e:
                logger.error(
                    f"Error processing {self.var_key} data for period {dt_ini} -> {dt_end}: {e}"
                )
                continue

            finally:
                del ddf_new
                gc.collect()
