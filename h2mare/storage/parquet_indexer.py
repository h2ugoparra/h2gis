"""
 Parquet files handling and manipulation.
"""

from __future__ import annotations

from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Optional, Union

if TYPE_CHECKING:
    from h2mare.storage.parquet_plotter import ParquetPlotter

import shutil
from datetime import timedelta

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from loguru import logger

from h2mare.types import BBox, DateRange
from h2mare.utils.datetime_utils import to_datetime

from .parquet_helpers import polars_float64_to_float32

# For parquet writing partition size adn schema
TARGET_FILE_MB = 256
EST_BYTES_PER_ROW = 200
MAX_ROWS_PER_FILE = int((TARGET_FILE_MB * 1024**2) / EST_BYTES_PER_ROW)

PARTITION_SCHEMA = pa.schema(
    [
        pa.field("year", pa.int32(), nullable=False),
        pa.field("month", pa.int32(), nullable=False),
    ]
)


class ParquetIndexer:
    def __init__(
        self,
        parquet_root: str | Path,
        *,
        time_col: str = "time",
        lon_col: str = "lon",
        lat_col: str = "lat",
    ):
        """
        Parquet data indexer.

        Args:
            parquet_root (str | Path): Root directory for parquet data
            time_col (str, optional): Time column name. Defaults to "time".
            lon_col (str, optional): Longitude column name. Defaults to "lon".
            lat_col (str, optional): Latitude column name. Defaults to "lat".

        Raises:
            ValueError: If time, lat, lon cols not in data.
        """
        self.parquet_root = Path(parquet_root)
        self.time_col = time_col
        self.lon_col = lon_col
        self.lat_col = lat_col

        # Set pyarrow partitioning schema and cols name. This avoids adding them to physical schema
        self.partition_schema = PARTITION_SCHEMA
        self.partition_cols = set(self.partition_schema.names)

        # Initialize before any method calls that may reference these attributes
        self.physical_schema = None
        self.physical_cols: set[str] = set()

        # Set metadata
        self._init_dataset_metadata()

        # ---- No data in parquet_root ----
        if not self.parquet_root.exists() or not any(
            self.parquet_root.rglob("*.parquet")
        ):
            logger.warning(f"No data in {self.parquet_root}. Creating directory.")
            self.parquet_root.mkdir(parents=True, exist_ok=True)

        # ---- Data exists ----
        else:
            all_present = set([self.time_col, self.lon_col, self.lat_col]).issubset(
                set(self.get_schema().keys())
            )
            if not all_present:
                raise ValueError(
                    f"{self.time_col}, {self.lon_col} or {self.lat_col} not present in dataset."
                )
            self.physical_schema = self.get_schema()
            self.physical_cols = set(self.physical_schema.keys())

    def __repr__(self) -> str:

        if self.physical_schema is None:
            return ""

        time_cov = self.get_time_coverage()
        bbox = self.get_geoextent()

        return (
            f"ParquetIndexer(\n"
            f"  path={self.parquet_root},\n"
            f"  coverage={time_cov if time_cov is not None else None},\n"
            f"  bbox={bbox.to_label() if bbox is not None else None},\n"
            f"  n_columns={len(self.get_schema().keys())},\n"
            f")"
        )

    # ======================  METADATA ========================
    def _get_partition_years(self) -> list[int]:
        return sorted(
            int(p.name.split("=", 1)[1])
            for p in self.parquet_root.iterdir()
            if p.is_dir() and p.name.startswith("year=")
        )

    def _get_partition_months(self, year: int) -> list[int]:
        year_dir = self.parquet_root / f"year={year}"
        return sorted(
            int(p.name.split("=", 1)[1])
            for p in year_dir.iterdir()
            if p.is_dir() and p.name.startswith("month=")
        )

    def _get_time_coverage(self) -> DateRange:
        """Fast time coverage extraction based on first and last files on parquet_root"""
        years = self._get_partition_years()

        # ---- first file ----
        y0 = years[0]
        m0 = self._get_partition_months(y0)[0]
        first_file = sorted(
            (self.parquet_root / f"year={y0}" / f"month={m0}").rglob("*.parquet")
        )[0]

        lf_min = pl.scan_parquet(first_file).select(pl.col(self.time_col).min())

        # --- latest ---
        y1 = years[-1]
        m1 = self._get_partition_months(y1)[-1]
        last_file = sorted(
            (self.parquet_root / f"year={y1}" / f"month={m1}").rglob("*.parquet")
        )[-1]

        lf_max = pl.scan_parquet(last_file).select(pl.col(self.time_col).max())

        dt_min, dt_max = (r.item() for r in pl.collect_all([lf_min, lf_max]))

        return DateRange(dt_min, dt_max)

    def _init_dataset_metadata(self) -> None:
        """
        Initialize dataset-level metadata from parquet_root.
        Must be called whenever data becomes available.
        """
        if not any(self.parquet_root.rglob("*.parquet")):
            self._time_range = None
            self._geoextent = None
            self._dataset_meta_initialized = False
            return

        self._time_range = self._get_time_coverage()
        assert self._time_range.start <= self._time_range.end

        # Get geoextent from first file
        y0 = self._get_partition_years()[0]
        m0 = self._get_partition_months(y0)[0]

        first_file = next(
            (self.parquet_root / f"year={y0}" / f"month={m0}").rglob("*.parquet")
        )

        scan = pl.scan_parquet(first_file)

        self._geoextent = BBox.from_dataframe(
            scan, lon_col=self.lon_col, lat_col=self.lat_col
        )
        self._dataset_meta_initialized = True

    def _update_physical_schema(self, df: pl.DataFrame) -> None:
        """Updates physycal schema with new varaibles if present

        Args:
            df (pl.DataFrame): input dataframe
        """
        if self.physical_schema is None:
            return

        candidate_cols = set(df.columns) - self.partition_cols
        new_cols = candidate_cols - set(self.physical_schema.keys())
        if not new_cols:
            return

        logger.info(f"Extending physical schema with: {new_cols}")

        for col in new_cols:
            self.physical_schema[col] = df.schema[col]

        self.physical_cols = set(self.physical_schema.keys())

    def _init_physical_schema(self, df: pl.DataFrame) -> None:
        """When no data exists in parquet_root, collects schema from input df.
        Applies float64→float32 conversion to match what _prepare_df will write."""
        self.physical_schema = dict(polars_float64_to_float32(df).schema)
        self.physical_cols = set(self.physical_schema.keys())

    def _align_to_schema(
        self, df: pl.DataFrame, include_partitions: bool = True
    ) -> pl.DataFrame:
        """
        Align dataframe to physical schema, adding missing columns with nulls and reordering.

        Args:
            df (pl.DataFrame): Input dataframe
            include_partitions (bool): Whether to include partition columns in the output. Defaults to True.
        """
        physical_cols = set(self.physical_schema.keys())  # type: ignore
        partition_cols = set(self.partition_cols)

        # Split dataframe
        df_partitions = df.select([c for c in df.columns if c in partition_cols])
        df_physical = df.select([c for c in df.columns if c not in partition_cols])

        # Fails if schema update was skipped
        extra = set(df.columns) - physical_cols - self.partition_cols
        if extra:
            raise RuntimeError(
                f"New columns {extra} detected but physical schema was not updated"
            )

        # Missing physical columns → fill with nulls (exclude partition cols which are handled separately)
        missing = (physical_cols - self.partition_cols) - set(df_physical.columns)
        if missing:
            logger.warning(f"Missing variables in new data: {missing}")
            df_physical = df_physical.with_columns(
                [
                    pl.lit(None).cast(self.physical_schema[col]).alias(col)  # type: ignore
                    for col in missing
                ]
            )

        # Reorder columns to match physical schema (skip partition cols — not present in df_physical)
        df_physical = df_physical.select(
            [
                pl.col(col).cast(dtype)
                for col, dtype in self.physical_schema.items()  # type: ignore
                if col not in self.partition_cols
            ]
        )

        if not include_partitions:
            return df_physical

        # Reattach partition columns
        return pl.concat([df_physical, df_partitions], how="horizontal")

    # ========================  I/O  =========================

    def _resolve_time_col(
        self,
        df: pl.DataFrame,
        time_mode: Literal["date", "datetime"] = "date",
        fmt: str | None = None,
    ) -> pl.DataFrame:
        """Resolve time column type before saving.

        Args:
            df (pl.DataFrame): input dataframe
            time_mode (Literal['date', 'datetime'], optional): 'date' if daily dates (e.g. YYYY-MM-DD) or datetime (e.g. YYYY-MM-DD HH:MM:SS). Defaults to 'date'.
            fmt (str | None, optional): String format to parse time column. Defaults to None (auto-detect in to_datetime function).

        Raises:
            ValueError: `fmt` is only valid when time column is Utf8
            ValueError: time_mode must be 'date' or 'datetime'

        Returns:
            pl.DataFrame: _description_
        """
        dtype = df[self.time_col].dtype
        expr = pl.col(self.time_col)

        if dtype == pl.Utf8:
            if fmt is not None:
                expr = expr.str.to_datetime(format=fmt)
            else:
                expr = expr.cast(pl.Datetime, strict=False)
        else:
            if fmt is not None:
                raise ValueError("`fmt` is only valid when time column is Utf8")

            expr = expr.cast(pl.Datetime, strict=False)

        if time_mode == "date":
            expr = expr.dt.date()
        elif time_mode == "datetime":
            pass
        else:
            raise ValueError("time_mode must be 'date' or 'datetime'")

        return df.with_columns(expr.alias(self.time_col))

    def add_data(
        self,
        df: pl.DataFrame,
        time_mode: Literal["date", "datetime"] = "date",
        fmt: str | None = None,
    ) -> None:
        """
        Add or Replace data in parquet_root

        Args:
            df (pl.DataFrame): New data to be added to parquet dir
            time_mode (Literal['date', 'datetime'], optional): 'date' if daily dates (e.g. YYYY-MM-DD) or datetime (e.g. YYYY-MM-DD HH:MM:SS). Defaults to 'date'.
            fmt (str | None, optional): String format to parse time column. Defaults to None (auto-detect in to_datetime function).
        """
        logger.info(f"Saving partitioned parquet to {self.parquet_root}")

        # Resolve time column
        df = self._resolve_time_col(df, time_mode=time_mode, fmt=fmt)

        first_write = not self._dataset_meta_initialized

        if self.physical_schema is None:
            self._init_physical_schema(df)

        df = self._prepare_df(df)  # Adds year/month partition columns

        if any(self.parquet_root.rglob("*.parquet")):
            is_resolved = self.resolve_dims_overlap(df)
            if is_resolved:
                logger.success("Overlap resolved. Data added.")
                if "plot" in self.__dict__:
                    self.plot.clear_cache()
                return
            else:
                logger.info("Appending non-overlapping data.")
                df = self._align_to_schema(df)
        else:
            logger.info("Creating new parquet dataset.")

        ds.write_dataset(
            df.to_arrow(),
            base_dir=str(self.parquet_root),
            format="parquet",
            partitioning=ds.partitioning(self.partition_schema, flavor="hive"),
            existing_data_behavior="overwrite_or_ignore",
            max_rows_per_file=MAX_ROWS_PER_FILE,
        )

        # get metadata from first write
        if first_write:
            self._init_dataset_metadata()

        # invalidate plot cache if it has been created
        if "plot" in self.__dict__:
            self.plot.clear_cache()

    def resolve_dims_overlap(self, df: pl.DataFrame) -> bool | None:
        """
        Resolves spatial, temporal and column names overlap between existing and new data (df).
        If no temporal or vars overlap, returns None, else merges data and replaces partitions
        atomically to avoid memory issues. Returns True when overlap is resolved.

        Existing partitions are read in one parallel DuckDB query instead of N sequential
        Polars scans, then joined once. The write loop remains per-partition for atomicity.

        Args:
            df (pl.DataFrame): New data to be added to parquet dir

        Raises:
            ValueError: If spatial coordinates of new data are outside of existing data bbox.
        """
        import duckdb

        # ---- metadata from Existing data ----
        store_time_cov = self.get_time_coverage()
        store_bbox = self.get_geoextent()

        # ---- metadata from New data ----
        df_time_cov = DateRange.from_dataframe(df, time_col=self.time_col)
        df_bbox = BBox.from_dataframe(df, lon_col=self.lon_col, lat_col=self.lat_col)
        n_cols = set(df.columns)

        # Check spatial, temporal and vars overlap
        if store_bbox is None or df_bbox is None:
            return None
        if not store_bbox.overlaps(df_bbox):
            raise ValueError(
                "No spatial overlap between existing and new parquet data."
            )

        if store_time_cov is None or df_time_cov is None:
            return None
        if not store_time_cov.overlaps(df_time_cov):
            return None

        new_cols = n_cols - self.physical_cols
        duplicated_cols = self.physical_cols.intersection(n_cols) - {
            self.time_col,
            self.lat_col,
            self.lon_col,
        }

        if not duplicated_cols and not new_cols:
            return None

        self._update_physical_schema(df)

        affected = df.select(["year", "month"]).unique().rows()

        # Separate partitions that already exist from genuinely new ones
        existing_pairs = [
            (y, m)
            for y, m in affected
            if any((self.parquet_root / f"year={y}" / f"month={m}").rglob("*.parquet"))
        ]
        new_pairs = [(y, m) for y, m in affected if (y, m) not in set(existing_pairs)]

        # ---------- READ ALL EXISTING PARTITIONS IN ONE DUCKDB QUERY + JOIN ----------
        if existing_pairs:
            conn = duckdb.connect()

            # Register new data without partition cols (re-derived from time after join)
            conn.register("df_new", df.drop(["year", "month"]))

            # Columns to exclude from existing data: partition cols + cols being replaced
            exclude_cols = {"year", "month"} | duplicated_cols
            exclude_sql = ", ".join(exclude_cols)

            filter_sql = " OR ".join(
                f"(year = {y} AND month = {m})" for y, m in existing_pairs
            )
            # DuckDB requires forward slashes
            parquet_glob = str(
                self.parquet_root / "year=*" / "month=*" / "*.parquet"
            ).replace("\\", "/")
            key_cols = f"{self.time_col}, {self.lon_col}, {self.lat_col}"

            merged = conn.execute(
                f"""
                WITH existing AS (
                    SELECT * EXCLUDE ({exclude_sql})
                    FROM read_parquet('{parquet_glob}', hive_partitioning = true)
                    WHERE {filter_sql}
                )
                SELECT * FROM existing
                FULL OUTER JOIN df_new USING ({key_cols})
            """
            ).pl()

            conn.close()

            # Re-derive partition columns from the merged time column
            merged = merged.with_columns(
                [
                    pl.col(self.time_col).dt.year().cast(pl.Int32).alias("year"),
                    pl.col(self.time_col).dt.month().cast(pl.Int32).alias("month"),
                ]
            )

            for year, month in existing_pairs:
                partition_data = merged.filter(
                    (pl.col("year") == year) & (pl.col("month") == month)
                )
                partition_data = self._align_to_schema(
                    partition_data, include_partitions=False
                )
                self.atomic_partition_write(partition_data, (year, month))

        # ---------- WRITE GENUINELY NEW PARTITIONS DIRECTLY ----------
        for year, month in new_pairs:
            df_write = self._align_to_schema(
                df.filter((pl.col("year") == year) & (pl.col("month") == month)),
                include_partitions=False,
            )
            self.atomic_partition_write(df_write, (year, month))

        return True

    def atomic_partition_write(
        self, df: pl.DataFrame, partition: tuple[int, int]
    ) -> None:
        """
        Atomically replace (i.e. save file with tmp name and rename it after in parquet_dir, avoiding overwrite)
        with year/month Hive-style partition: i.e. year=YYYY/month=MM.
        """
        out = self.parquet_root
        year, month = partition

        final_path = out / f"year={year}" / f"month={month}"

        # ------------ Write into a temp folder ------------
        tmp_path = out / f".tmp_write_{year}_{month}"
        if tmp_path.exists():
            shutil.rmtree(tmp_path)

        tmp_path.mkdir(parents=True, exist_ok=True)

        ds.write_dataset(
            df.to_arrow(),
            base_dir=str(tmp_path),
            format="parquet",
            max_rows_per_file=MAX_ROWS_PER_FILE,
        )
        # Remove existing partition safely
        if final_path.exists():
            shutil.rmtree(final_path, ignore_errors=True)

        # Ensure parent year folder exists
        final_path.parent.mkdir(parents=True, exist_ok=True)

        tmp_path.rename(final_path)

    def _resolve_files(self, dates: Optional[Union[list, tuple]]) -> list[Path]:
        """
        Select parquet files efficiently based on dates if the data is partitioned
        by year/month[/day]. If not partitioned, return all files.
        """
        all_files = sorted(self.parquet_root.rglob("*.parquet"))
        if dates is None:
            return all_files

        # ---- range of dates ----
        if isinstance(dates, tuple) and len(dates) == 2:
            start, end = map(to_datetime, dates)

            # Build the set of (year, month) partitions covered by the range
            valid_partitions: set[tuple[int, int]] = set()
            y, m = start.year, start.month
            while (y, m) <= (end.year, end.month):
                valid_partitions.add((y, m))
                m += 1
                if m > 12:
                    m = 1
                    y += 1

            return [
                f
                for f in all_files
                if any(
                    f"year={y}/month={mo}" in f.as_posix() for y, mo in valid_partitions
                )
            ]

        # ---- Sparse list of dates ----
        elif isinstance(dates, list):
            result: set[Path] = set()

            for d in dates:
                try:
                    dt = to_datetime(d)
                    year, month = dt.year, dt.month

                    patterns = (
                        f"year={year}/month={month}",
                        f"{year}/{month:02d}",
                        f"{year}-{month:02d}",
                    )

                    for pattern in patterns:
                        result.update(self.parquet_root.rglob(f"*{pattern}*/*.parquet"))

                except Exception as e:
                    logger.exception(f"Failed to parse date '{d}': {e}")
                    continue

            return sorted(result) or all_files

        else:
            raise ValueError("`dates` must be list or (start, end) tuple")

    def scan(
        self,
        dates: Optional[Union[list, tuple]] = None,
        bbox: Optional[tuple[float, float, float, float]] = None,
        columns: Optional[str | list[str]] = None,
    ) -> pl.LazyFrame:
        """
        Returns a lazyframe (not loaded) with optional date range, spatial filter, and column subset.

        Parameters
        ----------
        dates : list[str] or (str, str), optional
            Discrete list of dates or (start, end) for range filtering.
        bbox : (xmin, ymin, xmax, ymax), optional
            Spatial subset for lon/lat columns.
        columns : list[str], optional
            Columns to select (in addition to date/lon/lat if needed).
        """
        if self.physical_schema is None:
            raise RuntimeError("No data in parquet store. Call add_data() first.")

        time_col = self.time_col
        lon_col = self.lon_col
        lat_col = self.lat_col

        parquet_files = self._resolve_files(dates)
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found under {self.parquet_root}")

        lf = pl.scan_parquet(parquet_files).with_columns(pl.col(time_col).cast(pl.Date))

        # ---- Filter by date ----
        if dates is not None:
            # Range type: (start, end)
            if isinstance(dates, tuple) and len(dates) == 2:
                start, end = map(to_datetime, dates)
                lf = lf.filter((pl.col(time_col) >= start) & (pl.col(time_col) <= end))

            #  Discrete sample of dates
            elif isinstance(dates, list):
                normalized = [to_datetime(d) for d in dates]
                lf = lf.filter(
                    pl.any_horizontal(
                        [
                            (pl.col(time_col) >= d)
                            & (pl.col(time_col) < d + timedelta(days=1))
                            for d in normalized
                        ]
                    )
                )

            else:
                raise ValueError("`dates` must be list[str] or (start, end) tuple")

        # ---- Filter by bounding box ----
        if bbox is not None:
            xmin, ymin, xmax, ymax = bbox
            lf = lf.filter(
                (pl.col(lon_col) >= xmin)
                & (pl.col(lon_col) <= xmax)
                & (pl.col(lat_col) >= ymin)
                & (pl.col(lat_col) <= ymax)
            )

        # ---- Select columns ----
        if columns:
            columns = [columns] if isinstance(columns, str) else columns
            # Make sure we include columns needed for filters
            mandatory = {time_col, lon_col, lat_col}
            cols = list(mandatory.union(columns))
            existing_cols = [c for c in cols if c in list(self.physical_schema.keys())]
            lf = lf.select(existing_cols)

        return lf

    def load(
        self,
        dates: Optional[Union[list, tuple]] = None,
        bbox: Optional[tuple[float, float, float, float]] = None,
        columns: Optional[str | list[str]] = None,
    ) -> pl.DataFrame:
        """
        Returns a loaded dataframe.

        Args:
            dates (Optional[Union[list, tuple]], optional): Discrete list of dates or (start, end) for range filtering. Defaults to None, using the whole data.
            bbox (Optional[tuple[float, float, float, float]], optional): Spatial subset for lon/lat columns. Defaults to None, using the whole data.
            columns (Optional[list[str]], optional): Columns to select (in addition to time/lon/lat). Defaults to None, using the whole data.

        Returns:
            pl.DataFrame: Loaded dataframe
        """
        return self.scan(dates=dates, bbox=bbox, columns=columns).collect()

    # ========================= HELPERS =======================

    def get_time_coverage(self) -> DateRange | None:
        """Get time range from a parquet data."""
        if not self._dataset_meta_initialized:
            raise RuntimeError("Dataset metadata not initialized yet")
        return self._time_range

    def get_geoextent(self) -> BBox | None:
        """Get geographical extent from parquet data."""
        if not self._dataset_meta_initialized:
            raise RuntimeError("Dataset metadata not initialized yet")
        return self._geoextent

    def get_schema(self) -> dict[str, pl.DataType]:
        """Get schema (columns names and dtypes).
        Returns cached physical_schema when available, otherwise reads from the first parquet file.
        """
        if self.physical_schema is not None:
            return self.physical_schema
        first_file = next(self.parquet_root.rglob("*.parquet"))
        return pl.read_parquet_schema(first_file)

    def _prepare_df(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Add year and month to df for 'hive' partitioning. Convert columns float64 to float32 (for better storage)
        """
        return df.with_columns(
            [
                pl.col(self.time_col).dt.year().cast(pl.Int32).alias("year"),
                pl.col(self.time_col).dt.month().cast(pl.Int32).alias("month"),
            ]
        ).pipe(polars_float64_to_float32)

    # ----------------------------------------
    #                  PLOTS
    # ----------------------------------------
    @cached_property
    def plot(self) -> "ParquetPlotter":
        """Visualization accessor. Use ``indexer.plot.time_series(...)`` or ``indexer.plot.spatial_maps(...)``."""
        from h2mare.storage.parquet_plotter import ParquetPlotter

        return ParquetPlotter(self)
