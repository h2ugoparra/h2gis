# Project: h2mare

## Project Overview

A pipeline for downloading and preprocessing multi-source oceanographic and atmospheric data into analysis-ready formats.

## Tech Stack

Python 3.11+. Key libraries: `xarray`/`dask` (lazy N-D arrays), `zarr` (chunked store), `polars`/`pyarrow`/`duckdb` (columnar data), `geopandas`/`rioxarray`/`cartopy` (geospatial), `copernicusmarine`/`cdsapi` (data sources), `typer` (CLI), `msgspec` (config), `plotly`/`matplotlib` (viz). Dev: `uv`, `ruff`, `pytest`, `tox`.

## Commands

```bash
# Install / sync dependencies
uv sync
uv sync --dev   # include dev dependencies (pytest, ruff)

# Run the pipeline
uv run h2mare run -v sst --start-date 2021-01-01 --end-date 2021-12-31
uv run h2mare run -v sst -v ssh     # multiple variables; omit dates to infer from store
uv run h2mare run -v sst --no-convert --no-compile --dry-run

# Other CLI commands
uv run h2mare compile -v sst -v ssh --start-date 2024-01-01 --end-date 2024-12-31
uv run h2mare convert -v sst
uv run h2mare catalog sst

# Tests
uv run pytest tests/
uv run pytest tests/test_types.py
uv run pytest tests/ -k "test_name"

# Lint / format
uv run ruff check h2mare/
uv run ruff check --fix h2mare/
uv run ruff format h2mare/
```

## Architecture

Pipeline: **Download -> Convert -> Compile -> Index/Visualize**

```text
CLI (h2mare/cli/main.py)
  └── PipelineManager (pipeline_manager.py)
        ├── Downloader (downloader/)               -> raw NetCDF/GRIB -> data/raw/downloads/
        ├── Netcdf2Zarr (format_converters/)        -> regridded Zarr at 0.25deg/daily -> $STORE_ROOT
        └── Compiler (processing/compiler.py)       -> unified h2ds Zarr (all vars merged)

Storage & Analysis:
  ├── ZarrCatalog (storage/zarr_catalog.py)         -> tracks processed files; enables resume on partial runs
  ├── ParquetIndexer (storage/parquet_indexer.py)   -> write, scan, and load Parquet data
  └── ParquetPlotter (storage/parquet_plotter.py)   -> interactive time-series and spatial maps

Standalone tools:
  ├── Zarr2Parquet (format_converters/)             -> Hive-partitioned Parquet store
  ├── parquet2csv (format_converters/)              -> optional CSV export
  └── Extractor (processing/extractor.py)           -> point/geometry extraction from Zarr
```

Per-variable preprocessing during Convert is registered in `processing/registry.py` (`var_key -> fn`); unregistered variables pass through unchanged. `Extractor` is a standalone analysis tool outside the pipeline.

## ParquetIndexer

Primary interface for reading and writing the Parquet store (`storage/parquet_indexer.py`).

```python
from h2mare.storage.parquet_indexer import ParquetIndexer

idx = ParquetIndexer("path/to/parquet_root")

idx.add_data(df)                                                             # write; resolves overlap via DuckDB
lf = idx.scan(dates=("2021-01-01", "2021-12-31"), bbox=(-10, 30, 20, 50))  # LazyFrame
df = idx.load(dates=["2021-06-01", "2021-07-01"])                           # DataFrame
idx.get_schema()           # {col: dtype}
idx.get_time_coverage()    # DateRange
idx.get_geoextent()        # BBox
idx.plot.time_series("sst", agg_by="month")
idx.plot.spatial_maps("sst", agg_by="season")
```

Partition writes are atomic (`.tmp_write_YYYY_MM` -> rename). Float64 downcast to Float32 on write. `indexer.plot` is a `cached_property` invalidated after `add_data()`.

## Coding Rules

- **Logging** — use `loguru` (`from loguru import logger`), not stdlib `logging`
- **Paths** — always access paths via `settings.*`; never hardcode
- **`.env`** — `STORE_ROOT` (required); `AVISO_FTP_SERVER`, `AVISO_USERNAME`, `AVISO_PASSWORD` (required for AVISO variables); `H2MARE_ROOT` (optional, overrides project root detection)
- **Types** — use `DateRange`, `BBox`, `DateLike` from `h2mare/types.py` for spatial/temporal boundaries; don't use raw tuples. Import them at module level — don't require callers to construct these objects just to pass an argument to a public function
