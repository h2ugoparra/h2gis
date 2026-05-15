# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install / sync dependencies (creates .venv and installs project in editable mode)
uv sync
uv sync --dev   # include dev dependencies (pytest, ruff)

# Run the pipeline CLI
uv run h2mare run -v sst --start-date 2021-01-01 --end-date 2021-12-31
uv run h2mare run -v sst                    # infers dates from existing store
uv run h2mare run -v sst -v ssh             # multiple variables
uv run h2mare run -v sst --no-convert       # download only, skip Zarr conversion
uv run h2mare run -v sst --no-compile       # skip compile step
uv run h2mare run -v sst --dry-run          # validate without downloading
uv run h2mare run                           # process all configured variables

# Compile h2ds dataset (merge per-variable Zarr stores into unified dataset)
uv run h2mare compile                       # all variables, inferred dates
uv run h2mare compile -v sst -v ssh --start-date 2024-01-01 --end-date 2024-12-31

# Convert already-downloaded files to Zarr (no download)
uv run h2mare convert -v sst -v ssh
uv run h2mare convert -v sst --in-dir /data/raw/CMEMS_SST  # custom input dir

# Inspect Zarr catalog metadata
uv run h2mare catalog sst                   # coverage, file count, variables
uv run h2mare catalog --all                 # all configured variables
uv run h2mare catalog sst --rows            # show individual catalog rows

# Run tests
uv run pytest tests/
uv run pytest tests/test_types.py          # run a single test file

# Format / lint
uv run ruff format h2mare/
uv run ruff check h2mare/
uv run ruff check --fix h2mare/

# Add / remove dependencies
uv add <package>
uv remove <package>
```

## Architecture

The project is a five-stage pipeline: **Download → Convert → Compile → Index → Visualize**

```
CLI (h2mare/cli/main.py)
  └── PipelineManager (pipeline_manager.py)
        ├── Downloader (downloader/)          → raw NetCDF/GRIB files
        ├── Netcdf2Zarr (format_converters/)  → regridded Zarr at 0.25°/daily
        ├── Compiler (processing/compiler.py) → unified h2ds Zarr dataset
        ├── Zarr2Parquet (format_converters/) → columnar Parquet store
        └── parquet2csv (format_converters/)  → optional CSV export
```

**Download** — Three downloader classes (`CMEMSDownloader`, `AVISODownloader`, `CDSDownloader`) fetch raw files from their respective APIs into `data/raw/downloads/`. Configuration per variable lives in `config.yaml` under each variable key (dataset IDs, bounding boxes, depth ranges, file naming patterns).

**Convert** — `Netcdf2Zarr` reads raw files, regrids to 0.25° × 0.25° and interpolates to daily resolution, then writes Zarr stores to `$STORE_ROOT/<local_folder>/`. `ZarrCatalog` in `h2mare/storage/zarr_catalog.py` tracks what has been processed via a Parquet index so partial runs can resume.

**Compile** — `Compiler` (`h2mare/processing/compiler.py`) merges per-variable Zarr stores into a unified h2ds dataset at a common 0.25° daily grid. Per-source preprocessing is delegated to `h2mare/processing/core/{cmems,aviso,cds,fronts}.py`.

**Extract** — `Extractor` (`h2mare/processing/extractor.py`) reads Zarr stores and extracts time series at point locations (CSV) or geometries (SHP) using concurrent `ThreadPoolExecutor` workers.

**Index / Visualize** — `ParquetIndexer` (`h2mare/storage/parquet_indexer.py`) manages a Hive-partitioned (year/month) Parquet store. It handles atomic writes, overlap resolution via DuckDB, and lazy scanning with spatial/temporal filters. Accessed via `ParquetPlotter` (`h2mare/storage/parquet_plotter.py`) through `indexer.plot` for interactive time-series and climatological spatial maps.

**Export** — `Zarr2Parquet` (`h2mare/format_converters/zarr2parquet.py`) converts yearly Zarr stores to Parquet using `ParquetIndexer.add_data()`. `parquet2csv` (`h2mare/format_converters/parquet2csv.py`) exports Parquet data to daily/monthly/yearly CSV files with parallel `ThreadPoolExecutor` writes.

## Configuration

**`config.yaml`** — Central configuration for all variables. Each key defines:

- `source`: data provider (`cmems`, `aviso`, `cds`)
- Dataset IDs for reprocessed (`rep`) and near-real-time (`nrt`) versions
- `local_folder`: subdirectory under `$STORE_ROOT` where Zarr is written
- Variable lists, bounding boxes, depth ranges, and file regex patterns

**`.env`** — Must define `STORE_ROOT` (path to external storage for Zarr output). Also used for AVISO FTP credentials.

**`h2mare/config.py`** — `Settings` class (lazy-loaded singleton) manages path resolution and exposes the parsed `AppConfig`.

**`h2mare/models.py`** — `AppConfig`, `VariablesConfig`, `KeyVarConfigEntry` are `msgspec.Struct` types that represent the parsed config.

## Key Types

All in `h2mare/types.py`:

- `DateLike = str | pd.Timestamp | datetime | date`
- `TimeResolution` — `YEAR` or `MONTH` enum, used to split processing ranges
- `DateRange` — dataclass with `start`/`end` datetime fields; supports `overlaps()`, `intersection()`, `from_dataframe()` constructors
- `BBox` — dataclass with `xmin, ymin, xmax, ymax`; supports `overlaps()`, `contains()`, `from_dataframe()` constructors
- `DownloadTask` — dataclass with `dataset_id`, `date_range`, `dataset_type` (`"rep"` or `"nrt"`)

## ParquetIndexer

`ParquetIndexer` (`h2mare/storage/parquet_indexer.py`) is the primary interface for the Parquet data store. It manages a Hive-partitioned dataset (year/month) and is used both internally (by `Zarr2Parquet`) and externally (for analysis).

```python
from h2mare.storage.parquet_indexer import ParquetIndexer

idx = ParquetIndexer("path/to/parquet_root")

# Write
idx.add_data(df)                             # adds or replaces data; resolves overlap via DuckDB

# Read
lf = idx.scan(dates=("2021-01-01", "2021-12-31"), bbox=(-10, 30, 20, 50))  # LazyFrame
df = idx.load(dates=["2021-06-01", "2021-07-01"])                           # DataFrame

# Metadata
idx.get_schema()           # {col: dtype}
idx.get_time_coverage()    # DateRange
idx.get_geoextent()        # BBox

# Visualization
idx.plot.time_series("sst", agg_by="month")
idx.plot.spatial_maps("sst", agg_by="season")
```

Key behaviors:
- Partition writes are **atomic** (write to `.tmp_write_YYYY_MM`, then rename).
- `resolve_dims_overlap()` handles spatial/temporal/column overlap using a single DuckDB `FULL OUTER JOIN` across all affected partitions, then writes each partition back atomically.
- Float64 columns are downcast to Float32 on write to reduce storage.
- `indexer.plot` is a `cached_property` returning a `ParquetPlotter`; the cache is invalidated after `add_data()`.

## Data Storage Layout

```
$BASE_DIR/
├── data/raw/downloads/     # Raw NetCDF/GRIB files from downloaders
├── data/interim/           # Intermediate processing artifacts
└── data/processed/
    ├── parquet/            # Hive-partitioned Parquet store (ParquetIndexer)
    │   └── year=YYYY/month=MM/*.parquet
    └── metadata/           # Catalog Parquet indices (ZarrCatalog)

$STORE_ROOT/
└── <var_config.local_folder>/
    └── *.zarr              # Processed 0.25° daily Zarr stores
```

## Variable Keys

Configured variables: `sst`, `ssh`, `mld`, `chl`, `seapodym`, `o2`, `fsle`, `eddies`, `atm-instante`, `atm-accum-avg`, `radiation`, `waves`.
