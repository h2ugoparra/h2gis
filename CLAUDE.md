# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install / sync dependencies (creates .venv and installs project in editable mode)
uv sync
uv sync --dev   # include dev dependencies (pytest, black, isort)

# Run the pipeline CLI
uv run h2mare run -v sst --start-date 2021-01-01 --end-date 2021-12-31
uv run h2mare run -v sst                    # infers dates from existing store
uv run h2mare run -v sst -v ssh             # multiple variables
uv run h2mare run -v sst --no-convert       # download only, skip Zarr conversion
uv run h2mare run -v sst --no-compile       # skip compile step
uv run h2mare run -v sst --dry-run          # validate without downloading
uv run h2mare run                           # process all configured variables

# Compile h2ds dataset
uv run h2mare compile                       # all variables, inferred dates
uv run h2mare compile -v sst -v ssh --start-date 2024-01-01 --end-date 2024-12-31

# Convert already-downloaded files to Zarr (no download)
uv run h2mare convert -v sst -v ssh

# Run tests
uv run pytest tests/
uv run pytest tests/test_types.py          # run a single test file

# Format
uv run black h2mare/
uv run isort h2mare/

# Add / remove dependencies
uv add <package>
uv remove <package>
```

## Architecture

The project is a three-stage pipeline: **Download → Convert → Extract**

```
CLI (h2mare/cli/main.py)
  └── PipelineManager (pipeline_manager.py)
        ├── Downloader (downloader/)       → raw NetCDF/GRIB files
        └── Netcdf2Zarr (format_converters/) → regridded Zarr at 0.25°/daily
```

**Download** — Three downloader classes (`CMEMSDownloader`, `AVISODownloader`, `CDSDownloader`) fetch raw files from their respective APIs into `data/raw/downloads/`. Configuration per variable lives in `config.yaml` under each variable key (dataset IDs, bounding boxes, depth ranges, file naming patterns).

**Convert** — `Netcdf2Zarr` reads raw files, regrids to 0.25° × 0.25° and interpolates to daily resolution, then writes Zarr stores to `$STORE_DIR/<local_folder>/`. `ZarrCatalog` in `h2mare/storage/zarr_catalog.py` tracks what has been processed via a Parquet index so partial runs can resume.

**Extract** — `Extractor` (`h2mare/processing/extractor.py`) reads Zarr stores and extracts time series at point locations (CSV) or geometries (SHP) using concurrent `ThreadPoolExecutor` workers.

`Compiler` (`h2mare/processing/compiler.py`) handles regridding and per-source preprocessing details that are delegated to `h2mare/processing/core/{cmems,aviso,cds,fronts}.py`.

## Configuration

**`config.yaml`** — Central configuration for all variables. Each key defines:

- `source`: data provider (`cmems`, `aviso`, `cds`)
- Dataset IDs for reprocessed (`rep`) and near-real-time (`nrt`) versions
- `local_folder`: subdirectory under `$STORE_DIR` where Zarr is written
- Variable lists, bounding boxes, depth ranges, and file regex patterns

**`.env`** — Must define `STORE_DIR` (path to external storage for Zarr output). Also used for AVISO FTP credentials.

**`h2mare/config.py`** — `Settings` class (lazy-loaded singleton) manages path resolution and exposes the parsed `AppConfig`.

**`h2mare/models.py`** — `AppConfig`, `VariablesConfig`, `KeyVarConfigEntry` are `msgspec.Struct` types that represent the parsed config.

## Key Types

- `DateLike = str | pd.Timestamp | datetime | date`
- `TimeResolution` — `YEAR` or `MONTH` enum
- `DateRange` — dataclass with `start`/`end` datetime fields
- `BBox` — dataclass with `xmin, ymin, xmax, ymax`
- `DownloadTask` — dataclass with `dataset_id`, `date_range`, `dataset_type` (`"rep"` or `"nrt"`)

## Data Storage Layout

```
$BASE_DIR/
├── data/raw/downloads/     # Raw NetCDF/GRIB files from downloaders
├── data/interim/           # Intermediate processing artifacts
└── data/processed/
    ├── parquet/            # Extracted point/geometry time series
    └── metadata/           # Catalog Parquet indices (ZarrCatalog)

$STORE_DIR/
└── <var_config.local_folder>/
    └── *.zarr              # Processed 0.25° daily Zarr stores
```

## Variable Keys

Configured variables: `sst`, `ssh`, `mld`, `chl`, `seapodym`, `o2`, `fsle`, `eddies`, `atm-instante`, `atm-accum-avg`, `radiation`, `waves`.
