# Architecture

H2GIS is a three-stage pipeline: **Download → Convert → Compile**, with an optional **Extract** step for point or geometry outputs.

---

## Pipeline overview

```
CLI (h2gis/cli/main.py)
  └── PipelineManager (pipeline_manager.py)
        ├── Downloader          → raw NetCDF / GRIB  →  data/raw/downloads/
        ├── Netcdf2Zarr         → regridded Zarr      →  $STORE_DIR/<local_folder>/
        └── Compiler            → unified h2ds Zarr   →  $STORE_DIR/h2ds/

ZarrCatalog (storage/zarr_catalog.py)
  └── Parquet index per variable  →  data/processed/metadata/

Extractor (processing/extractor.py)
  └── Point / geometry extraction →  data/processed/parquet/
```

---

## Stage 1 — Download

**Classes:** `CMEMSDownloader`, `AVISODownloader`, `CDSDownloader`  
**Output:** raw NetCDF or GRIB files in `data/raw/downloads/<local_folder>/`

Each downloader resolves the date range to fetch (explicit or inferred from the existing store), splits it into yearly or monthly tasks (`DownloadTask`), and calls the provider API. For CMEMS variables the downloader automatically switches from the reprocessed (`rep`) dataset to the near-real-time (`nrt`) dataset at the appropriate date boundary.

---

## Stage 2 — Convert

**Class:** `Netcdf2Zarr` (`format_converters/netcdf2zarr.py`)  
**Output:** Zarr stores in `$STORE_DIR/<local_folder>/`

Raw files are opened with xarray, regridded to a daily time axis, and written (or appended) as chunked Zarr stores. `ZarrCatalog` updates its Parquet index after each write so subsequent runs can resume from where they left off.

---

## Stage 3 — Compile

**Class:** `Compiler` (`processing/compiler.py`)  
**Output:** unified `h2ds` Zarr in `$STORE_DIR/h2ds/`

All per-variable Zarr stores are opened, interpolated to the common 0.25° × 0.25° daily grid defined in `config.yaml`, and merged into a single dataset. Variables without data for a given period are skipped gracefully. The compiled dataset is also synced to a local copy for fast access.

Special variables handled outside the general path:
- **`bathy`** — read from a static NetCDF file, no time dimension
- **`moon`** — computed on the fly from the `ephem` library
- **`o2`** — depth-sliced before interpolation

---

## ZarrCatalog

`ZarrCatalog` maintains a Parquet index for each variable key. It tracks:

- File paths, modification times, and scan timestamps
- Temporal coverage (`start_date`, `end_date`) and provenance per source dataset
- Spatial extent and variable names

The catalog is used by `open_dataset` for efficient range queries without opening every Zarr file. It auto-detects stale entries by comparing disk file names and modification times against the index on each cold-start load.

---

## Extractor

`Extractor` reads h2ds Zarr stores and extracts time series at:

- **Point locations** — from a CSV file with `lat`/`lon` columns
- **Geometries** — from a SHP file (polygons or lines)

Extraction is parallelised with `ThreadPoolExecutor`. Output is written as Parquet files under `data/processed/parquet/`.

---

## Key types

| Type | Description |
|---|---|
| `DateRange` | Dataclass with `start` / `end` datetime fields and overlap helpers |
| `BBox` | Dataclass with `xmin, ymin, xmax, ymax`; spatial overlap and label helpers |
| `DownloadTask` | Single download unit: `dataset_id`, `date_range`, `dataset_type` |
| `TimeResolution` | `YEAR` or `MONTH` enum controlling Zarr file granularity |
