# Configuration

H2GIS is configured through two files: `config.yaml` (variable definitions and processing parameters) and `.env` (paths and credentials).

---

## config.yaml

### Spatial parameters

```yaml
spatial:
  geo_extent: [-80, 0, 10, 70]   # [xmin, ymin, xmax, ymax] in degrees
  depth_range: [0, 1000]         # depth range for O2 variables (metres)
  dx: 0.25                       # output grid resolution (degrees)
  dy: 0.25
```

### Variable entries

Each key under `variables:` defines one data stream:

```yaml
variables:
  sst:
    local_folder: CMEMS_SST           # subdirectory under STORE_DIR
    variables: [analysed_sst, ...]    # variable names inside the source file
    dataset_id_rep: <cmems-id>        # reprocessed (multiyear) dataset ID
    dataset_id_nrt: <cmems-id>        # near-real-time dataset ID (optional)
    source: cmems                     # cmems | aviso | cds
    pattern: "(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})"  # filename date pattern
    subset: true                      # spatial subset on download
    bbox: [-80, 0, 10, 70]           # [xmin, ymin, xmax, ymax]
```

| Field | Required | Description |
|---|---|---|
| `local_folder` | yes | Subdirectory under `STORE_DIR` for this variable's Zarr files |
| `variables` | yes | Variable names to extract from source files |
| `dataset_id_rep` | yes | Reprocessed dataset identifier |
| `dataset_id_nrt` | no | Near-real-time dataset identifier. Omit for reanalysis-only products |
| `source` | yes | Provider: `cmems`, `aviso`, or `cds` |
| `pattern` | yes | Regex for extracting date ranges from raw filenames |
| `subset` | no | Whether to spatially subset on download (default `false`) |
| `bbox` | no | Bounding box for subset. Falls back to `spatial.geo_extent` |
| `depth_range` | no | Depth range for 3D variables (e.g. `o2`) |

### The `h2ds` key

The special `h2ds` variable defines the output grid for the compile step:

```yaml
  h2ds:
    local_folder: h2ds
    dataset_id_rep: compiled-data-0.25deg-P1D
    source: h2mare
    bbox: [-80, 0, 10, 70]
```

The `bbox` here sets the spatial extent of the compiled dataset.

---

## .env

| Variable | Required | Description |
|---|---|---|
| `STORE_DIR` | yes | Root path for Zarr output (can be an external drive) |
| `CMEMS_USERNAME` | CMEMS only | Copernicus Marine account username |
| `CMEMS_PASSWORD` | CMEMS only | Copernicus Marine account password |
| `AVISO_USERNAME` | AVISO only | AVISO account username |
| `AVISO_PASSWORD` | AVISO only | AVISO account password |
| `AVISO_FTP_SERVER` | AVISO only | FTP server hostname |

CDS / ERA5 credentials are handled by the `cdsapi` package and stored in `~/.cdsapirc`.

---

## Adding a new variable

1. Add an entry under `variables:` in `config.yaml` with the correct `source`, `dataset_id_rep`, and `local_folder`.
2. Add `variable_attrs` entries for each output variable name (used to set metadata in compiled Zarr files).
3. If the source is new, implement a downloader class inheriting from `BaseDownloader` and register it in `h2mare/cli/main.py`.
