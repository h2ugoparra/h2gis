# h2mare

**Geospatial processing pipeline for climate and ocean data.**

H2MARE downloads, converts, and harmonises multi-source oceanographic and atmospheric data into analysis-ready formats. It streamlines the full pipeline from raw API download through regridding and point extraction, optimised for large-scale spatiotemporal analysis.

---

## Features

| Feature | Description |
|---|---|
| Multi-source integration | CMEMS, AVISO, and ERA5 via their native APIs |
| Format conversion | NetCDF / GRIB → Zarr (chunked, compressed) |
| Compilation | Regrid all variables to a common 0.25° daily grid |
| Extraction | Extract time series at CSV points or SHP geometries |
| Incremental updates | Infers missing dates from the store; downloads only the gap |
| Catalog tracking | Metadata index per variable for fast Zarr range queries |
| Parquet store | Hive-partitioned Parquet store managed by `ParquetIndexer` with lazy scanning, overlap resolution, and built-in visualisation |

---

## Data sources

- **[CMEMS](https://marine.copernicus.eu/)** — Copernicus Marine Service
- **[AVISO](https://www.aviso.altimetry.fr/)** — Satellite Altimetry Data
- **[CDS / ERA5](https://cds.climate.copernicus.eu/)** — Copernicus Climate Data Store

API credentials are required for each provider. See [Installation](installation.md).

---

## Quick links

- [Installation & setup](installation.md)
- [Configuration reference](configuration.md)
- [CLI commands](cli.md)
- [Variable catalog](variables.md)
- [Architecture overview](architecture.md)
