# Downloaders

Three downloader classes share a common interface. All inherit from `BaseDownloader` and are selected automatically by `PipelineManager` based on the `source` field in `config.yaml`.

---

## CMEMSDownloader

Downloads data from the Copernicus Marine Service using the `copernicusmarine` Python client.

```python
from h2gis.downloader.cmems_downloader import CMEMSDownloader

dl = CMEMSDownloader("sst")
dl.run(start_date="2024-01-01", end_date="2024-12-31")
```

Automatically switches from the reprocessed (`rep`) dataset to the near-real-time (`nrt`) dataset at the appropriate boundary date. The boundary is fetched from the CMEMS catalogue on each run.

**Used for:** `sst`, `ssh`, `mld`, `chl`, `seapodym`, `o2`

---

## AVISODownloader

Downloads files from AVISO via FTP. Credentials are read from `.env`.

```python
from h2gis.downloader.aviso_downloader import AVISODownloader

dl = AVISODownloader("fsle")
dl.run(start_date="2024-01-01", end_date="2024-12-31")
```

**Used for:** `fsle`, `eddies`

---

## CDSDownloader

Downloads ERA5 data from the Copernicus Climate Data Store using `cdsapi`.

```python
from h2gis.downloader.cds_downloader import CDSDownloader

dl = CDSDownloader("atm-instante")
dl.run(start_date="2024-01-01", end_date="2024-12-31")
```

**Used for:** `atm-instante`, `atm-accum-avg`, `radiation`, `waves`

---

## Common interface

All downloaders expose:

| Method | Description |
|---|---|
| `run(start_date, end_date)` | Download data for the given date range |
| `resolve_date_range(var_key, start, end)` | Infer missing start/end from the existing store |
| `get_rep_availability()` | Return the `DateRange` covered by the rep dataset (CMEMS only) |

### Date inference

When `start_date` / `end_date` are `None`, the downloader calls `resolve_date_range` which inspects the existing Zarr store via `ZarrCatalog` and downloads only the gap between the last available date and today.
