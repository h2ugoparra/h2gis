# API Reference

The main classes you interact with directly.

| Class | Module | Description |
|---|---|---|
| `CMEMSDownloader` | `h2gis.downloader.cmems_downloader` | Download CMEMS datasets |
| `AVISODownloader` | `h2gis.downloader.aviso_downloader` | Download AVISO FTP datasets |
| `CDSDownloader` | `h2gis.downloader.cds_downloader` | Download ERA5 / CDS datasets |
| `Compiler` | `h2gis.processing.compiler` | Merge variables into the h2ds grid |
| `ZarrCatalog` | `h2gis.storage.zarr_catalog` | Query and manage Zarr stores |
| `Extractor` | `h2gis.processing.extractor` | Extract time series at points or geometries |
| `PipelineManager` | `h2gis.pipeline_manager` | Orchestrate the full download → convert pipeline |
