# H2MARE - Geospatial Processing for Climate and Ocean Data

![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)
[![PyPI](https://img.shields.io/pypi/v/h2mare)](https://pypi.org/project/h2mare/)

A Python pipeline for downloading and preprocessing multi-source oceanographic and atmospheric data into analysis-ready formats. H2MARE streamlines the acquisition and harmonization of data from major climate and ocean observation services, optimized for large-scale spatiotemporal analysis.

## Features

- **Multi-source data integration**: Download and process data from CMEMS, AVISO, and ERA5.
- **Variable grouping**: Organize related variables using configurable keys.
- **Format conversion**: Automated conversion from NetCDF/GRIB to optimized Zarr and Parquet format
- **Data compilation**: Regrid and interpolate multi-resolution datasets to a common grid
- **Point and geometry extraction**: Extract time series for specific locations or spatial features

## Data Sources

H2MARE supports the following data providers API keys and authentication are required for each:

- **[CMEMS](https://marine.copernicus.eu/)** - Copernicus Marine Service: Satellite and in-situ ocean observations
- **[AVISO](https://www.aviso.altimetry.fr/en/home.html)** - Archiving, Validation and Interpretation of Satellite Oceanographic data
- **[CDS-ERA5](https://cds.climate.copernicus.eu/)** - ERA5 hourly atmospheric reanalysis (1940-present)  
  *Hersbach, H., et al. (2023). DOI: 10.24381/cds.adbb2d47*

**Note**: Refer to each provider's documentation for authentication setup before use.

## Installation

### Prerequisites

- Python >= 3.11
- [uv](https://docs.astral.sh/uv/) — fast Python package and project manager
- Sufficient disk space for downloaded datasets (varies by region and time range)

### Install from PyPI

```bash
pip install h2mare
# or
uv add h2mare
```

### Install from source

```bash
git clone https://github.com/h2ugoparra/h2mare.git
cd h2mare
uv sync
```

## Configuration

H2MARE requires two configuration files in your working directory before first use.

### 1. `config.yaml`

Defines variables, dataset IDs, bounding boxes, and processing parameters. Copy the [template from the repository](https://github.com/h2ugoparra/h2mare/blob/main/config.yaml) as a starting point and edit it to match your needs.

### 2. `.env`

```env
# Path to external or large-capacity storage for processed Zarr files
STORE_DIR=/path/to/your/storage

# CMEMS credentials (required for SST, SSH, MLD, CHL, O2, SEAPODYM)
CMEMS_USERNAME=your_username
CMEMS_PASSWORD=your_password

# AVISO credentials (required for FSLE, Eddies)
AVISO_USERNAME=your_username
AVISO_PASSWORD=your_password
AVISO_FTP_SERVER=ftp-access.aviso.altimetry.fr
```

ERA5 / CDS credentials are configured separately via the `cdsapi` client — see the [CDS documentation](https://cds.climate.copernicus.eu/how-to-api) for setup.

> **Note:** Both files must be present in the directory where you run `h2mare`. You can also set the `H2MARE_ROOT` environment variable to point to a different directory containing them.

### Key variables groups

Edit `config.yaml` to define variable groups and processing parameters.

### Data Flow

- **Dowload** -  Raw NetCDF/GRIB files are fetched from configurated sources and saved at specified time resolution (monthly or yearly) as native-resolution Zarr files.
- **Compilation** (`h2mare/processing/compiler.py`) - Preprocessed data is regridded to a defined spatial/temporal resolution and geographic extent (configured via 'h2ds' key in `config.yaml`)
- **Extraction** (`h2mare/processing/extractor.py`) - Point (CSV files) or geometry (SHP files) data extraction from xarray datasets.

## Quick Start

```bash
# Download and process a single variable for a specific date range
uv run h2mare run sst --start-date 2021-01-01 --end-date 2021-12-31

# Multiple variables at once (space-separated)
uv run h2mare run seapodym mld o2 chl

# Infer missing dates from the existing store and download what's new
uv run h2mare run sst

# Download only (skip Zarr conversion)
uv run h2mare run sst --no-process

# Validate configuration without downloading
uv run h2mare run sst --dry-run

# Process all configured variables
uv run h2mare run
```

## Development

```bash
# Run the full test suite
uv run pytest tests/

# Run a single test file
uv run pytest tests/test_zarr_catalog.py -v

# Format code
uv run black h2mare/
uv run isort h2mare/
```

## Built with

| Library | Role |
|---------|------|
| [xarray](https://xarray.dev/) | N-dimensional labelled arrays and NetCDF/Zarr I/O |
| [zarr](https://zarr.dev/) | Chunked, compressed array storage |
| [dask](https://www.dask.org/) | Parallel and out-of-core computation |
| [polars](https://pola.rs/) | Fast DataFrame engine for extracted time series |
| [geopandas](https://geopandas.org/) | Geometry-based spatial extraction |
| [copernicusmarine](https://pypi.org/project/copernicusmarine/) | CMEMS dataset access |
| [cdsapi](https://pypi.org/project/cdsapi/) | ERA5 / CDS dataset access |

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests on [GitHub](https://github.com/h2ugoparra/h2mare.git).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## AI Assistance

Parts of this codebase were developed with the help of [Claude](https://claude.ai) (Anthropic).

## Acknowledgments

This project was developed under the framework of [COSTA project](https://costaproject.org/en/). This project relies on data from Copernicus Marine Service, AVISO, Copernicus Climate Data Store, and NOAA NCEI. We gratefully acknowledge these organizations for providing open access to their datasets.
