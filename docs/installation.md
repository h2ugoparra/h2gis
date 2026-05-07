# Installation

## Prerequisites

- Python >= 3.11
- [uv](https://docs.astral.sh/uv/) — fast Python package and project manager
- Sufficient disk space for downloaded datasets (varies by region and time range)
- API credentials for the data providers you intend to use

## Install from source

```bash
git clone https://github.com/h2ugoparra/h2mare.git
cd h2gis
uv sync
```

Include development dependencies (pytest, black, isort):

```bash
uv sync --dev
```

## Environment setup

Create a `.env` file in the project root. At minimum `STORE_DIR` is required:

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

## Verify installation

```bash
uv run h2mare --help
```

Expected output lists the available commands: `run`, `compile`, `convert`.

## Data storage layout

```
$STORE_DIR/
└── <var_config.local_folder>/   # one directory per variable key
    └── *.zarr                   # yearly or monthly Zarr stores

$PROJECT_ROOT/
├── data/raw/downloads/          # raw NetCDF / GRIB files
├── data/processed/
│   ├── parquet/                 # extracted point / geometry time series
│   └── metadata/                # ZarrCatalog Parquet indices
└── logs/                        # pipeline log files
```
