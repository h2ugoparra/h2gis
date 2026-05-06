"""
Compute and save ekman day-of-year(DOY) mean and monthly 90th percentile from 1998-2017
"""

import xarray as xr
from loguru import logger
import sys
from pathlib import Path

sys.path.append(str(Path().cwd().resolve()))
from h2gis.config import settings
from h2gis.utils.spatial import clip_land_data

import warnings
warnings.simplefilter('ignore', UserWarning)

import matplotlib.pyplot as plt

# HELPERS
def get_path_for_year(files: list[Path], year: int) -> Path | None:
    """
    Return the .zarr path corresponding to a given year.
    """
    for f in files:
        if f.stem.endswith(str(year)):  # f.stem removes the .zarr suffix
            return f
    return None


var_key = 'atm-instante'
var_cfg = settings.app_config.variables.get(var_key)
var_dir = STORE_DIR / var_cfg.local_folder # type: ignore
files = sorted(var_dir.glob("*.zarr"))

ekman = xr.open_mfdataset(files, engine="zarr")['ekman_pumping']

# Define 20 years of data for baseline period
baseline = ekman.sel(time=slice('1998-01-01', '2017-12-31'))
#
## 7-day rolling mean
ekman_7d = baseline.rolling(time=7, min_periods=1).mean()

## Remove leap days (February 29 every 4 years)
ek_noleap = ekman_7d.where(~((ekman_7d.time.dt.month == 2) & (ekman_7d.time.dt.day == 29)), drop=True)

## DOY mean
clim = ek_noleap.groupby('time.dayofyear').mean('time')

assert settings.STORE_DIR is not None, "STORE_DIR must be set in settings to save climatology files"
file_path = settings.STORE_DIR / "Climatology" / "cds_ekman-doy-mean_80W-10E-0-70N_1998-2017.nc"
logger.debug("saving")
clim.to_netcdf(file_path)
logger.debug("done")

anom = ek_noleap.groupby('time.dayofyear') - clim

ds = xr.Dataset({
    #'ekman_7d': ekman_7d,
    'ekman_pumping_anom': anom.reindex_like(ekman) # aligns time back to original
    })

# Compute monthly 90th percentile (dims:month, lat, lon) and save file
logger.debug("Computing quantile")
p90_monthly = ds['ekman_pumping_anom'].groupby("time.month").quantile(0.90, dim="time").compute()

logger.debug("Saving file")
file_path = settings.STORE_DIR / "Climatology" / "cds_ekman-montly-90thquantile_80W-10E-0-70N_1998-2017.nc"
p90_monthly.to_netcdf(file_path)
logger.debug(f"File saved at {file_path}")


# --------------------------------------------------------------------
# ---- PROCESS MULTI-YEAR EKMAN VARS AND ADD TO YEARLY ZARR FILES ----
# --------------------------------------------------------------------
logger.info("Processing Ekman variables")
clim_dir = settings.STORE_DIR / "Climatology"
p90 = xr.open_dataset(clim_dir / "cds_ekman-montly-90thquantile_80W-10E-0-70N_1998-2017.nc")['ekman_pumping_anom']
clim = xr.open_dataset(clim_dir / "cds_ekman-doy-mean_80W-10E-0-70N_1998-2017.nc")


ekman_7d = ekman.rolling(time=7, min_periods=1).mean()
anom = ekman_7d.groupby('time.dayofyear') - clim

ds_ekman = xr.Dataset({
    'ekman_7d': ekman_7d,
    'ekman_anom': anom['ekman_pumping']
    })

ds_ekman['ekman_7d'].attrs.update({
        'long_name': 'Ekman 7day-mean vertical velocity',
        'units': 'm/s',
        'description': 'Ekman pumping mean velocity within a rolling 7-day window.'
    })
ds_ekman['ekman_anom'].attrs.update({
        'long_name': 'Ekman anomaly',
        'units': 'm/s',
        'description': 'Ekman anomaly calculated as the difference between 7day mean Ekman pumping and 1998-2017 climatology, per day-of-year (DOY) and grid cell.'
    })

#----------------------
# Create lag anomalies
# ----------------------
for lag in [3, 7, 14]:
    ds_ekman[f'ekman_anom_lag{lag}'] = ds_ekman['ekman_anom'].shift(time=lag)
    ds_ekman[f'ekman_anom_lag{lag}'].attrs.update({
        'long_name': f'Ekman anomaly with a {lag} day lag',
        'units': 'm/s',
        'description': f'{lag}-days lagged Ekman anomaly alculated as the difference between 7day mean Ekman pumping and 1998-2017 climatology, per day-of-year (DOY) and grid cell.'
    })

# Event exceedance detection
p90_broadcast = xr.apply_ufunc(
    lambda m: p90.sel(month=m),
    ds_ekman['time'].dt.month,
    vectorize=True,
    dask="parallelized",
    input_core_dims=[[]],
    output_core_dims=[["lat", "lon"]],
    output_dtypes=[ds_ekman['ekman_anom'].dtype],
)

# Exceedances: anomaly > local monthly p90
exceed = ds_ekman['ekman_anom'] > p90_broadcast

# Rolling counts for 3, 7, 14 days
for w in [3, 7, 14]:
    ds_ekman[f'n_upwell_events_{w}d'] = clip_land_data(exceed.rolling(time=w, min_periods=1).sum())
    ds_ekman[f'n_upwell_events_{w}d'].attrs.update({
        'long_name': f'Number of Ekman pumping upwelling events within {w}-days',
        'units': 'count',
        'description': f'Daily count of events where Ekman pumping anomaly exceeded the 90th percentile '
        f'threshold from the 1998 to 2017 monthly climatology computed for each grid cell and accumulated within a rolling {w}-day window. '
        f'Values range from 0 (no events) to {w} (all days in the window exceed threshold). Note: values dont represent days but frequency of events.'
    })


# Subset by year to concatenate with year zarr files

years = list(range(1998, 1999))
for year in years:
    logger.info(f"Processing year: {year}")
    logger.info(year)
    ds2 = ds_ekman.where(ds_ekman.time.dt.year == year, drop=True).drop_vars('dayofyear')
    ds2 = ds2.chunk({'time': 30, 'lat': len(ds2.lat),'lon': len(ds2.lon)})

    year_path = get_path_for_year(files, year)
    ds1 = xr.open_zarr(year_path)

    xr.testing.assert_equal(ds1.time, ds2.time)

    ds_merged = xr.merge([ds1, ds2])

    ds_merged.to_zarr(year_path, mode="w")
    logger.info(f"Merged data saved at {year_path}")

