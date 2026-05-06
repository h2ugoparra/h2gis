"""
Script to process bathymetry data from NOAA ETOPO 15s resolution mosaic data. Mosaics are organized in 15x15 degree tiles. 
Objectives: 
    1) Create a merged layer with origin resolution of 15 arc-seconds for the North Atlantic domain.
    2) Create a mean and std bathymetry file at 0.25 degree resolution for the North Atlantic domain. Mean and std are computed from all 15s pixels within each 0.25 degree pixel.
"""

import xarray as xr
import numpy as np
from pathlib import Path
import warnings

import matplotlib.pyplot as plt

from h2gis.config import settings
from h2gis.types import BBox
from h2gis.utils import resolve_store_path, create_filename_label, GridBuilder

DX, DY = 0.25, 0.25

var_key = 'bathy'
var_cfg = settings.app_config.variables[var_key]

if not var_cfg:
	raise ValueError(f"No variable config found for key: {var_key}")

geo_extent = var_cfg.bbox
if geo_extent:
    xmin, ymin, xmax, ymax = geo_extent

var_dir = resolve_store_path(var_cfg) 



# ----------------------
# ---- Merged layer ----
# ----------------------

surf15_dir = var_dir / "15s_resolution/surface"

files = list(surf15_dir.glob("ETOPO_2022_v1_15s_*_surface.nc"))
ds = xr.open_mfdataset(files, combine='by_coords').drop_vars(['crs']).sel(
      lon=slice(xmin, xmax), lat=slice(ymin, ymax)
      )

file_name = f"etopo_15s_{create_filename_label(BBox.from_tuple(geo_extent if geo_extent else (xmin, ymin, xmax, ymax)), 'year')}_surface.nc"
ds.to_netcdf(var_dir / file_name)
ds.close()


with xr.open_dataset(var_dir / file_name) as ds:
    print(ds)

#import matplotlib.pyplot as plt
#ds.z.plot()
#plt.show()

#print(f"Found {len(files)} files")


# --------------------------------
# ---- Mean and Std at 0.25 ----
# --------------------------------

in_file_name = "etopo_15s_80W-10E-0-70N_surface.nc"
in_file_path = var_dir / in_file_name

out_file_name = f"etopo_0.25deg_{create_filename_label(BBox.from_tuple(geo_extent if geo_extent else (xmin, ymin, xmax, ymax)), 'year')}_mean-std_surface.nc"
out_file_path = var_dir / out_file_name

da = xr.open_dataset(in_file_path)['z']


#da.plot() # type: ignore
#plt.title('Original bathymetry at 15 arc-secs')
#plt.show()

# Create base grid
base_grid = GridBuilder(BBox.from_tuple((xmin, ymin, xmax, ymax)), DX, DY).generate_grid()

# 1. Compute how many cells from ds (finner) fit into base_grid cell (coarser)
coarsen_factor = int(round(DX / (da.lon.values[1] - da.lon.values[0]))) # 60 cells

# 2. Coarsen along both latitude and longitude
da_coarse = da.coarsen(
            lat=coarsen_factor, 
            lon=coarsen_factor, 
            boundary="pad", # keep partial cells from edges (may produce NaNs) when not exact fit between cells
            coord_func='mean'
            )
            
da_mean = da_coarse.mean() # type: ignore
da_std = da_coarse.std()   # type: ignore

ds_new = xr.Dataset({
    'bathy': da_mean,
    'bathy_std': da_std
    })

ds_new.to_netcdf(out_file_path)

