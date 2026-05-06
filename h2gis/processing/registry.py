"""
Registry mapping var_key → dataset-specific processor function.

Add a new entry here when a new source or variable needs custom preprocessing
during the NetCDF → Zarr conversion step.
"""

from __future__ import annotations

from typing import Callable

import xarray as xr

import h2gis.processing.core.aviso as aviso
import h2gis.processing.core.cds as cds
import h2gis.processing.core.cmems as cmems

PROCESSORS: dict[str, Callable[..., xr.Dataset]] = {
    "atm-instante": cds.process_atm_instante,
    "atm-accum-avg": cds.process_atm_accum_avg,
    "radiation": cds.process_radiation,
    "waves": cds.process_waves,
    "chl": cmems.process_chl,
    "sst": cmems.process_sst,
    "mld": cmems.process_mld,
    "ssh": cmems.process_ssh,
    "fsle": aviso.process_fsle,
}
