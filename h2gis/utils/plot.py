""" 
plot functions
"""

import calendar
import math
from pathlib import Path
from typing import Literal, Optional

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import xarray as xr
from IPython.display import clear_output, display

from h2gis.config import settings
from h2gis.storage.parquet_helpers import _required_columns
from h2gis.types import BBox


# --------------------------------
#       PARQUET
# --------------------------------
def plot_maps(
    df: pl.DataFrame,
    var_name: str,
    *,
    agg_by: Literal["month", "season"],
    time_col: str = "time",
    lon_col: str = "lon",
    lat_col: str = "lat",
    vminmax: Optional[tuple[int | float, int | float]] = None,
    data_bbox: Optional[tuple[float, float, float, float]] = None,
    map_bbox: Optional[tuple[float, float, float, float]] = None,
    main_title: Optional[str] = None,
    legend_title: Optional[str] = None,
    save_path: Optional[str | Path] = None,
) -> None:
    """Plots monthly or seasonal maps.

    Args:
        df (pl.DataFrame): Data input. Must contain var_name, lon_col, lat_col, and
            either a pre-computed group column (``agg_by`` value) or ``time_col`` so the
            group column can be derived automatically.
        var_name (str): Variable name.
        agg_by (Literal['month', 'season']): Time aggregation.
        time_col (str): Name of the datetime column used to derive the group
            column when it is not already present in *df*. Defaults to None.
        vminmax (tuple[int | float, int | float], optional): Variable min and max.
            Defaults to None, inferring from data.
        data_bbox (tuple[float, float, float, float], optional): Data geographic extent
            (xmin, ymin, xmax, ymax). Used to derive the map extent when *map_bbox* is
            not provided. Defaults to None, inferring from data.
        map_bbox (tuple[float, float, float, float], optional): Map display extent
            (xmin, ymin, xmax, ymax). Controls the visible region on each panel.
            Defaults to None, falling back to *data_bbox* or the inferred data extent.
        main_title (str, optional): Plot main title. Defaults to None.
        legend_title (str, optional): Legend title. Defaults to None; falls back to
            ``short_name`` in config.yaml, then to ``var_name``.
        save_path (Path, optional): Path to save plot. Defaults to None (show plot).

    Raises:
        ValueError: if df is empty, var_name is missing, or the group column cannot
            be derived.
    """
    if df.is_empty():
        raise ValueError("No data after aggregation.")

    _required_columns(df, var_name)

    # Derive group column from time_col when not already present
    if agg_by not in df.columns:
        if time_col is None:
            raise ValueError(
                f"Column '{agg_by}' not found in df. "
                f"Pass time_col so it can be derived automatically."
            )
        _required_columns(df, time_col)
        if agg_by == "month":
            df = df.with_columns(pl.col(time_col).dt.month().alias("month"))
        elif agg_by == "season":
            df = df.with_columns(
                pl.when(pl.col(time_col).dt.month().is_in([12, 1, 2]))
                .then(pl.lit("winter"))
                .when(pl.col(time_col).dt.month().is_in([3, 4, 5]))
                .then(pl.lit("spring"))
                .when(pl.col(time_col).dt.month().is_in([6, 7, 8]))
                .then(pl.lit("summer"))
                .otherwise(pl.lit("autumn"))
                .alias("season")
            )

    if data_bbox is not None:
        xmin, ymin, xmax, ymax = data_bbox
    else:
        metadata = BBox.from_dataframe(df, lon_col=lon_col, lat_col=lat_col)
        xmin = float(metadata.xmin)
        xmax = float(metadata.xmax)
        ymin = float(metadata.ymin)
        ymax = float(metadata.ymax)

    if map_bbox is not None:
        map_xmin, map_ymin, map_xmax, map_ymax = map_bbox
    else:
        map_xmin, map_ymin, map_xmax, map_ymax = xmin, ymin, xmax, ymax

    if vminmax is not None:
        vmin, vmax = vminmax
    else:
        metadata = df.select(
            [pl.col(var_name).min().alias("vmin"), pl.col(var_name).max().alias("vmax")]
        )

        vmin = float(metadata["vmin"][0])
        vmax = float(metadata["vmax"][0])

    groups = split_by_group(df, agg_by)

    fig, axes = make_axes(len(groups))

    if main_title:
        fig.suptitle(main_title, fontsize=12, fontweight="bold")

    meshes = []

    for ax, (group, subdf) in zip(axes, groups.items()):
        lon, lat, grid = df_to_grid(subdf, var_name, lon_col=lon_col, lat_col=lat_col)

        title = calendar.month_abbr[group] if isinstance(group, int) else str(group)

        mesh = plot_panel(
            ax,
            lon,
            lat,
            grid,
            title=title,
            extent=(map_xmin, map_xmax, map_ymin, map_ymax),
            vmin=vmin,
            vmax=vmax,
        )

        meshes.append(mesh)

    for ax in axes[len(groups) :]:
        fig.delaxes(ax)

    cbar = fig.colorbar(
        meshes[-1],
        ax=axes,
        location="bottom",
        pad=0.03,  # space between subplots and colorbar
        shrink=0.6,
    )

    legend_label = legend_title or settings.variable_attrs.get(var_name, {}).get(
        "short_name", var_name
    )
    cbar.set_label(legend_label)

    if save_path:
        save_path = Path(save_path) if isinstance(save_path, str) else save_path
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
        plt.close()
    else:
        plt.show()


def plot_panel(
    ax,
    lon,
    lat,
    grid,
    *,
    title: str,
    extent: tuple[float, float, float, float],
    vmin: float,
    vmax: float,
    cmap: str = "turbo",
):
    ax.set_extent(extent)

    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS, linestyle=":")
    ax.add_feature(cfeature.LAND, facecolor="lightgray", alpha=0.5)
    ax.add_feature(cfeature.OCEAN, facecolor="lightblue")

    mesh = ax.pcolormesh(
        lon,
        lat,
        grid,
        shading="auto",
        cmap=cmap,
        vmin=vmin,
        vmax=vmax,
        transform=ccrs.PlateCarree(),
    )

    ax.set_title(title, fontsize=8)
    return mesh


def make_axes(n_panels: int):
    """
    Define subplots layout according to n_panels
        - 4x4 (seasonal), 6x2 (monthly)
        - Ideal size for monthly (12 panels) layout: (ncols, nrows) -> figsize():
            - (4, 3) -> (7, 7)
            - (3, 4) -> (7, 5)
            - (2, 6) -> (5, 10)

    Args:
        n_panels (int): Number of panels
    """
    if n_panels == 12:
        nrows, ncols = 6, 2
        figsize = (5, 10)
    elif n_panels == 4:
        nrows, ncols = 2, 2
        figsize = (7, 5)
    else:
        ncols = math.ceil(math.sqrt(n_panels))
        nrows = math.ceil(n_panels / ncols)
        figsize = (ncols * 3.5, nrows * 2.5)

    fig, axes = plt.subplots(
        nrows,
        ncols,
        figsize=figsize,
        constrained_layout=True,
        subplot_kw={"projection": ccrs.PlateCarree()},
    )

    return fig, axes.flatten()


def df_to_grid(
    df: pl.DataFrame, var_name: str, *, lon_col: str = "lon", lat_col: str = "lat"
):
    _required_columns(df, [var_name, lon_col, lat_col])
    lon = df[lon_col].to_numpy()
    lat = df[lat_col].to_numpy()
    val = df[var_name].to_numpy()

    unique_lon = np.sort(np.unique(lon))
    unique_lat = np.sort(np.unique(lat))

    lon_idx = np.searchsorted(unique_lon, lon)
    lat_idx = np.searchsorted(unique_lat, lat)

    grid = np.full((unique_lat.size, unique_lon.size), np.nan)
    grid[lat_idx, lon_idx] = val

    return unique_lon, unique_lat, grid


def split_by_group(
    df: pl.DataFrame,
    group_col: str,
) -> dict[int | str, pl.DataFrame]:
    _required_columns(df, group_col)

    if group_col == "month":
        df = df.sort("month")
    elif group_col == "season":
        season_order = ["spring", "summer", "autumn", "winter"]
        df = df.with_columns(
            pl.col("season").cast(pl.Enum(season_order)).alias("_season_ord")
        ).sort("_season_ord")

    return {g[0]: subdf for g, subdf in df.group_by(group_col, maintain_order=True)}


# --------------------------------
#       XARRAY
# --------------------------------


def animate_vars(
    data: xr.Dataset | xr.DataArray, var_name: str | None = None, nsteps: int = 30
) -> None:
    """
    Animate plots from an xarray Dataset or DataArray over time.

    Args:
        data: Input data.
        var_name: Name of variable to plot (only needed if input is a Dataset).
        nsteps: Number of time steps to animate.
    """
    if isinstance(data, xr.Dataset):
        if var_name is None:
            raise ValueError("var_name must be provided when input is a Dataset")
        da = data[var_name]
    else:  # DataArray
        da = data

    if "time" not in da.dims:
        raise ValueError("Input must have a 'time' dimension for animation")

    nframes = min(nsteps, da.sizes["time"])
    for i in range(nframes):
        fig, ax = plt.subplots()
        da.isel(time=i).plot(ax=ax)  # type: ignore
        plt.title(f"Time index: {i} | {str(da['time'].values[i])}")
        display(fig)  # Display the figure in the notebook
        clear_output(wait=True)  # Clear previous output before showing new one
        plt.close()


def plot_allvars_timeidx(ds: xr.Dataset, time_idx: int) -> None:
    """
    Plot all variables for a specified time index.

    Args:
        ds (xr.Dataset): data input
        time_idx (int): time  index value
    """
    vars = [v for v in ds.data_vars if "time" in ds[v].dims]
    for var in vars:
        ds[var].isel(time=time_idx).plot()  # type: ignore
        plt.show()
