# ParquetPlotter

`ParquetPlotter` is the visualization accessor for [`ParquetIndexer`](parquet_indexer.md). Access it via `indexer.plot` — do not instantiate it directly.

```python
idx.plot.time_series("sst", agg_by="month")
idx.plot.spatial_maps("sst", agg_by="season")
```

---

## `time_series()`

```python
idx.plot.time_series(
    var_name,
    agg_by,           # "day" | "week" | "month" | "season" | "year"
    dates=None,
    bbox=None,
)
```

Returns an interactive Plotly line chart of `var_name` aggregated (mean) over space and time.

| Parameter | Description |
|---|---|
| `var_name` | Variable column to plot |
| `agg_by` | Temporal aggregation: `"day"`, `"week"`, `"month"`, `"season"`, or `"year"` |
| `dates` | `(start, end)` tuple or `list[str]` of dates. Defaults to full dataset |
| `bbox` | `(xmin, ymin, xmax, ymax)` spatial filter. Defaults to full extent |

Seasonal values are assigned to the first month of the season (e.g. spring → March 1st) for plotting purposes.

---

## `spatial_maps()`

```python
idx.plot.spatial_maps(
    var_name,
    agg_by="month",   # "month" | "season"
    dates=None,
    data_bbox=None,
    map_bbox=None,
    vminmax=None,
    main_title=None,
    legend_title=None,
    save_path=None,
)
```

Climatological panel maps — 12 panels for `agg_by="month"`, 4 for `agg_by="season"`. Each panel shows the long-term mean at every grid cell across all years in the selected data.

| Parameter | Description |
|---|---|
| `var_name` | Variable column to plot |
| `agg_by` | `"month"` (12 panels) or `"season"` (4 panels) |
| `dates` | Date range or list for filtering. Defaults to full dataset |
| `data_bbox` | Spatial filter applied before aggregation |
| `map_bbox` | Visible region on each panel. Defaults to extent of loaded data |
| `vminmax` | Fixed `(vmin, vmax)` for the colorbar. Defaults to data range |
| `main_title` | Figure title |
| `legend_title` | Colorbar label. Defaults to the variable short name from config |
| `save_path` | Path to save the figure. If `None`, shown interactively |

---

## Caching

Aggregation results are cached internally by `(var_name, agg_by, dates, bbox)`. Call `idx.plot.clear_cache()` to invalidate manually, or it is cleared automatically after each `add_data()` call on the parent indexer.
