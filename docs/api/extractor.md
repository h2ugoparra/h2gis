# Extractor

`Extractor` reads h2ds Zarr stores and extracts time series at point locations (CSV) or spatial geometries (SHP).

```python
from h2mare.processing.extractor import Extractor

extractor = Extractor()
extractor.run(
    locations="data/points.csv",
    start_date="2024-01-01",
    end_date="2024-12-31",
)
```

---

## Constructor

```python
Extractor(
    var_key="h2ds",
    app_config=None,
    store_root=None,
)
```

| Parameter | Default | Description |
|---|---|---|
| `var_key` | `"h2ds"` | Variable key to extract from |
| `app_config` | settings | Override the application configuration |
| `store_root` | `STORE_DIR` | Root directory of Zarr stores |

---

## `run()`

```python
extractor.run(
    locations,           # path to CSV (points) or SHP (geometries)
    start_date=None,
    end_date=None,
    variables=None,      # subset of variables to extract; None = all
    output_dir=None,     # defaults to data/processed/parquet/
)
```

| Parameter | Description |
|---|---|
| `locations` | Path to a CSV file with `lat`/`lon` columns, or a SHP file with polygon/line geometries |
| `start_date` | Start of extraction period |
| `end_date` | End of extraction period |
| `variables` | List of variable names to extract. Defaults to all variables in the dataset |
| `output_dir` | Output directory for Parquet files |

---

## Output format

One Parquet file is written per location or geometry. Each file contains a `time` column and one column per extracted variable.

For point extraction the output filename is derived from the row index or an ID column in the input CSV. For geometry extraction the filename is derived from the geometry's attribute table.

---

## Parallelism

Extraction across locations is parallelised with `ThreadPoolExecutor`. The degree of parallelism is controlled by the number of available CPU cores.
