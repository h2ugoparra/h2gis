# CLI Reference

All commands are run via `uv run h2mare <command> [options]`.

---

## `h2mare run`

Download raw data and convert it to Zarr for one or more variable keys.

```
uv run h2mare run [OPTIONS]
```

| Option | Type | Default | Description |
|---|---|---|---|
| `-v, --vars` | text (repeatable) | all keys | Variable key(s) to process |
| `--start-date` | YYYY-MM-DD | inferred | Start of date range. Must be paired with `--end-date` |
| `--end-date` | YYYY-MM-DD | inferred | End of date range. Must be paired with `--start-date` |
| `--store-path` | path | `STORE_ROOT` | Override the Zarr store root |
| `--no-convert` | flag | false | Download raw files only; skip Zarr conversion and compile |
| `--no-compile` | flag | false | Convert to Zarr but skip the h2ds compile step |
| `--dry-run` | flag | false | Plan tasks and log without downloading anything |

When `--start-date` / `--end-date` are omitted the pipeline infers the missing date range from the existing store.

**Examples**

```bash
# First-time download with explicit dates
uv run h2mare run -v sst --start-date 2021-01-01 --end-date 2021-12-31

# Update an existing store (dates inferred automatically)
uv run h2mare run -v sst

# Multiple variables at once
uv run h2mare run -v sst -v ssh -v mld

# Download only, skip Zarr conversion
uv run h2mare run -v sst --no-convert

# Skip the compile step after conversion
uv run h2mare run -v sst --no-compile

# Validate configuration without downloading
uv run h2mare run -v sst --dry-run

# Process all configured variables
uv run h2mare run
```

---

## `h2mare compile`

Merge per-variable Zarr stores into the unified h2ds compiled dataset.

```
uv run h2mare compile [OPTIONS]
```

| Option | Type | Default | Description |
|---|---|---|---|
| `-v, --vars` | text (repeatable) | all keys | Variable key(s) to include |
| `--start-date` | YYYY-MM-DD | inferred | Start of date range |
| `--end-date` | YYYY-MM-DD | inferred | End of date range |
| `--store-path` | path | `STORE_ROOT` | Override the Zarr store root |

**Examples**

```bash
# Compile all variables (dates inferred)
uv run h2mare compile

# Compile a subset of variables over a specific period
uv run h2mare compile -v sst -v ssh -v mld --start-date 2024-01-01 --end-date 2024-12-31

# Use a custom store path
uv run h2mare compile --store-path D:/GlobalData
```

---

## `h2mare convert`

Convert already-downloaded raw files to Zarr without re-downloading.

```
uv run h2mare convert [OPTIONS]
```

| Option | Type | Default | Description |
|---|---|---|---|
| `-v, --vars` | text (repeatable) | all keys | Variable key(s) to convert |
| `--in-dir` | path | `DOWNLOADS_DIR` | Override the input directory containing raw files |

**Examples**

```bash
# Convert downloaded files for sst and ssh
uv run h2mare convert -v sst -v ssh

# Convert from a custom input directory
uv run h2mare convert -v sst --in-dir /data/raw/CMEMS_SST
```

---

## `h2mare catalog`

Inspect `ZarrCatalog` metadata for one or more variable keys without opening any Zarr files.

```
uv run h2mare catalog [VAR_KEY] [OPTIONS]
```

| Option | Type | Default | Description |
|---|---|---|---|
| `VAR_KEY` | text | — | Variable key to inspect (e.g. `sst`, `ssh`) |
| `-a, --all` | flag | false | Show summary for all configured variables |
| `-r, --rows` | flag | false | Print individual catalog rows (filename, dataset, dates, timesteps) |

**Examples**

```bash
# Summary for SST
uv run h2mare catalog sst

# Summary for all configured variables
uv run h2mare catalog --all

# Show individual catalog rows
uv run h2mare catalog sst --rows
```

---

## Variable keys

Valid values for `-v / --vars`:

`sst` `ssh` `mld` `chl` `seapodym` `o2` `fsle` `eddies` `atm-instante` `atm-accum-avg` `radiation` `waves`

See [Variables](variables.md) for descriptions and source details.
