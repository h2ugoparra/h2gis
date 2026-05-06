"""Tests for storage/xarray_helpers.py."""
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from h2gis.storage.xarray_helpers import (
    chunk_dataset,
    convert360_180,
    get_dataset_encoding,
    have_vars_unique_values,
    rename_dims,
    unified_time_chunk,
    xr_float64_to_float32,
)


def _make_ds(n_time=10, n_lat=4, n_lon=4, dtype=np.float32):
    times = pd.date_range("2020-01-01", periods=n_time, freq="D")
    data = np.random.rand(n_time, n_lat, n_lon).astype(dtype)
    return xr.Dataset(
        {"sst": (["time", "lat", "lon"], data)},
        coords={
            "time": times,
            "lat": np.linspace(30, 40, n_lat),
            "lon": np.linspace(-10, 0, n_lon),
        },
    )


class TestGetDatasetEncoding:

    def test_returns_encoding_for_each_var(self):
        ds = _make_ds()
        enc = get_dataset_encoding(ds)
        assert "sst" in enc
        assert "chunks" in enc["sst"]

    def test_chunk_tuple_length_matches_dims(self):
        ds = _make_ds()
        enc = get_dataset_encoding(ds)
        assert len(enc["sst"]["chunks"]) == 3  # time, lat, lon


class TestUnifiedTimeChunk:

    def test_returns_positive_int(self):
        ds = _make_ds(n_time=365)
        chunk = unified_time_chunk(ds)
        assert isinstance(chunk, int)
        assert chunk >= 1

    def test_no_time_vars_raises(self):
        ds = xr.Dataset(
            {"sst": (["lat", "lon"], np.ones((4, 4)))},
            coords={"lat": [30.0, 31.0, 32.0, 33.0], "lon": [-10.0, -9.0, -8.0, -7.0]},
        )
        with pytest.raises(ValueError, match="time"):
            unified_time_chunk(ds)


class TestHaveVarsUniqueValues:

    def test_nonexistent_path_returns_false(self, tmp_path):
        bad_path = tmp_path / "nonexistent.zarr"
        assert have_vars_unique_values(bad_path) is False

    def test_dataset_with_varied_values_returns_false(self):
        ds = _make_ds(n_time=5)
        assert have_vars_unique_values(ds) is False

    def test_dataset_with_constant_last_slice_returns_true(self):
        times = pd.date_range("2020-01-01", periods=3, freq="D")
        data = np.random.rand(3, 4, 4).astype(np.float32)
        data[-1, :, :] = 5.0  # last time step is constant
        ds = xr.Dataset(
            {"sst": (["time", "lat", "lon"], data)},
            coords={"time": times, "lat": range(4), "lon": range(4)},
        )
        assert have_vars_unique_values(ds) is True


class TestConvert360To180:

    def test_converts_0_360_to_minus180_180(self):
        ds = xr.Dataset(
            {"sst": (["lat", "lon"], np.ones((3, 4)))},
            coords={"lat": [0.0, 1.0, 2.0], "lon": [0.0, 90.0, 180.0, 270.0]},
        )
        result = convert360_180(ds)
        assert float(result["lon"].min()) >= -180
        assert float(result["lon"].max()) <= 180

    def test_already_negative_lon_unchanged(self):
        ds = xr.Dataset(
            {"sst": (["lat", "lon"], np.ones((2, 3)))},
            coords={"lat": [0.0, 1.0], "lon": [-10.0, 0.0, 10.0]},
        )
        result = convert360_180(ds)
        assert list(result["lon"].values) == [-10.0, 0.0, 10.0]


class TestRenameDims:

    def test_renames_longitude_latitude(self):
        ds = xr.Dataset(
            {"sst": (["latitude", "longitude"], np.ones((3, 3)))},
            coords={
                "latitude": [30.0, 35.0, 40.0],
                "longitude": [-10.0, -5.0, 0.0],
            },
        )
        result = rename_dims(ds)
        assert "lat" in result.dims
        assert "lon" in result.dims

    def test_renames_valid_time(self):
        times = pd.date_range("2020-01-01", periods=3, freq="D")
        ds = xr.Dataset(
            {"sst": (["valid_time", "lat", "lon"], np.ones((3, 2, 2)))},
            coords={"valid_time": times, "lat": [30.0, 31.0], "lon": [-10.0, -9.0]},
        )
        result = rename_dims(ds)
        assert "time" in result.dims

    def test_no_rename_needed(self):
        ds = _make_ds()
        result = rename_dims(ds)
        assert "time" in result.dims
        assert "lat" in result.dims
        assert "lon" in result.dims
