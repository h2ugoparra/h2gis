"""Tests for write_append_zarr and atomic swap behaviour in storage.py."""
import shutil
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from h2mare.storage.storage import _append_data, write_append_zarr


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ds(start: str = "2020-01-01", n_days: int = 5, seed: int = 0) -> xr.Dataset:
    """Varied (non-constant) data so have_vars_unique_values does not fire."""
    times = pd.date_range(start, periods=n_days, freq="D")
    rng = np.random.default_rng(seed)
    data = rng.uniform(10.0, 30.0, size=(n_days, 3, 3))
    return xr.Dataset(
        {"sst": (["time", "lat", "lon"], data)},
        coords={
            "time": times,
            "lat": [30.0, 35.0, 40.0],
            "lon": [-10.0, -5.0, 0.0],
        },
    )


# ---------------------------------------------------------------------------
# write_append_zarr — new write path
# ---------------------------------------------------------------------------

class TestNewWrite:

    def test_creates_zarr_directory(self, tmp_path):
        path = tmp_path / "sst.zarr"
        write_append_zarr("sst", _make_ds(), path)
        assert path.exists()

    def test_written_data_is_readable(self, tmp_path):
        path = tmp_path / "sst.zarr"
        write_append_zarr("sst", _make_ds(), path)
        ds = xr.open_zarr(path)
        assert "sst" in ds.data_vars
        assert len(ds.time) == 5
        ds.close()

    def test_verification_failure_removes_partial_write(self, tmp_path, monkeypatch):
        """If the post-write open_zarr verification fails, the zarr directory is
        cleaned up and RuntimeError is raised — no partial file left behind."""
        path = tmp_path / "sst.zarr"

        def bad_open(*args, **kwargs):
            raise OSError("simulated corruption")

        monkeypatch.setattr("h2mare.storage.storage.xr.open_zarr", bad_open)

        with pytest.raises(RuntimeError, match="verification failed"):
            write_append_zarr("sst", _make_ds(), path)

        assert not path.exists()


# ---------------------------------------------------------------------------
# _append_data — atomic backup-swap
# ---------------------------------------------------------------------------

class TestAtomicSwap:

    def test_no_bak_file_after_success(self, tmp_path):
        """.bak file must be removed after a successful append."""
        path = tmp_path / "sst.zarr"
        _make_ds("2020-01-01", 5).to_zarr(path)
        _append_data("sst", _make_ds("2020-01-06", 5), path)
        assert not path.with_name(path.name + ".bak").exists()

    def test_no_tmp_file_after_success(self, tmp_path):
        """.tmp directory must be removed after a successful append."""
        path = tmp_path / "sst.zarr"
        _make_ds("2020-01-01", 5).to_zarr(path)
        _append_data("sst", _make_ds("2020-01-06", 5), path)
        assert not path.with_name(path.name + ".tmp").exists()

    def test_result_spans_both_periods(self, tmp_path):
        """Appended zarr should contain all timesteps from both writes."""
        path = tmp_path / "sst.zarr"
        _make_ds("2020-01-01", 5).to_zarr(path)
        _append_data("sst", _make_ds("2020-01-06", 5), path)
        ds = xr.open_zarr(path)
        assert len(ds.time) == 10
        ds.close()

    def test_original_restored_when_final_move_fails(self, tmp_path):
        """If renaming tmp → final fails, the original is restored from backup."""
        path = tmp_path / "sst.zarr"
        _make_ds("2020-01-01", 5).to_zarr(path)

        call_count = [0]
        original_move = shutil.move

        def failing_move(src, dst):
            call_count[0] += 1
            if call_count[0] == 2:  # second call: tmp → final
                raise OSError("simulated disk full")
            return original_move(src, dst)

        with patch("h2mare.storage.storage.shutil.move", side_effect=failing_move):
            with pytest.raises(RuntimeError, match="original restored from backup"):
                _append_data("sst", _make_ds("2020-01-06", 5), path)

        # Original data still intact
        ds = xr.open_zarr(path)
        assert len(ds.time) == 5
        ds.close()

    def test_no_orphan_bak_after_swap_failure(self, tmp_path):
        """After a failed swap the .bak is moved back; no .bak should remain."""
        path = tmp_path / "sst.zarr"
        _make_ds("2020-01-01", 5).to_zarr(path)

        call_count = [0]
        original_move = shutil.move

        def failing_move(src, dst):
            call_count[0] += 1
            if call_count[0] == 2:
                raise OSError("simulated disk full")
            return original_move(src, dst)

        with patch("h2mare.storage.storage.shutil.move", side_effect=failing_move):
            with pytest.raises(RuntimeError):
                _append_data("sst", _make_ds("2020-01-06", 5), path)

        assert not path.with_name(path.name + ".bak").exists()
