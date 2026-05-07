"""
Helpers for ZarrCatalog data coverage and store management.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
from loguru import logger

from h2mare.storage.zarr_catalog import get_zarr_time_coverage
from h2mare.types import DateRange, TimeResolution


def split_time_range(date_range: DateRange, split: TimeResolution) -> list[DateRange]:
    """
    Split date range into chunks.

    Args:
        date_range: Range to split
        split: Split strategy

    Returns:
        List of (start, end) tuples
    """
    chunks = []
    current = date_range.start

    while current <= date_range.end:
        if split == TimeResolution.MONTH:
            # End of current month
            chunk_end = current + pd.offsets.MonthEnd(0)
        elif split == TimeResolution.YEAR:
            # End of current year
            chunk_end = pd.Timestamp(year=current.year, month=12, day=31)
        else:
            raise ValueError("Invalid split value. Options are: 'month' or 'year'")

        # Don't exceed requested end
        chunk_end = min(chunk_end, date_range.end)

        chunks.append(DateRange(start=current, end=chunk_end))

        # Move to next period
        current = chunk_end + pd.Timedelta(days=1)

    return chunks


def get_store_coverage(var_key: str) -> Optional[DateRange]:
    """
    Get time coverage of existing data in store.

    Returns:
        DateRange of stored data, or None if no data exists
    """
    try:
        coverage = get_zarr_time_coverage(var_key)

        if coverage is None:
            logger.debug(f"No existing data found for {var_key}")
            return None

        return DateRange(start=coverage.start, end=coverage.end)

    except Exception as e:
        logger.warning(f"Could not read store coverage: {e}")
        return None
