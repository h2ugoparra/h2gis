"""
Utilities for generating standardized labels from spatial/temporal data.

These labels are used in filenames, catalog entries, and logging.
"""

from __future__ import annotations

from typing import Literal, Optional, Sequence

import xarray as xr
from loguru import logger

from h2mare.types import BBox, DateRange


def create_filename_label(
    bbox: BBox | Sequence[float],
    date_format: Literal["year", "date", "yearmonth"],
    date_range: Optional[DateRange] = None,
) -> str:
    """
    Create a filename label from spatial and temporal information.

    Args:
        bbox: Bounding box (BBox object or tuple)
        date_format: How to format dates
        date_range: Date range (optional)

    Example:
        >>> from h2mare.core import BBox, DateRange
        >>> bbox = BBox(-10, 30, 20, 40)
        >>> dr = DateRange(
        ...     start=pd.Timestamp("2023-01-01"),
        ...     end=pd.Timestamp("2023-12-31")
        ... )
        >>> create_filename_label(bbox, dr, "year")
        '10W-20E-30N-40N_2023'
    """
    # Convert bbox to BBox object if needed
    if not isinstance(bbox, BBox):
        bbox = BBox.from_tuple(bbox)

    # Start with spatial label
    label = bbox.to_label()

    # Add temporal label if provided
    if date_range is not None:
        temporal_label = date_range.to_label(date_format)
        label = f"{label}_{temporal_label}"

    return label


def create_label_from_dataset(
    ds: xr.Dataset,
    date_format: Literal["year", "date", "yearmonth"],
    warn_multi_year: bool = True,
) -> str:
    """
    Generate a filename label from dataset metadata.

    Args:
        ds: Dataset with lon/lat and time coordinates
        date_format: Date format for label
            - 'year': YYYY
            - 'date': YYYY-MM-DD-YYYY-MM-DD
            - 'yearmonth': YYYY-MM
        warn_multi_year: Log warning if dataset spans multiple years

    Returns:
        Filename label string

    Example:
        >>> ds = xr.open_dataset("data.nc")
        >>> label = create_label_from_dataset(ds, date_format="year")
        '10W-20E-30N-40N_2023'
    """
    # Extract spatial/temporal extent
    bbox = BBox.from_dataset(ds)
    date_range = DateRange.from_dataset(ds)

    # Warn if multi-year
    if warn_multi_year and date_range.spans_multiple_years() and date_format == "year":
        source = ds.encoding.get("source", "unknown")
        logger.warning(
            f"Dataset spans multiple years ({date_range.start.year} to "
            f"{date_range.end.year}) but using 'year' format. Source: {source}"
        )
    return create_filename_label(bbox, date_format, date_range)
