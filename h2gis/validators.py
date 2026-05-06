"""
Validation utilities for h2gis.

Provides common validation functions used across multiple modules.
"""

from __future__ import annotations

from typing import Sequence

from h2gis.config import AppConfig
from h2gis.types import TimeResolution


def validate_var_key(var_key: str, config: AppConfig) -> str:
    """
    Validate that a variable key exists in config.

    Args:
        var_key: Variable identifier to validate
        config: Application configuration

    Raises:
        ValueError: If var_key not found in config.variables

    Example:
        >>> validate_var_key("sea_surface_height", app_config)
        >>> # Raises ValueError if not found
    """
    if var_key not in config.variables:
        available = ", ".join(config.variables.keys())
        raise ValueError(
            f"Variable key '{var_key}' not found in config. "
            f"Available keys: {available}"
        )
    return var_key


def validate_var_keys(var_keys: Sequence[str], config: AppConfig) -> None:
    """
    Validate multiple variable keys.

    Args:
        var_keys: Variable identifiers to validate
        config: Application configuration

    Raises:
        ValueError: If any var_key not found
    """
    invalid = [key for key in var_keys if key not in config.variables]

    if invalid:
        available = ", ".join(config.variables.keys())
        raise ValueError(
            f"Variable key(s) not found in config: {', '.join(invalid)}. "
            f"Available keys: {available}"
        )


def validate_time_resolution(period: str | TimeResolution) -> TimeResolution:
    """
    Validate supported period granularity for data storage.

    Raises:
        ValueError: if period not 'year' or 'month'
    """
    if isinstance(period, TimeResolution):
        return period

    # Validate string value
    if not isinstance(period, str):
        raise ValueError(
            f"Period must be a string or Period enum, got {type(period).__name__}"
        )
    # Normalize to lowercase for case-insensitive comparison
    period_lower = period.lower()

    try:
        return TimeResolution(period_lower)
    except ValueError:
        valid_values = ", ".join(p.value for p in TimeResolution)
        raise ValueError(f"Invalid period '{period}'. Must be one of: {valid_values}")


# def validate_depth_range(depth_range: Sequence[float]) -> None:
#    """ Validate depth range for vertical data variables"""
#    if depth_range and len(depth_range) != 2:
#            raise ValueError("Depth range requires a min and max values.")
