"""Feature engineering mathematical utilities for transaction risk analysis.

Provides pure-Python Haversine distance calculations, implied physical speed,
and behavioral spending anomaly Z-scores.
"""

import math
from typing import Optional


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates great-circle distance between two coordinates on Earth using the Haversine formula.

    Args:
        lat1: Latitude of point 1 in degrees.
        lon1: Longitude of point 1 in degrees.
        lat2: Latitude of point 2 in degrees.
        lon2: Longitude of point 2 in degrees.

    Returns:
        Distance in kilometers (returns 0.0 if any coordinate is missing).
    """
    if None in (lat1, lon1, lat2, lon2):
        return 0.0
    r = 6371.0  # Earth mean radius in kilometers
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2.0) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2.0) ** 2
    )
    c = 2.0 * math.asin(math.sqrt(max(0.0, min(1.0, a))))
    return r * c


def calculate_implied_speed_kmh(
    distance_km: float,
    seconds_elapsed: float,
) -> float:
    """Computes implied travel speed in km/h from distance and time elapsed.

    Args:
        distance_km: Great-circle distance traveled in kilometers.
        seconds_elapsed: Seconds elapsed between two consecutive card events.

    Returns:
        Implied speed in kilometers per hour. Returns 0.0 if elapsed time <= 0.
    """
    if seconds_elapsed <= 0 or distance_km <= 0:
        return 0.0
    hours = seconds_elapsed / 3600.0
    return distance_km / hours


def calculate_amount_zscore(
    amount: float,
    avg_amount_30d: Optional[float],
    std_amount_30d: Optional[float],
) -> float:
    """Calculates behavioral spending anomaly Z-score: (amount - mu) / sigma.

    Args:
        amount: Current transaction amount in cents.
        avg_amount_30d: Rolling 30-day mean transaction amount for customer.
        std_amount_30d: Rolling 30-day standard deviation of transaction amount.

    Returns:
        Standard score (Z-score). Returns 0.0 if baseline metrics are unavailable or sigma <= 0.
    """
    if avg_amount_30d is None or std_amount_30d is None or std_amount_30d <= 0.0:
        return 0.0
    return (amount - avg_amount_30d) / std_amount_30d
