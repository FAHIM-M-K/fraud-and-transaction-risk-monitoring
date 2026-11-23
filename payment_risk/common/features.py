import math
from typing import Optional


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    if None in (lat1, lon1, lat2, lon2):
        return 0.0
    r = 6371.0
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
    if seconds_elapsed <= 0 or distance_km <= 0:
        return 0.0
    hours = seconds_elapsed / 3600.0
    return distance_km / hours


def calculate_amount_zscore(
    amount: float,
    avg_amount_30d: Optional[float],
    std_amount_30d: Optional[float],
) -> float:
    if avg_amount_30d is None or std_amount_30d is None or std_amount_30d <= 0.0:
        return 0.0
    return (amount - avg_amount_30d) / std_amount_30d
