from payment_risk.common.features import (
    haversine_distance_km,
    calculate_implied_speed_kmh,
    calculate_amount_zscore,
)


def test_haversine_known_distance_nyc_to_london():
    dist = haversine_distance_km(40.7128, -74.0060, 51.5074, -0.1278)
    assert 5560.0 < dist < 5600.0


def test_implied_speed_impossible_travel():
    speed = calculate_implied_speed_kmh(5570.0, 1800.0)
    assert speed > 800.0


def test_amount_zscore_anomaly_and_zero_std_guard():
    assert calculate_amount_zscore(10000, 5000, 1000) == 5.0
    assert calculate_amount_zscore(10000, 5000, 0) == 0.0
