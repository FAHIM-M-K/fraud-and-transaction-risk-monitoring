from typing import Optional, Tuple


def calculate_chargeback_rate_bps(fraud_volume_cents: int, total_volume_cents: int) -> float:
    if total_volume_cents <= 0:
        return 0.0
    return round((fraud_volume_cents / total_volume_cents) * 10000.0, 2)


def calculate_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def evaluate_merchant_compliance_tier(dispute_rate_pct: float) -> Tuple[bool, bool, str]:
    is_critical = dispute_rate_pct >= 1.5
    is_excessive = dispute_rate_pct >= 0.9
    if is_critical:
        tier = "HIGH"
    elif is_excessive:
        tier = "MEDIUM"
    else:
        tier = "LOW"
    return is_excessive, is_critical, tier


def classify_transaction_alert(
    speed_kmh: float,
    distance_km: float,
    amount_zscore: float,
    tx_count_1h: int,
    is_avs_mismatch: bool,
    is_cvv_mismatch: bool,
) -> Optional[Tuple[str, str, str]]:
    if speed_kmh > 800.0 and distance_km > 100.0:
        return (
            "IMPOSSIBLE_TRAVEL",
            "CRITICAL",
            f"Implied speed of {speed_kmh:.1f} km/h across {distance_km:.1f} km exceeds physical travel limits",
        )
    if amount_zscore > 3.0:
        return (
            "SPEND_SURGE",
            "HIGH",
            f"Transaction amount deviated {amount_zscore:.2f} standard deviations from customer 30-day baseline",
        )
    if tx_count_1h >= 5 and (is_avs_mismatch or is_cvv_mismatch):
        return (
            "VELOCITY_BURST_MISMATCH",
            "HIGH",
            f"High velocity burst ({tx_count_1h} tx/1h) with billing verification mismatch",
        )
    return None
