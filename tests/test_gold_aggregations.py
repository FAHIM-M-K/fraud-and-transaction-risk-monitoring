import pytest
from payment_risk.common.metrics import (
    calculate_chargeback_rate_bps,
    calculate_rate,
    evaluate_merchant_compliance_tier,
    classify_transaction_alert,
)


def test_calculate_chargeback_rate_bps_standard():
    # 5,000 cents fraud out of 500,000 cents volume = 1% = 100 bps
    bps = calculate_chargeback_rate_bps(5000, 500000)
    assert bps == 100.0


def test_calculate_chargeback_rate_bps_zero_volume():
    bps = calculate_chargeback_rate_bps(5000, 0)
    assert bps == 0.0


def test_calculate_rate_decline():
    # 25 declines out of 1000 transactions = 0.025
    rate = calculate_rate(25, 1000)
    assert rate == 0.025


def test_calculate_rate_zero_denominator():
    assert calculate_rate(5, 0) == 0.0


def test_merchant_compliance_low_tier():
    is_excessive, is_critical, tier = evaluate_merchant_compliance_tier(0.45)
    assert not is_excessive
    assert not is_critical
    assert tier == "LOW"


def test_merchant_compliance_warning_tier():
    # Dispute rate >= 0.9% triggers Visa VAMP / Mastercard excessive dispute warning
    is_excessive, is_critical, tier = evaluate_merchant_compliance_tier(1.15)
    assert is_excessive
    assert not is_critical
    assert tier == "MEDIUM"


def test_merchant_compliance_critical_tier():
    # Dispute rate >= 1.5% triggers high-risk / immediate fine tier
    is_excessive, is_critical, tier = evaluate_merchant_compliance_tier(1.92)
    assert is_excessive
    assert is_critical
    assert tier == "HIGH"


def test_classify_alert_impossible_travel():
    res = classify_transaction_alert(
        speed_kmh=920.0,
        distance_km=450.0,
        amount_zscore=0.8,
        tx_count_1h=1,
        is_avs_mismatch=False,
        is_cvv_mismatch=False,
    )
    assert res is not None
    alert_type, severity, reason = res
    assert alert_type == "IMPOSSIBLE_TRAVEL"
    assert severity == "CRITICAL"
    assert "exceeds physical travel limits" in reason


def test_classify_alert_spend_surge():
    res = classify_transaction_alert(
        speed_kmh=35.0,
        distance_km=15.0,
        amount_zscore=4.25,
        tx_count_1h=1,
        is_avs_mismatch=False,
        is_cvv_mismatch=False,
    )
    assert res is not None
    alert_type, severity, reason = res
    assert alert_type == "SPEND_SURGE"
    assert severity == "HIGH"
    assert "deviated 4.25 standard deviations" in reason


def test_classify_alert_velocity_burst():
    res = classify_transaction_alert(
        speed_kmh=0.0,
        distance_km=0.0,
        amount_zscore=1.1,
        tx_count_1h=7,
        is_avs_mismatch=True,
        is_cvv_mismatch=False,
    )
    assert res is not None
    alert_type, severity, reason = res
    assert alert_type == "VELOCITY_BURST_MISMATCH"
    assert severity == "HIGH"
    assert "High velocity burst (7 tx/1h)" in reason


def test_classify_alert_benign_transaction():
    res = classify_transaction_alert(
        speed_kmh=45.0,
        distance_km=10.0,
        amount_zscore=0.5,
        tx_count_1h=1,
        is_avs_mismatch=False,
        is_cvv_mismatch=False,
    )
    assert res is None
