import pytest
from payment_risk.common.metrics import (
    calculate_chargeback_rate_bps,
    evaluate_merchant_compliance_tier,
    classify_transaction_alert,
)


def test_calculate_chargeback_rate_bps():
    assert calculate_chargeback_rate_bps(5000, 500000) == 100.0
    assert calculate_chargeback_rate_bps(5000, 0) == 0.0


@pytest.mark.parametrize(
    "dispute_rate_pct,expected_excessive,expected_critical,expected_tier",
    [
        (0.45, False, False, "LOW"),
        (1.15, True, False, "MEDIUM"),
        (1.92, True, True, "HIGH"),
    ],
)
def test_merchant_compliance_vfmp_tiers(
    dispute_rate_pct, expected_excessive, expected_critical, expected_tier
):
    is_excessive, is_critical, tier = evaluate_merchant_compliance_tier(dispute_rate_pct)
    assert is_excessive is expected_excessive
    assert is_critical is expected_critical
    assert tier == expected_tier


def test_classify_transaction_alert():
    travel = classify_transaction_alert(920.0, 450.0, 0.8, 1, False, False)
    assert travel is not None and travel[0] == "IMPOSSIBLE_TRAVEL" and travel[1] == "CRITICAL"

    surge = classify_transaction_alert(35.0, 15.0, 4.25, 1, False, False)
    assert surge is not None and surge[0] == "SPEND_SURGE" and surge[1] == "HIGH"

    assert classify_transaction_alert(45.0, 10.0, 0.5, 1, False, False) is None
