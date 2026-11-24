from payment_risk.common.risk_scorer import (
    calculate_composite_risk_score,
    ACTION_APPROVE,
    ACTION_CHALLENGE,
    ACTION_BLOCK,
    TIER_LOW,
    TIER_ELEVATED,
    TIER_CRITICAL,
)


def test_low_risk_transaction():
    score, penalties, tier, action = calculate_composite_risk_score(
        ml_probability=0.05,
        is_impossible_travel=0,
        distinct_cards_per_device_10m=1,
        amount_zscore=0.2,
        merchant_risk_tier="LOW",
    )
    assert score < 40 and penalties == 0
    assert tier == TIER_LOW and action == ACTION_APPROVE


def test_impossible_travel_plus_spend_anomaly_triggers_challenge():
    score, penalties, tier, action = calculate_composite_risk_score(
        ml_probability=0.10,
        is_impossible_travel=1,
        distinct_cards_per_device_10m=1,
        amount_zscore=4.0,
        merchant_risk_tier="LOW",
    )
    assert penalties == 45 and score >= 40
    assert tier == TIER_ELEVATED and action == ACTION_CHALLENGE


def test_monotonic_safety_escalation_ml_block_cannot_be_downgraded():
    score, _, tier, action = calculate_composite_risk_score(
        ml_probability=0.85,
        is_impossible_travel=0,
        distinct_cards_per_device_10m=1,
        amount_zscore=0.0,
        merchant_risk_tier="LOW",
    )
    assert action == ACTION_BLOCK and tier == TIER_CRITICAL and score >= 75
