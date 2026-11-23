import pytest
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
    assert score < 40
    assert penalties == 0
    assert tier == TIER_LOW
    assert action == ACTION_APPROVE


def test_impossible_travel_triggers_elevated_or_critical():
    score, penalties, tier, action = calculate_composite_risk_score(
        ml_probability=0.10,
        is_impossible_travel=1,
        distinct_cards_per_device_10m=1,
        amount_zscore=0.5,
        merchant_risk_tier="LOW",
    )
    assert penalties >= 50
    assert score >= 50
    assert tier in (TIER_ELEVATED, TIER_CRITICAL)
    assert action in (ACTION_CHALLENGE, ACTION_BLOCK)


def test_critical_multi_rule_burst():
    score, penalties, tier, action = calculate_composite_risk_score(
        ml_probability=0.75,
        is_impossible_travel=1,
        distinct_cards_per_device_10m=6,
        amount_zscore=4.2,
        merchant_risk_tier="HIGH",
    )
    assert score == 100
    assert tier == TIER_CRITICAL
    assert action == ACTION_BLOCK
