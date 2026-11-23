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


def test_monotonic_safety_escalation_ml_block_cannot_be_downgraded():
    # ML probability is high (0.85 >= threshold_block 0.70)
    # Even without rule penalties, rule evaluation cannot downgrade an ML BLOCK to APPROVE/CHALLENGE
    score, penalties, tier, action = calculate_composite_risk_score(
        ml_probability=0.85,
        is_impossible_travel=0,
        distinct_cards_per_device_10m=1,
        amount_zscore=0.0,
        merchant_risk_tier="LOW",
    )
    assert action == ACTION_BLOCK
    assert tier == TIER_CRITICAL
    assert score >= 75


def test_rule_escalation_from_approve_to_challenge_and_block():
    # Base ML probability is 0.02 (APPROVE)
    # 1. Impossible travel escalates APPROVE -> CHALLENGE_3DS
    score_1, _, tier_1, action_1 = calculate_composite_risk_score(
        ml_probability=0.02,
        is_impossible_travel=1,
    )
    assert action_1 == ACTION_CHALLENGE
    assert tier_1 == TIER_ELEVATED

    # 2. Impossible travel (+50) + High Risk Merchant (+20) + Distinct Cards (+40) escalates to BLOCK
    score_2, _, tier_2, action_2 = calculate_composite_risk_score(
        ml_probability=0.02,
        is_impossible_travel=1,
        distinct_cards_per_device_10m=5,
        merchant_risk_tier="HIGH",
    )
    assert action_2 == ACTION_BLOCK
    assert tier_2 == TIER_CRITICAL
    assert score_2 >= 75
