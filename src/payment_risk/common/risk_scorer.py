"""Hybrid composite risk scoring engine.

Combines ML probabilities with deterministic rule penalties to classify risk tiers
and dictate real-time operational routing actions (APPROVE, CHALLENGE_3DS, BLOCK).
"""

from typing import Tuple

ACTION_APPROVE: str = "APPROVE"
ACTION_CHALLENGE: str = "CHALLENGE_3DS"
ACTION_BLOCK: str = "BLOCK"

TIER_LOW: str = "LOW"
TIER_ELEVATED: str = "ELEVATED"
TIER_CRITICAL: str = "CRITICAL"


def calculate_composite_risk_score(
    ml_probability: float,
    is_impossible_travel: int = 0,
    distinct_cards_per_device_10m: int = 1,
    amount_zscore: float = 0.0,
    merchant_risk_tier: str = "LOW",
) -> Tuple[int, int, str, str]:
    """Computes composite hybrid risk score (0-100), rule penalty points,

    risk tier (LOW/ELEVATED/CRITICAL), and operational action (APPROVE/CHALLENGE_3DS/BLOCK).

    Formula:
        Composite Score = min(100, int(ml_probability * 60 + rule_penalties))

    Rule Penalties:
        +50 pts: is_impossible_travel == 1 (implied velocity > 800 km/h)
        +40 pts: distinct_cards_per_device_10m >= 5 (card testing cluster)
        +30 pts: amount_zscore > 3.5 (extreme spend deviation)
        +20 pts: merchant_risk_tier == 'HIGH'

    Routing Decision Matrix:
        Score >= 75: CRITICAL -> BLOCK
        Score >= 40: ELEVATED -> CHALLENGE_3DS
        Score <  40: LOW      -> APPROVE

    Returns:
        Tuple of (composite_score, rule_penalties, risk_tier, operational_action)
    """
    rule_penalties = 0

    if is_impossible_travel == 1:
        rule_penalties += 50

    if distinct_cards_per_device_10m >= 5:
        rule_penalties += 40

    if amount_zscore > 3.5:
        rule_penalties += 30

    if str(merchant_risk_tier).upper() == "HIGH":
        rule_penalties += 20

    raw_score = int(round(ml_probability * 60.0)) + rule_penalties
    composite_score = max(0, min(100, raw_score))

    if composite_score >= 75:
        risk_tier = TIER_CRITICAL
        action = ACTION_BLOCK
    elif composite_score >= 40:
        risk_tier = TIER_ELEVATED
        action = ACTION_CHALLENGE
    else:
        risk_tier = TIER_LOW
        action = ACTION_APPROVE

    return composite_score, rule_penalties, risk_tier, action
