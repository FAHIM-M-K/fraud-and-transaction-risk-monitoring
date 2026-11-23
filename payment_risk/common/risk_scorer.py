from typing import Any, Dict, List, Tuple

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


def calculate_optimal_operational_threshold(
    y_true: List[int],
    y_prob: List[float],
    amounts_cents: List[int],
    chargeback_fee_cents: int = 1500,
    fp_friction_cost_cents: int = 250,
    steps: int = 100,
) -> Tuple[float, float, Dict[str, Any]]:
    baseline_loss = sum(amt + chargeback_fee_cents for yt, amt in zip(y_true, amounts_cents) if yt == 1)

    best_threshold = 0.5
    min_loss = float("inf")
    best_metrics: Dict[str, Any] = {}

    for i in range(1, steps):
        t = i / float(steps)
        fn_loss = 0
        fp_loss = 0
        tp_count = 0
        fp_count = 0
        fn_count = 0
        tn_count = 0

        for yt, yp, amt in zip(y_true, y_prob, amounts_cents):
            pred = 1 if yp >= t else 0
            if yt == 1 and pred == 0:
                fn_loss += amt + chargeback_fee_cents
                fn_count += 1
            elif yt == 0 and pred == 1:
                fp_loss += fp_friction_cost_cents
                fp_count += 1
            elif yt == 1 and pred == 1:
                tp_count += 1
            else:
                tn_count += 1

        total_loss = fn_loss + fp_loss
        if total_loss < min_loss:
            min_loss = total_loss
            best_threshold = round(t, 4)
            precision = tp_count / (tp_count + fp_count) if (tp_count + fp_count) > 0 else 0.0
            recall = tp_count / (tp_count + fn_count) if (tp_count + fn_count) > 0 else 0.0
            f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

            best_metrics = {
                "threshold": best_threshold,
                "total_loss_cents": total_loss,
                "baseline_loss_cents": baseline_loss,
                "dollars_saved_cents": baseline_loss - total_loss,
                "precision": round(precision, 4),
                "recall": round(recall, 4),
                "f1": round(f1, 4),
                "tp": tp_count,
                "fp": fp_count,
                "fn": fn_count,
                "tn": tn_count,
            }

    return best_threshold, float(min_loss), best_metrics

