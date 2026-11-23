from typing import Any, Dict, List, Tuple

ACTION_APPROVE: str = "APPROVE"
ACTION_CHALLENGE: str = "CHALLENGE_3DS"
ACTION_BLOCK: str = "BLOCK"

TIER_LOW: str = "LOW"
TIER_ELEVATED: str = "ELEVATED"
TIER_CRITICAL: str = "CRITICAL"

ACTION_HIERARCHY: Dict[str, int] = {
    ACTION_APPROVE: 1,
    ACTION_CHALLENGE: 2,
    ACTION_BLOCK: 3,
}


def calculate_composite_risk_score(
    ml_probability: float,
    is_impossible_travel: int = 0,
    distinct_cards_per_device_10m: int = 1,
    amount_zscore: float = 0.0,
    merchant_risk_tier: str = "LOW",
    threshold_challenge: float = 0.40,
    threshold_block: float = 0.80,
) -> Tuple[int, int, str, str]:
    prob = max(0.0, min(1.0, float(ml_probability)))

    if prob >= threshold_block:
        base_action = ACTION_BLOCK
    elif prob >= threshold_challenge:
        base_action = ACTION_CHALLENGE
    else:
        base_action = ACTION_APPROVE

    rule_penalties = 0

    if is_impossible_travel == 1:
        rule_penalties += 20

    if distinct_cards_per_device_10m >= 5:
        rule_penalties += 35

    if amount_zscore > 3.5:
        rule_penalties += 25

    if str(merchant_risk_tier).upper() == "HIGH":
        rule_penalties += 20

    raw_score = int(round(prob * 60.0)) + rule_penalties
    composite_score = max(0, min(100, raw_score))

    if composite_score >= 75:
        score_action = ACTION_BLOCK
    elif composite_score >= 40:
        score_action = ACTION_CHALLENGE
    else:
        score_action = ACTION_APPROVE

    if ACTION_HIERARCHY[score_action] > ACTION_HIERARCHY[base_action]:
        final_action = score_action
    else:
        final_action = base_action

    if final_action == ACTION_BLOCK:
        composite_score = max(composite_score, 75)
        risk_tier = TIER_CRITICAL
    elif final_action == ACTION_CHALLENGE:
        composite_score = max(composite_score, 40)
        risk_tier = TIER_ELEVATED
    else:
        risk_tier = TIER_LOW

    return composite_score, rule_penalties, risk_tier, final_action


def calculate_3way_cost_utility_thresholds(
    y_true: List[int],
    y_prob: List[float],
    amounts_cents: List[int],
    chargeback_fee_cents: int = 1500,
    fp_challenge_friction_cents: int = 250,
    fp_block_insult_cents: int = 1500,
    step: float = 0.05,
) -> Tuple[float, float, float, Dict[str, Any]]:
    baseline_loss = sum(amt + chargeback_fee_cents for yt, amt in zip(y_true, amounts_cents) if yt == 1)

    best_t_low = 0.35
    best_t_high = 0.70
    min_cost = float("inf")
    best_metrics: Dict[str, Any] = {}

    threshold_grid = [round(i * step, 2) for i in range(1, int(1.0 / step))]

    for t_low in threshold_grid:
        for t_high in threshold_grid:
            if t_low >= t_high:
                continue

            total_cost = 0
            n_approve = 0
            n_challenge = 0
            n_block = 0
            fraud_blocked = 0
            fraud_challenged = 0
            fraud_missed = 0
            fp_challenged = 0
            fp_blocked = 0

            for yt, yp, amt in zip(y_true, y_prob, amounts_cents):
                if yp >= t_high:
                    n_block += 1
                    if yt == 1:
                        fraud_blocked += 1
                    else:
                        fp_blocked += 1
                        total_cost += fp_block_insult_cents
                elif yp >= t_low:
                    n_challenge += 1
                    if yt == 1:
                        fraud_challenged += 1
                    else:
                        fp_challenged += 1
                        total_cost += fp_challenge_friction_cents
                else:
                    n_approve += 1
                    if yt == 1:
                        fraud_missed += 1
                        total_cost += amt + chargeback_fee_cents

            if total_cost < min_cost:
                min_cost = total_cost
                best_t_low = t_low
                best_t_high = t_high
                dollars_saved = baseline_loss - total_cost
                best_metrics = {
                    "threshold_low_challenge": best_t_low,
                    "threshold_high_block": best_t_high,
                    "min_loss_cents": total_cost,
                    "baseline_loss_cents": baseline_loss,
                    "dollars_saved_cents": dollars_saved,
                    "n_approve": n_approve,
                    "n_challenge": n_challenge,
                    "n_block": n_block,
                    "fraud_blocked": fraud_blocked,
                    "fraud_challenged": fraud_challenged,
                    "fraud_missed": fraud_missed,
                    "fp_challenged": fp_challenged,
                    "fp_blocked": fp_blocked,
                }

    return best_t_low, best_t_high, float(min_cost), best_metrics


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

    # Also compute 3-way thresholds for enriched decisioning
    t_low, t_high, three_way_loss, three_way_metrics = calculate_3way_cost_utility_thresholds(
        y_true=y_true,
        y_prob=y_prob,
        amounts_cents=amounts_cents,
        chargeback_fee_cents=chargeback_fee_cents,
        fp_challenge_friction_cents=fp_friction_cost_cents,
    )
    best_metrics["threshold_low_challenge"] = t_low
    best_metrics["threshold_high_block"] = t_high
    best_metrics["three_way_cost_metrics"] = three_way_metrics

    return best_threshold, float(min_loss), best_metrics

