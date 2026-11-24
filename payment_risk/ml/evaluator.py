from typing import Any, Dict, List, Tuple
from payment_risk.common.risk_scorer import (
    calculate_optimal_operational_threshold,
    calculate_3way_cost_utility_thresholds,
)


def calculate_confusion_matrix(y_true: List[int], y_pred: List[int]) -> Dict[str, int]:
    tp = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 1 and yp == 1)
    fp = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 0 and yp == 1)
    fn = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 1 and yp == 0)
    tn = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 0 and yp == 0)
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn}


def calculate_precision_recall_curve(
    y_true: List[int],
    y_prob: List[float],
    thresholds: List[float],
) -> List[Tuple[float, float, float]]:
    points = []
    total_pos = sum(y_true)
    if total_pos == 0:
        return [(0.0, 1.0, 0.0)]

    for t in thresholds:
        tp = sum(1 for yt, yp in zip(y_true, y_prob) if yt == 1 and yp >= t)
        fp = sum(1 for yt, yp in zip(y_true, y_prob) if yt == 0 and yp >= t)
        precision = tp / (tp + fp) if (tp + fp) > 0 else 1.0
        recall = tp / total_pos
        points.append((round(precision, 4), round(recall, 4), round(t, 4)))

    return points


def calculate_pr_auc(y_true: List[int], y_prob: List[float], steps: int = 100) -> float:
    """Computes exact step-wise Average Precision (PR-AUC) matching sklearn.metrics.average_precision_score."""
    total_pos = sum(y_true)
    if total_pos == 0 or len(y_true) == 0:
        return 0.0

    paired = sorted(zip(y_prob, y_true), key=lambda x: x[0], reverse=True)
    tp = 0
    fp = 0
    prev_recall = 0.0
    ap = 0.0
    idx = 0
    n = len(paired)
    while idx < n:
        score = paired[idx][0]
        while idx < n and paired[idx][0] == score:
            if paired[idx][1] == 1:
                tp += 1
            else:
                fp += 1
            idx += 1
        recall = tp / float(total_pos)
        precision = tp / float(tp + fp)
        ap += (recall - prev_recall) * precision
        prev_recall = recall

    return round(min(1.0, max(0.0, ap)), 4)


def calculate_recall_at_precision(
    y_true: List[int],
    y_prob: List[float],
    min_precision: float = 0.90,
    steps: int = 100,
) -> float:
    total_pos = sum(y_true)
    if total_pos == 0 or len(y_true) == 0:
        return 0.0

    paired = sorted(zip(y_prob, y_true), key=lambda x: x[0], reverse=True)
    tp = 0
    fp = 0
    best_recall = 0.0
    idx = 0
    n = len(paired)
    while idx < n:
        score = paired[idx][0]
        while idx < n and paired[idx][0] == score:
            if paired[idx][1] == 1:
                tp += 1
            else:
                fp += 1
            idx += 1
        precision = tp / float(tp + fp)
        if precision >= min_precision:
            best_recall = max(best_recall, round(tp / float(total_pos), 4))

    return best_recall


def calculate_dollar_weighted_recall(
    y_true: List[int],
    y_prob: List[float],
    amounts_cents: List[int],
    threshold: float,
) -> float:
    total_fraud_dollars = sum(amt for yt, amt in zip(y_true, amounts_cents) if yt == 1)
    if total_fraud_dollars == 0:
        return 0.0

    caught_fraud_dollars = sum(
        amt for yt, yp, amt in zip(y_true, y_prob, amounts_cents) if yt == 1 and yp >= threshold
    )
    return round(caught_fraud_dollars / float(total_fraud_dollars), 4)


def calculate_dollar_weighted_recall_at_precision(
    y_true: List[int],
    y_prob: List[float],
    amounts_cents: List[int],
    min_precision: float = 0.90,
    steps: int = 100,
) -> float:
    total_fraud_dollars = sum(amt for yt, amt in zip(y_true, amounts_cents) if yt == 1)
    if total_fraud_dollars == 0 or len(y_true) == 0:
        return 0.0

    paired = sorted(zip(y_prob, y_true, amounts_cents), key=lambda x: x[0], reverse=True)
    tp = 0
    fp = 0
    caught_dollars = 0
    best_dollar_recall = 0.0
    idx = 0
    n = len(paired)
    while idx < n:
        score = paired[idx][0]
        while idx < n and paired[idx][0] == score:
            if paired[idx][1] == 1:
                tp += 1
                caught_dollars += paired[idx][2]
            else:
                fp += 1
            idx += 1
        precision = tp / float(tp + fp)
        if precision >= min_precision:
            best_dollar_recall = max(
                best_dollar_recall, round(caught_dollars / float(total_fraud_dollars), 4)
            )

    return best_dollar_recall


def calculate_brier_score(y_true: List[int], y_prob: List[float]) -> float:
    if not y_true:
        return 0.0
    mse = sum((yp - yt) ** 2 for yt, yp in zip(y_true, y_prob)) / len(y_true)
    return round(mse, 4)


def evaluate_model_performance(
    y_true: List[int],
    y_prob: List[float],
    amounts_cents: List[int],
) -> Dict[str, Any]:
    pr_auc = calculate_pr_auc(y_true, y_prob)
    rec_at_90 = calculate_recall_at_precision(y_true, y_prob, min_precision=0.90)
    dollar_rec_at_90 = calculate_dollar_weighted_recall_at_precision(
        y_true, y_prob, amounts_cents, min_precision=0.90
    )
    brier_score = calculate_brier_score(y_true, y_prob)

    best_thresh, min_loss, cost_metrics = calculate_optimal_operational_threshold(
        y_true=y_true,
        y_prob=y_prob,
        amounts_cents=amounts_cents,
    )

    dollar_rec_optimal = calculate_dollar_weighted_recall(
        y_true, y_prob, amounts_cents, threshold=best_thresh
    )

    return {
        "pr_auc": pr_auc,
        "recall_at_90_precision": rec_at_90,
        "dollar_weighted_recall_at_90": dollar_rec_at_90,
        "dollar_weighted_recall_at_optimal": dollar_rec_optimal,
        "brier_calibration_score": brier_score,
        "optimal_threshold": best_thresh,
        "threshold_low_challenge": cost_metrics.get("threshold_low_challenge", 0.35),
        "threshold_high_block": cost_metrics.get("threshold_high_block", 0.70),
        "min_expected_loss_cents": min_loss,
        "dollars_saved_cents": cost_metrics.get("dollars_saved_cents", 0),
        "f1_at_optimal": cost_metrics.get("f1", 0.0),
        "precision_at_optimal": cost_metrics.get("precision", 0.0),
        "recall_at_optimal": cost_metrics.get("recall", 0.0),
        "tp": cost_metrics.get("tp", 0),
        "fp": cost_metrics.get("fp", 0),
        "fn": cost_metrics.get("fn", 0),
        "tn": cost_metrics.get("tn", 0),
        "three_way_cost_metrics": cost_metrics.get("three_way_cost_metrics", {}),
    }
