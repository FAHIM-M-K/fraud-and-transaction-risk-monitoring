from typing import Any, Dict, List, Tuple
from payment_risk.common.risk_scorer import calculate_optimal_operational_threshold


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
    total_pos = sum(y_true)
    if total_pos == 0 or len(y_true) == 0:
        return 0.0

    thresholds = [i / float(steps) for i in range(steps + 1)]
    pr_points = calculate_precision_recall_curve(y_true, y_prob, thresholds)

    # Sort by recall ascending
    pr_points.sort(key=lambda x: x[1])

    area = 0.0
    for i in range(1, len(pr_points)):
        r0 = pr_points[i - 1][1]
        r1 = pr_points[i][1]
        p0 = pr_points[i - 1][0]
        p1 = pr_points[i][0]
        width = r1 - r0
        if width > 0:
            avg_p = (p0 + p1) / 2.0
            area += width * avg_p

    return round(min(1.0, max(0.0, area)), 4)


def calculate_recall_at_precision(
    y_true: List[int],
    y_prob: List[float],
    min_precision: float = 0.90,
    steps: int = 100,
) -> float:
    thresholds = [i / float(steps) for i in range(steps + 1)]
    pr_points = calculate_precision_recall_curve(y_true, y_prob, thresholds)

    qualifying_recalls = [rec for prec, rec, _ in pr_points if prec >= min_precision]
    if qualifying_recalls:
        return max(qualifying_recalls)
    return 0.0


def evaluate_model_performance(
    y_true: List[int],
    y_prob: List[float],
    amounts_cents: List[int],
) -> Dict[str, Any]:
    pr_auc = calculate_pr_auc(y_true, y_prob)
    rec_at_90 = calculate_recall_at_precision(y_true, y_prob, min_precision=0.90)

    best_thresh, min_loss, cost_metrics = calculate_optimal_operational_threshold(
        y_true=y_true,
        y_prob=y_prob,
        amounts_cents=amounts_cents,
    )

    return {
        "pr_auc": pr_auc,
        "recall_at_90_precision": rec_at_90,
        "optimal_threshold": best_thresh,
        "min_expected_loss_cents": min_loss,
        "dollars_saved_cents": cost_metrics.get("dollars_saved_cents", 0),
        "f1_at_optimal": cost_metrics.get("f1", 0.0),
        "precision_at_optimal": cost_metrics.get("precision", 0.0),
        "recall_at_optimal": cost_metrics.get("recall", 0.0),
        "tp": cost_metrics.get("tp", 0),
        "fp": cost_metrics.get("fp", 0),
        "fn": cost_metrics.get("fn", 0),
        "tn": cost_metrics.get("tn", 0),
    }
