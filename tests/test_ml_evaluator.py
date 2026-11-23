import pytest
from payment_risk.common.risk_scorer import (
    calculate_composite_risk_score,
    calculate_optimal_operational_threshold,
    ACTION_APPROVE,
    ACTION_CHALLENGE,
    ACTION_BLOCK,
    TIER_LOW,
    TIER_CRITICAL,
)
from payment_risk.ml.evaluator import (
    calculate_confusion_matrix,
    calculate_pr_auc,
    calculate_recall_at_precision,
    evaluate_model_performance,
)


def test_confusion_matrix():
    y_true = [1, 1, 0, 0, 1]
    y_pred = [1, 0, 0, 1, 1]
    cm = calculate_confusion_matrix(y_true, y_pred)
    assert cm["tp"] == 2
    assert cm["fn"] == 1
    assert cm["fp"] == 1
    assert cm["tn"] == 1


def test_pr_auc_perfect_ranking():
    y_true = [1, 1, 1, 0, 0, 0]
    y_prob = [0.95, 0.90, 0.85, 0.20, 0.15, 0.10]
    pr_auc = calculate_pr_auc(y_true, y_prob)
    assert pr_auc >= 0.90


def test_pr_auc_zero_positives():
    y_true = [0, 0, 0]
    y_prob = [0.1, 0.2, 0.3]
    assert calculate_pr_auc(y_true, y_prob) == 0.0


def test_recall_at_90_precision():
    y_true = [1, 1, 1, 0, 0, 0]
    y_prob = [0.99, 0.98, 0.97, 0.10, 0.05, 0.01]
    recall = calculate_recall_at_precision(y_true, y_prob, min_precision=0.90)
    assert recall == 1.0


def test_cost_utility_threshold_optimization():
    # 5 transactions: 2 fraudulent ($200 and $500), 3 legitimate ($50 each)
    y_true = [1, 1, 0, 0, 0]
    y_prob = [0.92, 0.85, 0.10, 0.05, 0.02]
    amounts = [20000, 50000, 5000, 5000, 5000]  # in cents

    best_thresh, min_loss, metrics = calculate_optimal_operational_threshold(
        y_true=y_true,
        y_prob=y_prob,
        amounts_cents=amounts,
        chargeback_fee_cents=1500,
        fp_friction_cost_cents=250,
    )

    # Threshold should sit between highest negative (0.10) and lowest positive (0.85)
    assert 0.10 <= best_thresh <= 0.85
    # Model should prevent all fraud with zero false positives
    assert metrics["dollars_saved_cents"] == (20000 + 1500) + (50000 + 1500)
    assert metrics["fn"] == 0
    assert metrics["fp"] == 0


def test_hybrid_risk_scorer_actions():
    # Low probability + no penalties = APPROVE
    score_low, _, tier_low, action_low = calculate_composite_risk_score(0.05)
    assert tier_low == TIER_LOW
    assert action_low == ACTION_APPROVE

    # Impossible travel penalty (+50) + moderate probability = BLOCK / CRITICAL
    score_high, pen_high, tier_high, action_high = calculate_composite_risk_score(
        ml_probability=0.55,
        is_impossible_travel=1,
    )
    assert score_high >= 75
    assert tier_high == TIER_CRITICAL
    assert action_high == ACTION_BLOCK


def test_evaluate_model_performance_summary():
    y_true = [1, 1, 0, 0]
    y_prob = [0.9, 0.8, 0.1, 0.2]
    amounts = [10000, 20000, 5000, 5000]
    perf = evaluate_model_performance(y_true, y_prob, amounts)
    assert "pr_auc" in perf
    assert "optimal_threshold" in perf
    assert perf["dollars_saved_cents"] > 0
