import pytest
from payment_risk.common.risk_scorer import calculate_optimal_operational_threshold
from payment_risk.ml.evaluator import (
    calculate_dollar_weighted_recall,
    evaluate_model_performance,
)


def test_dollar_weighted_recall():
    y_true = [1, 1, 0, 0]
    y_prob = [0.95, 0.40, 0.10, 0.05]
    amounts = [100000, 10000, 5000, 5000]  # $1,000 fraud vs $100 fraud

    dollar_rec = calculate_dollar_weighted_recall(y_true, y_prob, amounts, threshold=0.50)
    assert dollar_rec == pytest.approx(0.9091, abs=1e-3)


def test_cost_utility_threshold_optimization():
    y_true = [1, 1, 0, 0, 0]
    y_prob = [0.92, 0.85, 0.10, 0.05, 0.02]
    amounts = [20000, 50000, 5000, 5000, 5000]

    best_thresh, _, metrics = calculate_optimal_operational_threshold(
        y_true=y_true,
        y_prob=y_prob,
        amounts_cents=amounts,
        chargeback_fee_cents=1500,
        fp_friction_cost_cents=250,
    )

    assert 0.10 <= best_thresh <= 0.85
    assert metrics["dollars_saved_cents"] == (20000 + 1500) + (50000 + 1500)
    assert metrics["fn"] == 0 and metrics["fp"] == 0


def test_evaluate_model_performance_summary():
    perf = evaluate_model_performance(
        [1, 1, 0, 0],
        [0.9, 0.8, 0.1, 0.2],
        [10000, 20000, 5000, 5000],
    )
    assert perf["pr_auc"] > 0
    assert perf["dollars_saved_cents"] > 0
    assert "dollar_weighted_recall_at_90" in perf
    assert "threshold_low_challenge" in perf
    assert "threshold_high_block" in perf
