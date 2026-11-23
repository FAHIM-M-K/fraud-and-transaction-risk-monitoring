from typing import Any, Dict, Optional
import json
import numpy as np
import pandas as pd
from payment_risk.common.config import LakehouseConfig
from payment_risk.ml.dataset import (
    NUMERICAL_FEATURES,
    CATEGORICAL_FEATURES,
    TARGET_COL,
    prepare_feature_dataframe,
    split_oot_dataset,
)
from payment_risk.ml.evaluator import evaluate_model_performance


def train_fraud_detection_model(
    spark: Any,
    config: LakehouseConfig,
    experiment_name: str = "/Shared/payment_risk_fraud_ml",
    model_name: Optional[str] = None,
) -> Dict[str, Any]:
    import mlflow
    import mlflow.lightgbm
    import lightgbm as lgb

    target_model_name = model_name or f"{config.catalog_name}.ml.fraud_detector"

    # 1. Prepare Enriched Training Data from Lakehouse Silver Layer
    df_features = prepare_feature_dataframe(spark, config)
    df_train_spark, df_test_spark, split_date = split_oot_dataset(df_features)

    feature_cols = NUMERICAL_FEATURES + CATEGORICAL_FEATURES
    all_cols = feature_cols + [TARGET_COL, "amount_cents", "tx_id"]

    train_pdf = df_train_spark.select(*all_cols).toPandas()
    test_pdf = df_test_spark.select(*all_cols).toPandas()

    for col in CATEGORICAL_FEATURES:
        train_pdf[col] = train_pdf[col].astype("category")
        test_pdf[col] = test_pdf[col].astype("category")

    X_train = train_pdf[feature_cols]
    y_train = train_pdf[TARGET_COL].astype(int)

    X_test = test_pdf[feature_cols]
    y_test = test_pdf[TARGET_COL].astype(int)
    amounts_test = test_pdf["amount_cents"].tolist()

    pos_count = int(y_train.sum())
    neg_count = len(y_train) - pos_count
    scale_weight = float(neg_count) / max(1.0, float(pos_count))

    # 2. Configure LightGBM with Domain-Calibrated Fraud Parameters
    params = {
        "objective": "binary",
        "metric": "auc_mu",
        "boosting_type": "gbdt",
        "n_estimators": 150,
        "learning_rate": 0.05,
        "num_leaves": 31,
        "max_depth": 6,
        "min_child_samples": 20,
        "scale_pos_weight": scale_weight,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42,
        "n_jobs": -1,
    }

    try:
        mlflow.set_experiment(experiment_name)
    except Exception:
        pass

    with mlflow.start_run(run_name="fraud_lightgbm_champion") as run:
        mlflow.log_params(params)
        mlflow.log_param("train_samples", len(X_train))
        mlflow.log_param("test_samples", len(X_test))
        mlflow.log_param("train_fraud_count", pos_count)
        mlflow.log_param("test_fraud_count", int(y_test.sum()))
        mlflow.log_param("temporal_split_date", split_date)

        model = lgb.LGBMClassifier(**params)
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_test, y_test)],
            callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)],
        )

        # 3. Model Predictions & Probabilities
        y_prob = model.predict_proba(X_test)[:, 1].tolist()
        y_true = y_test.tolist()

        # 4. Evaluate Expert Metrics
        metrics = evaluate_model_performance(y_true, y_prob, amounts_test)
        mlflow.log_metrics({
            "pr_auc": metrics["pr_auc"],
            "recall_at_90_precision": metrics["recall_at_90_precision"],
            "optimal_threshold": metrics["optimal_threshold"],
            "f1_score": metrics["f1_at_optimal"],
            "precision": metrics["precision_at_optimal"],
            "recall": metrics["recall_at_optimal"],
            "dollars_saved_cents": float(metrics["dollars_saved_cents"]),
        })

        # 5. Log Feature Importances
        importance_dict = dict(zip(feature_cols, model.feature_importances_.tolist()))
        sorted_importance = dict(sorted(importance_dict.items(), key=lambda item: item[1], reverse=True))
        mlflow.log_dict(sorted_importance, "feature_importance.json")

        # 6. Log Model Artifact & Register in Unity Catalog / MLflow Registry
        model_info = mlflow.lightgbm.log_model(
            lgb_model=model,
            artifact_path="model",
            registered_model_name=target_model_name,
        )

        return {
            "run_id": run.info.run_id,
            "model_uri": model_info.model_uri,
            "registered_model_name": target_model_name,
            "temporal_split_date": split_date,
            "metrics": metrics,
            "top_features": list(sorted_importance.keys())[:5],
        }
