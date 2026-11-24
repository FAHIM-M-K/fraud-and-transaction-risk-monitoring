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
    maturation_days: int = 0,
) -> Dict[str, Any]:
    import mlflow
    try:
        import lightgbm as lgb
        import xgboost as xgb
        from sklearn.ensemble import IsolationForest
        import mlflow.lightgbm
        import mlflow.xgboost
    except ModuleNotFoundError as e:
        raise ImportError(
            f"Missing required ML dependency: {e.name}. "
            "Please ensure lightgbm, xgboost, and scikit-learn are installed in the environment."
        ) from e

    target_model_name = model_name or f"{config.catalog_name}.ml.fraud_detector"

    df_features = prepare_feature_dataframe(spark, config, maturation_days=maturation_days)
    df_train_spark, df_test_spark, split_date = split_oot_dataset(df_features)

    num_base = [f for f in NUMERICAL_FEATURES if f != "iso_anomaly_score"]
    feature_cols = num_base + CATEGORICAL_FEATURES
    all_cols = list(dict.fromkeys(feature_cols + [TARGET_COL, "amount_cents", "tx_id"]))

    train_pdf = df_train_spark.select(*all_cols).toPandas()
    test_pdf = df_test_spark.select(*all_cols).toPandas()

    train_pdf = train_pdf.loc[:, ~train_pdf.columns.duplicated()]
    test_pdf = test_pdf.loc[:, ~test_pdf.columns.duplicated()]

    for col in CATEGORICAL_FEATURES:
        train_pdf[col] = train_pdf[col].astype("category")
        test_pdf[col] = pd.Categorical(test_pdf[col], categories=train_pdf[col].cat.categories)

    iso_model = IsolationForest(
        n_estimators=100,
        contamination=0.03,
        random_state=42,
        n_jobs=-1,
    )
    train_num_imputed = train_pdf[num_base].fillna(0.0)
    test_num_imputed = test_pdf[num_base].fillna(0.0)
    iso_model.fit(train_num_imputed)

    train_pdf["iso_anomaly_score"] = -iso_model.score_samples(train_num_imputed)
    test_pdf["iso_anomaly_score"] = -iso_model.score_samples(test_num_imputed)

    full_feature_cols = NUMERICAL_FEATURES + CATEGORICAL_FEATURES

    X_train = train_pdf[full_feature_cols]
    y_train = train_pdf[TARGET_COL].astype(int)

    X_test = test_pdf[full_feature_cols]
    y_test = test_pdf[TARGET_COL].astype(int)
    amounts_test = test_pdf["amount_cents"].tolist() if isinstance(test_pdf["amount_cents"], pd.Series) else test_pdf["amount_cents"].iloc[:, 0].tolist()

    pos_count = int(y_train.sum())
    neg_count = len(y_train) - pos_count
    scale_weight = float(neg_count) / max(1.0, float(pos_count))

    try:
        mlflow.set_experiment(experiment_name)
    except Exception:
        pass

    with mlflow.start_run(run_name="fraud_model_bakeoff") as bakeoff_run:
        mlflow.log_param("train_samples", len(X_train))
        mlflow.log_param("test_samples", len(X_test))
        mlflow.log_param("train_fraud_count", pos_count)
        mlflow.log_param("test_fraud_count", int(y_test.sum()))
        mlflow.log_param("temporal_split_date", split_date)
        mlflow.log_param("maturation_days", maturation_days)

        lgb_params = {
            "objective": "binary",
            "metric": "auc",
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
        lgb_model = lgb.LGBMClassifier(**lgb_params)
        lgb_model.fit(
            X_train,
            y_train,
            eval_set=[(X_test, y_test)],
            callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)],
        )
        lgb_prob = lgb_model.predict_proba(X_test)[:, 1].tolist()
        lgb_metrics = evaluate_model_performance(y_test.tolist(), lgb_prob, amounts_test)

        xgb_params = {
            "objective": "binary:logistic",
            "eval_metric": "aucpr",
            "n_estimators": 150,
            "learning_rate": 0.05,
            "max_depth": 6,
            "scale_pos_weight": scale_weight,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "random_state": 42,
            "enable_categorical": True,
            "n_jobs": -1,
        }
        xgb_model = xgb.XGBClassifier(**xgb_params)
        xgb_model.fit(
            X_train,
            y_train,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )
        xgb_prob = xgb_model.predict_proba(X_test)[:, 1].tolist()
        xgb_metrics = evaluate_model_performance(y_test.tolist(), xgb_prob, amounts_test)

        lgb_score = (
            lgb_metrics["dollar_weighted_recall_at_90"] * 0.6
            + lgb_metrics["pr_auc"] * 0.4
            - lgb_metrics["brier_calibration_score"] * 0.1
        )
        xgb_score = (
            xgb_metrics["dollar_weighted_recall_at_90"] * 0.6
            + xgb_metrics["pr_auc"] * 0.4
            - xgb_metrics["brier_calibration_score"] * 0.1
        )

        if xgb_score > lgb_score:
            champion_name = "XGBoost"
            champion_model = xgb_model
            champion_metrics = xgb_metrics
            champion_log_func = mlflow.xgboost.log_model
            model_kwarg = {"xgb_model": champion_model}
            feature_importances = dict(zip(full_feature_cols, champion_model.feature_importances_.tolist()))
        else:
            champion_name = "LightGBM"
            champion_model = lgb_model
            champion_metrics = lgb_metrics
            champion_log_func = mlflow.lightgbm.log_model
            model_kwarg = {"lgb_model": champion_model}
            feature_importances = dict(zip(full_feature_cols, champion_model.feature_importances_.tolist()))

        mlflow.log_metrics({
            "champion_is_xgboost": 1 if champion_name == "XGBoost" else 0,
            "lgb_pr_auc": lgb_metrics["pr_auc"],
            "lgb_dollar_recall_at_90": lgb_metrics["dollar_weighted_recall_at_90"],
            "lgb_brier_score": lgb_metrics["brier_calibration_score"],
            "xgb_pr_auc": xgb_metrics["pr_auc"],
            "xgb_dollar_recall_at_90": xgb_metrics["dollar_weighted_recall_at_90"],
            "xgb_brier_score": xgb_metrics["brier_calibration_score"],
            "champion_pr_auc": champion_metrics["pr_auc"],
            "champion_dollar_recall_at_90": champion_metrics["dollar_weighted_recall_at_90"],
            "champion_optimal_threshold": champion_metrics["optimal_threshold"],
            "champion_dollars_saved_cents": float(champion_metrics["dollars_saved_cents"]),
        })

        sorted_importance = dict(sorted(feature_importances.items(), key=lambda item: item[1], reverse=True))
        mlflow.log_dict(sorted_importance, "champion_feature_importance.json")

        bakeoff_summary = {
            "champion": champion_name,
            "lightgbm_metrics": lgb_metrics,
            "xgboost_metrics": xgb_metrics,
            "selection_criteria": "dollar_weighted_recall_at_90 (60%) + pr_auc (40%) - brier (10%)",
        }
        mlflow.log_dict(bakeoff_summary, "bakeoff_summary.json")

        try:
            model_info = champion_log_func(
                artifact_path="model",
                registered_model_name=target_model_name,
                **model_kwarg,
            )
        except Exception:
            model_info = champion_log_func(
                artifact_path="model",
                **model_kwarg,
            )

        return {
            "run_id": bakeoff_run.info.run_id,
            "champion": champion_name,
            "model_uri": model_info.model_uri,
            "registered_model_name": target_model_name,
            "temporal_split_date": split_date,
            "metrics": champion_metrics,
            "bakeoff_summary": bakeoff_summary,
            "top_features": list(sorted_importance.keys())[:5],
        }
