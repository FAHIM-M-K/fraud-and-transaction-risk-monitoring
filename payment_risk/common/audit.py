import hashlib
import json
from typing import Any, Dict, List, Tuple


def verify_row_algebra(
    bronze_count: int,
    silver_count: int,
    quarantine_count: int,
) -> Tuple[bool, str]:
    total_accounted = silver_count + quarantine_count
    discrepancy = bronze_count - total_accounted
    if discrepancy == 0:
        return True, f"Reconciliation passed: {bronze_count:,} Bronze = {silver_count:,} Silver + {quarantine_count:,} Quarantine"
    return False, f"Reconciliation failed: {bronze_count:,} Bronze != {silver_count:,} Silver + {quarantine_count:,} Quarantine (Discrepancy: {discrepancy:,})"


def verify_monetary_reconciliation(
    bronze_total_cents: int,
    silver_total_cents: int,
    quarantine_total_cents: int,
) -> Tuple[bool, str]:
    expected_cents = silver_total_cents + quarantine_total_cents
    discrepancy = bronze_total_cents - expected_cents
    if discrepancy == 0:
        return True, f"Monetary balance verified: Bronze {bronze_total_cents:,} cents matches Silver + Quarantine."
    return False, f"Monetary imbalance detected: Bronze {bronze_total_cents:,} vs Expected {expected_cents:,} cents."


def verify_manifest_checksum(
    records: List[Dict[str, Any]],
    expected_sha256: str,
) -> Tuple[bool, str, str]:
    serialized = json.dumps(records, sort_keys=True, default=str)
    computed_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    match = computed_hash.lower() == expected_sha256.lower()
    msg = "SHA-256 match confirmed" if match else f"Checksum mismatch: computed {computed_hash} != expected {expected_sha256}"
    return match, computed_hash, msg


def compute_binary_metrics(
    y_true: List[int],
    y_pred: List[int],
) -> Dict[str, float]:
    tp = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 1 and yp == 1)
    fp = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 0 and yp == 1)
    fn = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 1 and yp == 0)
    tn = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 0 and yp == 0)

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2.0 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    accuracy = (tp + tn) / len(y_true) if len(y_true) > 0 else 0.0

    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy,
    }
