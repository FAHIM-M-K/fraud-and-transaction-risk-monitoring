"""Shared utilities, configuration, watermarking, validators, and audit assertions."""

from payment_risk.common.config import LakehouseConfig, resolve_lakehouse_config
from payment_risk.common.watermark import get_watermark, update_watermark
from payment_risk.common.validators import (
    validate_transaction_dict,
    REJECT_ERR_NULL_TX_ID,
    REJECT_ERR_NEGATIVE_AMOUNT,
    REJECT_ERR_INVALID_CURRENCY,
    REJECT_ERR_MALFORMED_TIMESTAMP,
    REJECT_ERR_INVALID_GEO,
    REJECT_ERR_DUPLICATE_TX,
)
from payment_risk.common.features import (
    haversine_distance_km,
    calculate_implied_speed_kmh,
    calculate_amount_zscore,
)
from payment_risk.common.risk_scorer import (
    calculate_composite_risk_score,
    ACTION_APPROVE,
    ACTION_CHALLENGE,
    ACTION_BLOCK,
    TIER_LOW,
    TIER_ELEVATED,
    TIER_CRITICAL,
)
from payment_risk.common.audit import (
    verify_row_algebra,
    verify_monetary_reconciliation,
    verify_manifest_checksum,
)

__all__ = [
    "LakehouseConfig",
    "resolve_lakehouse_config",
    "get_watermark",
    "update_watermark",
    "validate_transaction_dict",
    "haversine_distance_km",
    "calculate_implied_speed_kmh",
    "calculate_amount_zscore",
    "calculate_composite_risk_score",
    "verify_row_algebra",
    "verify_monetary_reconciliation",
    "verify_manifest_checksum",
    "ACTION_APPROVE",
    "ACTION_CHALLENGE",
    "ACTION_BLOCK",
    "TIER_LOW",
    "TIER_ELEVATED",
    "TIER_CRITICAL",
    "REJECT_ERR_NULL_TX_ID",
    "REJECT_ERR_NEGATIVE_AMOUNT",
    "REJECT_ERR_INVALID_CURRENCY",
    "REJECT_ERR_MALFORMED_TIMESTAMP",
    "REJECT_ERR_INVALID_GEO",
    "REJECT_ERR_DUPLICATE_TX",
]
