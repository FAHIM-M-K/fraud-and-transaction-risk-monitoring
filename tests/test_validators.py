"""Unit tests for transaction data quality validation rules and reject taxonomy."""

import pytest
from payment_risk.common.validators import (
    validate_transaction_dict,
    REJECT_ERR_NULL_TX_ID,
    REJECT_ERR_NEGATIVE_AMOUNT,
    REJECT_ERR_INVALID_CURRENCY,
    REJECT_ERR_MALFORMED_TIMESTAMP,
    REJECT_ERR_INVALID_GEO,
    REJECT_ERR_DUPLICATE_TX,
)


@pytest.fixture
def base_valid_tx():
    return {
        "tx_id": "tx_test_valid_001",
        "customer_id": "cust_100",
        "amount_cents": 4500,
        "currency": "USD",
        "timestamp": "2026-10-15T14:30:00Z",
        "latitude": 37.7749,
        "longitude": -122.4194,
    }


def test_valid_transaction(base_valid_tx):
    rejects = validate_transaction_dict(base_valid_tx)
    assert rejects == []


def test_null_tx_id_rejection(base_valid_tx):
    base_valid_tx["tx_id"] = None
    rejects = validate_transaction_dict(base_valid_tx)
    assert REJECT_ERR_NULL_TX_ID in rejects

    base_valid_tx["tx_id"] = "   "
    rejects_blank = validate_transaction_dict(base_valid_tx)
    assert REJECT_ERR_NULL_TX_ID in rejects_blank


def test_negative_amount_rejection(base_valid_tx):
    base_valid_tx["amount_cents"] = -500
    rejects = validate_transaction_dict(base_valid_tx)
    assert REJECT_ERR_NEGATIVE_AMOUNT in rejects


def test_invalid_currency_rejection(base_valid_tx):
    base_valid_tx["currency"] = "XYZ"
    rejects = validate_transaction_dict(base_valid_tx)
    assert REJECT_ERR_INVALID_CURRENCY in rejects


def test_malformed_timestamp_rejection(base_valid_tx):
    base_valid_tx["timestamp"] = "not-a-timestamp"
    rejects = validate_transaction_dict(base_valid_tx)
    assert REJECT_ERR_MALFORMED_TIMESTAMP in rejects


def test_invalid_geo_coordinates_rejection(base_valid_tx):
    base_valid_tx["latitude"] = 105.0  # Invalid latitude (> 90)
    rejects = validate_transaction_dict(base_valid_tx)
    assert REJECT_ERR_INVALID_GEO in rejects


def test_duplicate_tx_rejection(base_valid_tx):
    seen = {"tx_test_valid_001"}
    rejects = validate_transaction_dict(base_valid_tx, seen_tx_ids=seen)
    assert REJECT_ERR_DUPLICATE_TX in rejects
