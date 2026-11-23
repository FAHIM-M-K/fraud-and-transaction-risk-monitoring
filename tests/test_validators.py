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


def test_reject_null_tx_id(base_valid_tx):
    base_valid_tx["tx_id"] = "   "
    rejects = validate_transaction_dict(base_valid_tx)
    assert REJECT_ERR_NULL_TX_ID in rejects


def test_reject_negative_amount(base_valid_tx):
    base_valid_tx["amount_cents"] = -500
    rejects = validate_transaction_dict(base_valid_tx)
    assert REJECT_ERR_NEGATIVE_AMOUNT in rejects


def test_reject_invalid_currency(base_valid_tx):
    base_valid_tx["currency"] = "XYZ"
    rejects = validate_transaction_dict(base_valid_tx)
    assert REJECT_ERR_INVALID_CURRENCY in rejects


def test_reject_malformed_timestamp(base_valid_tx):
    base_valid_tx["timestamp"] = "15/10/2026 14:30"
    rejects = validate_transaction_dict(base_valid_tx)
    assert REJECT_ERR_MALFORMED_TIMESTAMP in rejects


def test_reject_invalid_latitude(base_valid_tx):
    base_valid_tx["latitude"] = 95.0
    rejects = validate_transaction_dict(base_valid_tx)
    assert REJECT_ERR_INVALID_GEO in rejects


def test_reject_batch_duplicate_tx_id(base_valid_tx):
    seen = set()
    r1 = validate_transaction_dict(base_valid_tx, seen_tx_ids=seen)
    assert r1 == []
    r2 = validate_transaction_dict(base_valid_tx, seen_tx_ids=seen)
    assert REJECT_ERR_DUPLICATE_TX in r2
