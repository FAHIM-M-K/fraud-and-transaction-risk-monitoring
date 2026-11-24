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
    assert validate_transaction_dict(base_valid_tx) == []


@pytest.mark.parametrize(
    "field,bad_value,expected_code",
    [
        ("tx_id", "   ", REJECT_ERR_NULL_TX_ID),
        ("amount_cents", -500, REJECT_ERR_NEGATIVE_AMOUNT),
        ("currency", "XYZ", REJECT_ERR_INVALID_CURRENCY),
        ("timestamp", "15/10/2026 14:30", REJECT_ERR_MALFORMED_TIMESTAMP),
        ("latitude", 95.0, REJECT_ERR_INVALID_GEO),
    ],
)
def test_quarantine_validation_rules(base_valid_tx, field, bad_value, expected_code):
    base_valid_tx[field] = bad_value
    assert expected_code in validate_transaction_dict(base_valid_tx)


def test_reject_batch_duplicate_tx_id(base_valid_tx):
    seen = set()
    assert validate_transaction_dict(base_valid_tx, seen_tx_ids=seen) == []
    assert REJECT_ERR_DUPLICATE_TX in validate_transaction_dict(base_valid_tx, seen_tx_ids=seen)
