from datetime import datetime
from typing import Any, Dict, List, Optional, Set

VALID_CURRENCIES: Set[str] = {"USD", "EUR", "GBP", "AED"}

REJECT_ERR_NULL_TX_ID: str = "ERR_NULL_TX_ID"
REJECT_ERR_NEGATIVE_AMOUNT: str = "ERR_NEGATIVE_AMOUNT"
REJECT_ERR_INVALID_CURRENCY: str = "ERR_INVALID_CURRENCY"
REJECT_ERR_MALFORMED_TIMESTAMP: str = "ERR_MALFORMED_TIMESTAMP"
REJECT_ERR_INVALID_GEO: str = "ERR_INVALID_GEO"
REJECT_ERR_DUPLICATE_TX: str = "ERR_DUPLICATE_TX"


def validate_transaction_dict(
    tx: Dict[str, Any],
    seen_tx_ids: Optional[Set[str]] = None,
    allowed_currencies: Optional[Set[str]] = None,
) -> List[str]:
    currencies = allowed_currencies or VALID_CURRENCIES
    rejects: List[str] = []

    tx_id = tx.get("tx_id")
    if not tx_id or not str(tx_id).strip():
        rejects.append(REJECT_ERR_NULL_TX_ID)
    elif seen_tx_ids is not None:
        if tx_id in seen_tx_ids:
            rejects.append(REJECT_ERR_DUPLICATE_TX)
        else:
            seen_tx_ids.add(tx_id)

    amount = tx.get("amount_cents")
    try:
        if amount is None or int(amount) < 0:
            rejects.append(REJECT_ERR_NEGATIVE_AMOUNT)
    except (ValueError, TypeError):
        rejects.append(REJECT_ERR_NEGATIVE_AMOUNT)

    currency = tx.get("currency")
    if not currency or str(currency).upper() not in currencies:
        rejects.append(REJECT_ERR_INVALID_CURRENCY)

    raw_ts = tx.get("timestamp")
    if not raw_ts:
        rejects.append(REJECT_ERR_MALFORMED_TIMESTAMP)
    else:
        try:
            clean_ts = str(raw_ts).replace("Z", "+00:00")
            datetime.fromisoformat(clean_ts)
        except Exception:
            rejects.append(REJECT_ERR_MALFORMED_TIMESTAMP)

    lat = tx.get("latitude")
    lon = tx.get("longitude")
    try:
        if lat is None or lon is None:
            rejects.append(REJECT_ERR_INVALID_GEO)
        else:
            flat, flon = float(lat), float(lon)
            if not (-90.0 <= flat <= 90.0 and -180.0 <= flon <= 180.0):
                rejects.append(REJECT_ERR_INVALID_GEO)
    except (ValueError, TypeError):
        rejects.append(REJECT_ERR_INVALID_GEO)

    return rejects
