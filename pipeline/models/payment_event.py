"""Модели данных: входящее платёжное событие и выходное обработанное событие."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class PaymentEvent:
    """Входящее платёжное событие из Kafka topics: payments.raw."""

    event_id: str
    payment_id: str
    source_system: str
    event_type: str
    event_ts: int                       # Unix millis (event time)
    ingestion_ts: int                   # Unix millis (processing time)
    amount: str                         # Decimal as string
    currency: str                       # ISO 4217
    status: str                         # Raw status from source
    payer_id: Optional[str] = None
    payee_id: Optional[str] = None
    merchant_id: Optional[str] = None
    card_token: Optional[str] = None
    metadata: dict = field(default_factory=dict)

    # --- Serde ----------------------------------------------------------

    @classmethod
    def from_json(cls, data: str | bytes) -> "PaymentEvent":
        d = json.loads(data)
        return cls(
            event_id=d["event_id"],
            payment_id=d["payment_id"],
            source_system=d["source_system"],
            event_type=d["event_type"],
            event_ts=int(d["event_ts"]),
            ingestion_ts=int(d["ingestion_ts"]),
            amount=d["amount"],
            currency=d["currency"],
            status=d["status"],
            payer_id=d.get("payer_id"),
            payee_id=d.get("payee_id"),
            merchant_id=d.get("merchant_id"),
            card_token=d.get("card_token"),
            metadata=d.get("metadata", {}),
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, default=str)


@dataclass
class DlqMessage:
    """Сообщение в Dead Letter Queue."""

    original_topic: str
    original_partition: int
    original_offset: int
    error_code: str
    error_message: str
    failed_at_ts: int                   # Unix millis
    original_payload_b64: str           # base64-encoded raw bytes

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)
