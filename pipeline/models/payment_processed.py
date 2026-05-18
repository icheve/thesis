"""Модель обработанного платёжного события и строки истории."""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class PaymentProcessed:
    """
    Обработанное и обогащённое событие.
    Публикуется в Kafka payments.processed и записывается в ClickHouse.
    """

    event_id: str
    payment_id: str
    source_system: str
    event_type: str
    status_normalized: str
    event_ts: int                           # Unix millis (event time)
    ingestion_ts: int                       # Unix millis
    processed_ts: int                       # Unix millis (time of Flink processing)
    amount_original: str                    # Decimal as string
    currency_original: str
    amount_rub: str                         # Decimal as string
    exchange_rate: str                      # Decimal as string
    payer_id: Optional[str]
    payee_id: Optional[str]
    merchant_id: Optional[str]
    merchant_name: Optional[str]
    merchant_category: Optional[str]
    card_token: Optional[str]
    version: int
    is_duplicate: bool = False

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, default=str)

    def to_clickhouse_current_row(self) -> dict:
        """Строка для таблицы payment_current."""
        return {
            "payment_id": self.payment_id,
            "source_system": self.source_system,
            "event_type": self.event_type,
            "status_normalized": self.status_normalized,
            "event_ts": _ms_to_ch_datetime(self.event_ts),
            "processed_ts": _ms_to_ch_datetime(self.processed_ts),
            "amount_original": self.amount_original,
            "currency_original": self.currency_original,
            "amount_rub": self.amount_rub,
            "exchange_rate": self.exchange_rate,
            "payer_id": self.payer_id or "",
            "payee_id": self.payee_id or "",
            "merchant_id": self.merchant_id or "",
            "merchant_name": self.merchant_name or "",
            "merchant_category": self.merchant_category or "",
            "card_token": self.card_token or "",
            "version": self.version,
        }


@dataclass
class PaymentHistoryRow:
    """Строка полной истории изменений платежа для таблицы payment_history."""

    payment_id: str
    source_system: str
    version: int
    event_id: str
    event_type: str
    status_normalized: str
    event_ts: int                           # Unix millis
    effective_from: int                     # Unix millis
    effective_to: Optional[int]             # Unix millis, None = текущая версия
    processed_ts: int                       # Unix millis
    amount_original: str
    currency_original: str
    amount_rub: str
    exchange_rate: str
    payer_id: Optional[str]
    payee_id: Optional[str]
    merchant_id: Optional[str]
    merchant_name: Optional[str]
    merchant_category: Optional[str]
    card_token: Optional[str]
    field_hash: str                         # SHA-1 значимых полей для дедупликации

    def to_clickhouse_history_row(self) -> dict:
        """Строка для таблицы payment_history."""
        return {
            "payment_id": self.payment_id,
            "source_system": self.source_system,
            "version": self.version,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "status_normalized": self.status_normalized,
            "event_ts": _ms_to_ch_datetime(self.event_ts),
            "effective_from": _ms_to_ch_datetime(self.effective_from),
            "effective_to": _ms_to_ch_datetime(self.effective_to) if self.effective_to else None,
            "processed_ts": _ms_to_ch_datetime(self.processed_ts),
            "amount_original": self.amount_original,
            "currency_original": self.currency_original,
            "amount_rub": self.amount_rub,
            "exchange_rate": self.exchange_rate,
            "payer_id": self.payer_id or "",
            "payee_id": self.payee_id or "",
            "merchant_id": self.merchant_id or "",
            "merchant_name": self.merchant_name or "",
            "merchant_category": self.merchant_category or "",
            "card_token": self.card_token or "",
            "is_current": 1 if self.effective_to is None else 0,
            "field_hash": self.field_hash,
        }


def _ms_to_ch_datetime(ms: int) -> str:
    """Конвертирует Unix millis в строку формата ClickHouse DateTime64."""
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # до миллисекунд
