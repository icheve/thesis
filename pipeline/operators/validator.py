"""
ValidatorOperator — stateless валидация входящих платёжных событий.

Проверяет обязательные поля, форматы и допустимые значения.
Корректные события передаются дальше; невалидные — в DLQ через filter в main.py.
"""

from __future__ import annotations

import base64
import logging
import time
from decimal import Decimal, InvalidOperation
from typing import Optional

from pyflink.datastream.functions import MapFunction, RuntimeContext

from pipeline.config import ALLOWED_CURRENCIES, ALLOWED_SOURCE_SYSTEMS
from pipeline.models.payment_event import DlqMessage, PaymentEvent

logger = logging.getLogger(__name__)


class ValidationResult:
    """Обёртка результата валидации: валидное событие или DLQ-сообщение."""
    __slots__ = ("is_valid", "event", "dlq_msg")

    def __init__(self, is_valid: bool, event=None, dlq_msg=None):
        self.is_valid = is_valid
        self.event = event
        self.dlq_msg = dlq_msg

# Допустимый диапазон event_ts: не в будущем (+ 60 с допуск на clock skew)
# и не старше 30 суток
_MAX_FUTURE_SKEW_MS = 60_000
_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000


class ValidatorOperator(MapFunction):
    """
    Stateless map: PaymentEvent → ValidationResult.

    Возвращает ValidationResult(is_valid=True, event=...) или
    ValidationResult(is_valid=False, dlq_msg=...) для сплиттинга в main.py.
    """

    def open(self, runtime_context: RuntimeContext) -> None:
        # Метрика счётчика ошибок (через Flink MetricGroup)
        metrics = runtime_context.get_metrics_group()
        self._validation_errors = metrics.counter("validation_errors_total")
        # Счётчик всех обработанных событий — нужен для расчёта dq_error_rate в Prometheus
        self._events_total = metrics.counter("dq_events_total")

    def map(self, event: PaymentEvent) -> ValidationResult:
        """PaymentEvent → ValidationResult."""
        raw_bytes = event.to_json().encode()
        self._events_total.inc()
        error = self._validate(event)
        if error:
            code, msg = error
            self._validation_errors.inc()
            logger.warning(
                "Validation failed",
                extra={"error_code": code, "error_message": msg},
            )
            dlq_msg = DlqMessage(
                original_topic="payments.raw",
                original_partition=0,
                original_offset=0,
                error_code=code,
                error_message=msg,
                failed_at_ts=int(time.time() * 1000),
                original_payload_b64=base64.b64encode(raw_bytes).decode(),
            )
            return ValidationResult(is_valid=False, dlq_msg=dlq_msg)
        return ValidationResult(is_valid=True, event=event)

    # ------------------------------------------------------------------

    def _validate(self, e: PaymentEvent) -> tuple[str, str] | None:
        """Возвращает (error_code, message) или None если всё ок."""

        # E002: обязательные поля
        for field_name in ("event_id", "payment_id", "source_system",
                           "event_type", "amount", "currency"):
            val = getattr(e, field_name, None)
            if not val:
                return "E002", f"Required field '{field_name}' is null or empty"

        # E002: payment_id не пустая строка
        if not e.payment_id.strip():
            return "E002", "payment_id is blank"

        # E004: source_system из разрешённых
        if e.source_system not in ALLOWED_SOURCE_SYSTEMS:
            return "E004", f"Unknown source_system: {e.source_system!r}"

        # E003: currency из разрешённых ISO 4217
        if e.currency not in ALLOWED_CURRENCIES:
            return "E003", f"Unknown currency: {e.currency!r}"

        # E001: amount парсится как положительное Decimal
        try:
            amount = Decimal(e.amount)
            if amount <= 0:
                return "E001", f"amount must be > 0, got {e.amount!r}"
        except InvalidOperation:
            return "E001", f"amount is not a valid decimal: {e.amount!r}"

        # E001: event_ts — разумный диапазон
        now_ms = int(time.time() * 1000)
        if e.event_ts > now_ms + _MAX_FUTURE_SKEW_MS:
            return "E001", f"event_ts is too far in future: {e.event_ts}"
        if e.event_ts < now_ms - _MAX_AGE_MS:
            return "E001", f"event_ts is older than 30 days: {e.event_ts}"

        return None

    # (DLQ creation is now inline in map())
