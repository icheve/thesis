"""
ScdMergerOperator — stateful ядро конвейера.

Реализует SCD Type 2 merge на базе Flink KeyedProcessFunction:
  - дедупликация по event_id (keyed state, TTL 24ч)
  - контентная дедупликация по хешу значимых полей
  - создание новой версии и закрытие предыдущей (effective_to)

Примечание: маршрутизация late arrivals в side output отключена —
Beam PyFlink runtime не поддерживает ctx.timer_service().
Все события обрабатываются в основном потоке независимо от watermark.
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Optional

from pyflink.datastream.state import (
    MapStateDescriptor,
    StateTtlConfig,
    ValueStateDescriptor,
)
from pyflink.common.typeinfo import Types
from pyflink.common import Time
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext

from pipeline.config import FlinkConfig
from pipeline.models.payment_processed import PaymentHistoryRow, PaymentProcessed

logger = logging.getLogger(__name__)

# Поля, изменение которых означает новую версию платежа.
# Включает payment_id, merchant_id и currency_original для корректной
# контентной дедупликации между источниками (см. ADR-004).
_SIGNIFICANT_FIELDS = (
    "payment_id",
    "status_normalized",
    "event_type",
    "amount_rub",
    "merchant_id",
    "currency_original",
)


def _compute_field_hash(event: PaymentProcessed) -> str:
    """SHA-256 хеш значимых полей для контентной дедупликации."""
    raw = "|".join(str(getattr(event, f, "")) for f in _SIGNIFICANT_FIELDS)
    return hashlib.sha256(raw.encode()).hexdigest()


class ScdMergerOperator(KeyedProcessFunction):
    """
    Keyed по (payment_id, source_system).

    State:
      current_state     — ValueState[dict]  — последняя версия платежа
      processed_events  — MapState[str, int] — event_id → processed_ts (TTL 24ч)
    """

    def open(self, runtime_context: RuntimeContext) -> None:
        # TTL только для processed_events: устаревшие event_id можно забыть через 24ч.
        # current_version НЕ имеет TTL — платёж может не меняться больше суток,
        # и state должен сохраняться бесконечно, иначе следующий update
        # создаст version=1 снова и сломает историю.
        dedup_ttl_config = (
            StateTtlConfig
            .new_builder(Time.hours(FlinkConfig.STATE_TTL_HOURS))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .build()
        )

        # Состояние текущей (последней) версии платежа — без TTL
        current_desc = ValueStateDescriptor("current_version", Types.MAP(Types.STRING(), Types.STRING()))
        self._current_state = runtime_context.get_state(current_desc)

        # Множество обработанных event_id (дедупликация) — TTL 24ч
        events_desc = MapStateDescriptor(
            "processed_events", Types.STRING(), Types.LONG()
        )
        events_desc.enable_time_to_live(dedup_ttl_config)
        self._processed_events = runtime_context.get_map_state(events_desc)

        # Метрики
        metrics = runtime_context.get_metrics_group()
        self._duplicates_counter = metrics.counter("duplicate_events_total")
        self._new_versions_counter = metrics.counter("new_versions_total")
        self._last_e2e_latency_s = 0.0
        metrics.gauge("pipeline_e2e_latency_seconds", lambda: self._last_e2e_latency_s)

    def process_element(self, event: PaymentProcessed, ctx: KeyedProcessFunction.Context):
        """
        Основная логика SCD Type 2.

        Использует yield для вывода элементов (PyFlink Beam runtime).
        """
        now_ms = int(time.time() * 1000)

        # Late arrival check disabled — ctx.timer_service() not supported in Beam PyFlink runtime
        # All events are processed regardless of watermark

        # --- 2. Дедупликация по event_id ----------------------------------
        if self._processed_events.contains(event.event_id):
            self._duplicates_counter.inc()
            logger.debug("Duplicate event skipped", extra={"event_id": event.event_id})
            return

        # --- 3. Контентная дедупликация -----------------------------------
        field_hash = _compute_field_hash(event)
        current_raw = self._current_state.value()

        if current_raw and current_raw.get("field_hash") == field_hash:
            # Значимые поля не изменились — обновляем processed_events и выходим
            self._processed_events.put(event.event_id, now_ms)
            self._duplicates_counter.inc()
            logger.debug("Content duplicate skipped", extra={"event_id": event.event_id})
            return

        # --- 4. Создание новой версии -------------------------------------
        if current_raw:
            prev_version = int(current_raw["version"])
            new_version = prev_version + 1

            # Закрываем предыдущую версию: обновляем effective_to
            closed_row = _dict_to_history_row(current_raw)
            closed_row.effective_to = event.event_ts
            yield closed_row
        else:
            new_version = 1

        # Создаём новую строку истории
        new_row = _wrap_as_history_row(
            event=event,
            version=new_version,
            effective_from=event.event_ts,
            effective_to=None,     # текущая версия открыта
            field_hash=field_hash,
        )
        event.version = new_version

        # --- 5. Обновление state ------------------------------------------
        self._current_state.update(_history_row_to_dict(new_row))
        self._processed_events.put(event.event_id, now_ms)

        # --- 6. Эмит новой версии -----------------------------------------
        self._new_versions_counter.inc()
        # E2E latency: время от event_ts источника до момента обработки в Flink
        e2e_latency_s = (now_ms - event.event_ts) / 1000.0
        if e2e_latency_s >= 0:
            self._last_e2e_latency_s = e2e_latency_s
        yield new_row

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext):
        pass


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _wrap_as_history_row(
    event: PaymentProcessed,
    version: int,
    effective_from: int,
    effective_to: Optional[int],
    field_hash: str,
) -> PaymentHistoryRow:
    return PaymentHistoryRow(
        payment_id=event.payment_id,
        source_system=event.source_system,
        version=version,
        event_id=event.event_id,
        event_type=event.event_type,
        status_normalized=event.status_normalized,
        event_ts=event.event_ts,
        effective_from=effective_from,
        effective_to=effective_to,
        processed_ts=event.processed_ts,
        amount_original=event.amount_original,
        currency_original=event.currency_original,
        amount_rub=event.amount_rub,
        exchange_rate=event.exchange_rate,
        payer_id=event.payer_id,
        payee_id=event.payee_id,
        merchant_id=event.merchant_id,
        merchant_name=event.merchant_name,
        merchant_category=event.merchant_category,
        card_token=event.card_token,
        field_hash=field_hash,
    )


def _history_row_to_dict(row: PaymentHistoryRow) -> dict:
    return {
        "payment_id": row.payment_id,
        "source_system": row.source_system,
        "version": str(row.version),
        "event_id": row.event_id,
        "event_type": row.event_type,
        "status_normalized": row.status_normalized,
        "event_ts": str(row.event_ts),
        "effective_from": str(row.effective_from),
        "effective_to": str(row.effective_to) if row.effective_to else "",
        "processed_ts": str(row.processed_ts),
        "amount_original": row.amount_original,
        "currency_original": row.currency_original,
        "amount_rub": row.amount_rub,
        "exchange_rate": row.exchange_rate,
        "payer_id": row.payer_id or "",
        "payee_id": row.payee_id or "",
        "merchant_id": row.merchant_id or "",
        "merchant_name": row.merchant_name or "",
        "merchant_category": row.merchant_category or "",
        "card_token": row.card_token or "",
        "field_hash": row.field_hash,
    }


def _dict_to_history_row(d: dict) -> PaymentHistoryRow:
    return PaymentHistoryRow(
        payment_id=d["payment_id"],
        source_system=d["source_system"],
        version=int(d["version"]),
        event_id=d["event_id"],
        event_type=d["event_type"],
        status_normalized=d["status_normalized"],
        event_ts=int(d["event_ts"]),
        effective_from=int(d["effective_from"]),
        effective_to=int(d["effective_to"]) if d.get("effective_to") else None,
        processed_ts=int(d["processed_ts"]),
        amount_original=d["amount_original"],
        currency_original=d["currency_original"],
        amount_rub=d["amount_rub"],
        exchange_rate=d["exchange_rate"],
        payer_id=d.get("payer_id") or None,
        payee_id=d.get("payee_id") or None,
        merchant_id=d.get("merchant_id") or None,
        merchant_name=d.get("merchant_name") or None,
        merchant_category=d.get("merchant_category") or None,
        card_token=d.get("card_token") or None,
        field_hash=d["field_hash"],
    )
