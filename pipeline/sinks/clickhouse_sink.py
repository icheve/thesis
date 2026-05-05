"""
ClickHouseSink — батчевая запись в таблицы payment_current и payment_history.

Накапливает строки в памяти и сбрасывает их:
  - при достижении BATCH_SIZE
  - по истечении FLUSH_INTERVAL_MS (фоновый поток, не зависит от входящих событий)

При ошибке записи — экспоненциальный backoff + retry (MAX_RETRIES), затем DLQ.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Optional

import requests

from pipeline.config import ClickHouseConfig
from pipeline.models.payment_processed import PaymentHistoryRow

logger = logging.getLogger(__name__)


class ClickHouseBatchWriter:
    """
    Низкоуровневый HTTP-клиент для батчевой вставки в ClickHouse (JSONEachRow).
    Используется внутри ClickHouseSink.
    """

    def __init__(self):
        self._base_url = ClickHouseConfig.base_url()
        self._db = ClickHouseConfig.DATABASE
        self._user = ClickHouseConfig.USER
        self._password = ClickHouseConfig.PASSWORD
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/x-ndjson"})

    def insert(self, table: str, rows: list[dict]) -> None:
        """Вставляет список строк в указанную таблицу через HTTP INSERT."""
        if not rows:
            return

        ndjson = "\n".join(json.dumps(row, ensure_ascii=False, default=str) for row in rows)
        query = f"INSERT INTO {self._db}.{table} FORMAT JSONEachRow"

        attempt = 0
        delay_ms = ClickHouseConfig.RETRY_BASE_DELAY_MS
        last_exc: Optional[Exception] = None

        while attempt < ClickHouseConfig.MAX_RETRIES:
            try:
                resp = self._session.post(
                    self._base_url,
                    params={
                        "query": query,
                        "user": self._user,
                        "password": self._password,
                        "async_insert": "0",
                        "wait_for_async_insert": "1",
                    },
                    data=ndjson.encode("utf-8"),
                    timeout=10.0,
                )
                resp.raise_for_status()
                logger.debug("ClickHouse INSERT OK", extra={"table": table, "rows": len(rows)})
                return
            except (requests.RequestException, OSError) as exc:
                last_exc = exc
                attempt += 1
                logger.warning(
                    "ClickHouse INSERT failed, retrying",
                    extra={"table": table, "attempt": attempt, "delay_ms": delay_ms, "error": str(exc)},
                )
                time.sleep(delay_ms / 1000.0)
                delay_ms = min(delay_ms * 2, 30_000)  # cap at 30s

        raise RuntimeError(
            f"ClickHouse INSERT into {table} failed after {ClickHouseConfig.MAX_RETRIES} attempts: {last_exc}"
        )

    def close(self):
        self._session.close()


class ClickHouseSink:
    """
    Буферизованный sink: накапливает PaymentHistoryRow и сбрасывает батчами.

    Используется как RichSinkFunction в Flink (интеграция через main.py).
    Для упрощения также работает как standalone Python-объект (для тестов).
    """

    def __init__(self):
        self._writer: Optional[ClickHouseBatchWriter] = None
        self._history_buffer: list[dict] = []
        self._current_buffer: list[dict] = []
        self._last_flush_ts: float = 0.0
        self._lock = threading.Lock()
        self._stop_event: Optional[threading.Event] = None
        self._flush_thread: Optional[threading.Thread] = None

    def open(self) -> None:
        self._writer = ClickHouseBatchWriter()
        self._last_flush_ts = time.monotonic()
        self._stop_event = threading.Event()
        self._flush_thread = threading.Thread(
            target=self._background_flush_loop,
            daemon=True,
            name="clickhouse-flush",
        )
        self._flush_thread.start()
        logger.info("ClickHouseSink opened")

    def _background_flush_loop(self) -> None:
        """Фоновый поток: сбрасывает буфер каждую секунду."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=1.0)
            with self._lock:
                self._maybe_flush(force=False)

    def invoke(self, row: PaymentHistoryRow) -> None:
        """Принимает строку истории, добавляет в оба буфера."""
        if row.is_duplicate if hasattr(row, "is_duplicate") else False:
            return

        history_dict = row.to_clickhouse_history_row()
        with self._lock:
            self._history_buffer.append(history_dict)

            # В payment_current записываем только последние версии (effective_to IS NULL)
            if row.effective_to is None:
                self._current_buffer.append(_history_to_current_dict(row))

            self._maybe_flush(force=False)

    def _maybe_flush(self, force: bool = False) -> None:
        elapsed_ms = (time.monotonic() - self._last_flush_ts) * 1000
        size_threshold = len(self._history_buffer) >= ClickHouseConfig.BATCH_SIZE
        time_threshold = elapsed_ms >= ClickHouseConfig.FLUSH_INTERVAL_MS

        if force or size_threshold or time_threshold:
            self._flush()

    def _flush(self) -> None:
        if self._history_buffer:
            try:
                self._writer.insert("payment_history", self._history_buffer)
                logger.info("Flushed payment_history", extra={"rows": len(self._history_buffer)})
            except RuntimeError as exc:
                logger.error("Failed to flush payment_history: %s", exc)
                raise
            finally:
                self._history_buffer.clear()

        if self._current_buffer:
            try:
                self._writer.insert("payment_current", self._current_buffer)
                logger.info("Flushed payment_current", extra={"rows": len(self._current_buffer)})
            except RuntimeError as exc:
                logger.error("Failed to flush payment_current: %s", exc)
                raise
            finally:
                self._current_buffer.clear()

        self._last_flush_ts = time.monotonic()

    def close(self) -> None:
        if self._stop_event:
            self._stop_event.set()
        with self._lock:
            self._maybe_flush(force=True)
        if self._writer:
            self._writer.close()
        logger.info("ClickHouseSink closed")


def _history_to_current_dict(row: PaymentHistoryRow) -> dict:
    """Конвертирует строку истории в строку для payment_current."""
    from pipeline.models.payment_processed import _ms_to_ch_datetime
    return {
        "payment_id": row.payment_id,
        "source_system": row.source_system,
        "event_type": row.event_type,
        "status_normalized": row.status_normalized,
        "event_ts": _ms_to_ch_datetime(row.event_ts),
        "processed_ts": _ms_to_ch_datetime(row.processed_ts),
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
        "version": row.version,
    }
