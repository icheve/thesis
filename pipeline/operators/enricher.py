"""
EnricherOperator — stateless обогащение платёжного события.

Выполняет:
  1. Конвертацию суммы в RUB по справочному курсу (кеш, TTL 1ч)
  2. Нормализацию статуса (source raw status → internal enum)
  3. Обогащение данными мерчанта (name, category) из справочника
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Optional

import requests

from pipeline.config import (
    ClickHouseConfig,
    STATUS_MAP,
)
from pipeline.models.payment_event import PaymentEvent
from pipeline.models.payment_processed import PaymentProcessed

logger = logging.getLogger(__name__)

_CACHE_TTL_SEC = 3600  # 1 час


@dataclass
class _CacheEntry:
    value: object
    expires_at: float


class _SimpleCache:
    """Простой in-process кеш с TTL."""

    def __init__(self, ttl_sec: int = _CACHE_TTL_SEC):
        self._store: dict = {}
        self._ttl = ttl_sec

    def get(self, key: str):
        entry = self._store.get(key)
        if entry and time.monotonic() < entry.expires_at:
            return entry.value
        return None

    def set(self, key: str, value) -> None:
        self._store[key] = _CacheEntry(
            value=value,
            expires_at=time.monotonic() + self._ttl,
        )


class EnricherOperator:
    """
    Stateless MapFunction: PaymentEvent → PaymentProcessed.

    Может использоваться как обычный callable (для тестов) или
    как Flink MapFunction (наследование добавляется в main.py через адаптер).
    """

    def __init__(self):
        self._currency_cache = _SimpleCache(ttl_sec=_CACHE_TTL_SEC)
        self._merchant_cache = _SimpleCache(ttl_sec=_CACHE_TTL_SEC)
        self._processed_ts_fn = lambda: int(time.time() * 1000)

    # ------------------------------------------------------------------
    # Основной метод обработки
    # ------------------------------------------------------------------

    def enrich(self, event: PaymentEvent) -> PaymentProcessed:
        processed_ts = self._processed_ts_fn()

        # 1. Нормализация статуса
        status_normalized = self._normalize_status(event.source_system, event.status)

        # 2. Конвертация суммы
        exchange_rate = self._get_exchange_rate(event.currency)
        amount_original = Decimal(event.amount)
        if event.currency == "RUB":
            amount_rub = amount_original
        else:
            amount_rub = (amount_original * exchange_rate).quantize(
                Decimal("0.0001"), rounding=ROUND_HALF_UP
            )

        # 3. Обогащение мерчантом
        merchant_name, merchant_category = self._get_merchant_info(event.merchant_id)

        return PaymentProcessed(
            event_id=event.event_id,
            payment_id=event.payment_id,
            source_system=event.source_system,
            event_type=event.event_type,
            status_normalized=status_normalized,
            event_ts=event.event_ts,
            ingestion_ts=event.ingestion_ts,
            processed_ts=processed_ts,
            amount_original=str(amount_original),
            currency_original=event.currency,
            amount_rub=str(amount_rub),
            exchange_rate=str(exchange_rate),
            payer_id=event.payer_id,
            payee_id=event.payee_id,
            merchant_id=event.merchant_id,
            merchant_name=merchant_name,
            merchant_category=merchant_category,
            card_token=event.card_token,
            version=0,          # версия выставляется в ScdMergerOperator
            is_duplicate=False,
        )

    # ------------------------------------------------------------------
    # Нормализация статуса
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_status(source_system: str, raw_status: str) -> str:
        source_map = STATUS_MAP.get(source_system, {})
        normalized = source_map.get(raw_status)
        if normalized is None:
            # Неизвестный статус — сохраняем как есть (не блокируем пайплайн)
            logger.warning(
                "Unknown status for source",
                extra={"source_system": source_system, "raw_status": raw_status},
            )
            return raw_status.upper()
        return normalized

    # ------------------------------------------------------------------
    # Справочник курсов валют
    # ------------------------------------------------------------------

    def _get_exchange_rate(self, currency: str) -> Decimal:
        if currency == "RUB":
            return Decimal("1.0")

        cached = self._currency_cache.get(currency)
        if cached is not None:
            return Decimal(cached)

        rate = self._fetch_exchange_rate_from_clickhouse(currency)
        self._currency_cache.set(currency, str(rate))
        return rate

    def _fetch_exchange_rate_from_clickhouse(self, currency: str) -> Decimal:
        """
        Запрашивает актуальный курс из таблицы exchange_rates в ClickHouse.
        Возвращает курс currency/RUB (сколько рублей за 1 единицу валюты).
        """
        query = (
            f"SELECT rate_to_rub FROM {ClickHouseConfig.DATABASE}.exchange_rates "
            f"WHERE currency = '{currency}' "
            f"ORDER BY valid_from DESC LIMIT 1 FORMAT TabSeparated"
        )
        try:
            resp = requests.get(
                ClickHouseConfig.base_url(),
                params={"query": query, "user": ClickHouseConfig.USER,
                        "password": ClickHouseConfig.PASSWORD},
                timeout=2.0,
            )
            resp.raise_for_status()
            rate_str = resp.text.strip()
            if rate_str:
                return Decimal(rate_str)
        except Exception as exc:
            logger.error("Failed to fetch exchange rate", extra={"currency": currency, "error": str(exc)})

        # Fallback: захардкоженные приблизительные курсы для отказоустойчивости
        _FALLBACK_RATES = {"USD": "90.0", "EUR": "97.0", "CNY": "12.5", "GBP": "113.0"}
        return Decimal(_FALLBACK_RATES.get(currency, "1.0"))

    # ------------------------------------------------------------------
    # Справочник мерчантов
    # ------------------------------------------------------------------

    def _get_merchant_info(self, merchant_id: Optional[str]) -> tuple[Optional[str], Optional[str]]:
        if not merchant_id:
            return None, None

        cached = self._merchant_cache.get(merchant_id)
        if cached is not None:
            return cached

        info = self._fetch_merchant_from_clickhouse(merchant_id)
        self._merchant_cache.set(merchant_id, info)
        return info

    def _fetch_merchant_from_clickhouse(self, merchant_id: str) -> tuple[Optional[str], Optional[str]]:
        """Запрашивает name и category мерчанта из справочника."""
        # Защита от SQL-инъекций: merchant_id должен быть буквенно-цифровым
        safe_id = "".join(c for c in merchant_id if c.isalnum() or c in "-_")
        query = (
            f"SELECT merchant_name, merchant_category "
            f"FROM {ClickHouseConfig.DATABASE}.merchant_dict "
            f"WHERE merchant_id = '{safe_id}' LIMIT 1 FORMAT TabSeparated"
        )
        try:
            resp = requests.get(
                ClickHouseConfig.base_url(),
                params={"query": query, "user": ClickHouseConfig.USER,
                        "password": ClickHouseConfig.PASSWORD},
                timeout=2.0,
            )
            resp.raise_for_status()
            line = resp.text.strip()
            if line:
                parts = line.split("\t")
                return parts[0] or None, parts[1] if len(parts) > 1 else None
        except Exception as exc:
            logger.warning("Failed to fetch merchant info", extra={"merchant_id": merchant_id, "error": str(exc)})

        return None, None
