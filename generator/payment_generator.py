"""
Генератор синтетических платёжных событий.
Отправляет события в Kafka-топик payments.raw в формате JSON.
(прототип; Avro + Schema Registry — production-расширение)
"""

import uuid
import time
import random
import hashlib
import argparse
import json
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Optional
from enum import Enum


# ---------------------------------------------------------------------------
# Справочники
# ---------------------------------------------------------------------------

class EventType(str, Enum):
    CREATED = "CREATED"
    AUTHORIZED = "AUTHORIZED"
    CLEARED = "CLEARED"
    REFUNDED = "REFUNDED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


SOURCE_SYSTEMS = {
    "ACQUIRING": 0.60,
    "MOBILE_APP": 0.25,
    "INTERNET_BANKING": 0.10,
    "BATCH_TRANSFER": 0.05,
}

CURRENCIES = {
    "RUB": (0.80, 1.0),
    "USD": (0.12, 90.0),
    "EUR": (0.05, 97.0),
    "CNY": (0.03, 12.5),
}

# Статусы источников → внутренняя номенклатура
STATUS_MAP = {
    "ACQUIRING": {
        "NEW": "PENDING",
        "AUTH_OK": "AUTHORIZED",
        "SETTLED": "COMPLETED",
        "DECLINED": "FAILED",
        "REVERSED": "REFUNDED",
        "VOID": "CANCELLED",
    },
    "MOBILE_APP": {
        "processing": "PENDING",
        "authorized": "AUTHORIZED",
        "done": "COMPLETED",
        "failed": "FAILED",
        "refund": "REFUNDED",
        "cancelled": "CANCELLED",
    },
    "INTERNET_BANKING": {
        "INIT": "PENDING",
        "CONFIRMED": "AUTHORIZED",
        "EXECUTED": "COMPLETED",
        "ERROR": "FAILED",
        "REVERSED": "REFUNDED",
        "CANCELED": "CANCELLED",
    },
    "BATCH_TRANSFER": {
        "QUEUED": "PENDING",
        "PROCESSED": "COMPLETED",
        "REJECTED": "FAILED",
    },
}

# Обратный маппинг: internal status → source-specific raw status (для генерации)
RAW_STATUS_MAP = {
    src: {v: k for k, v in mapping.items()}
    for src, mapping in STATUS_MAP.items()
}

MERCHANTS = [
    ("MCH-001", "Пятёрочка", "GROCERY"),
    ("MCH-002", "Перекрёсток", "GROCERY"),
    ("MCH-003", "Wildberries", "ECOMMERCE"),
    ("MCH-004", "Ozon", "ECOMMERCE"),
    ("MCH-005", "РЖД", "TRANSPORT"),
    ("MCH-006", "Аэрофлот", "TRANSPORT"),
    ("MCH-007", "McDonald's", "FOOD"),
    ("MCH-008", "Burger King", "FOOD"),
    ("MCH-009", "Детский мир", "RETAIL"),
    ("MCH-010", "М.Видео", "ELECTRONICS"),
]

# Суточный профиль нагрузки (100 = baseline)
HOURLY_LOAD_PROFILE = [
    20, 15, 10, 10, 15, 30,   # 00–05
    60, 85, 95, 100, 100, 100, # 06–11
    95, 90, 85, 90, 95, 100,  # 12–17
    100, 90, 75, 60, 45, 30,  # 18–23
]


# ---------------------------------------------------------------------------
# Модель события
# ---------------------------------------------------------------------------

@dataclass
class PaymentEvent:
    event_id: str
    payment_id: str
    source_system: str
    event_type: str
    event_ts: int          # Unix timestamp millis (event time)
    ingestion_ts: int      # Unix timestamp millis (now)
    amount: str
    currency: str
    status: str
    payer_id: Optional[str] = None
    payee_id: Optional[str] = None
    merchant_id: Optional[str] = None
    card_token: Optional[str] = None
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)


# ---------------------------------------------------------------------------
# Генератор
# ---------------------------------------------------------------------------

class PaymentGenerator:
    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
        self._source_systems = list(SOURCE_SYSTEMS.keys())
        self._source_weights = list(SOURCE_SYSTEMS.values())
        self._currency_codes = list(CURRENCIES.keys())
        self._currency_weights = [v[0] for v in CURRENCIES.values()]

    # -- Вспомогательные методы ------------------------------------------

    def _weighted_choice(self, population, weights):
        return random.choices(population, weights=weights, k=1)[0]

    def _make_payer_id(self) -> str:
        raw = f"user_{random.randint(1, 500_000)}"
        return "TOK-" + hashlib.sha256(raw.encode()).hexdigest()[:16].upper()

    def _make_card_token(self) -> str:
        return "CTOKEN-" + uuid.uuid4().hex[:12].upper()

    def _make_amount(self) -> tuple[str, str]:
        """Возвращает (amount_str, currency)."""
        import math
        mu, sigma = 7.5, 1.2
        raw = random.lognormvariate(mu, sigma)
        raw = max(10.0, min(500_000.0, raw))
        amount = round(raw, 2)
        currency = self._weighted_choice(self._currency_codes, self._currency_weights)
        return str(amount), currency

    def _raw_status(self, source: str, internal_status: str) -> str:
        return RAW_STATUS_MAP.get(source, {}).get(internal_status, internal_status)

    # -- Генерация одного платежа (последовательность событий) -----------

    def generate_payment_lifecycle(
        self,
        base_ts: Optional[datetime] = None,
    ) -> list[PaymentEvent]:
        """Генерирует список событий для одного платежа."""
        if base_ts is None:
            base_ts = datetime.now(tz=timezone.utc)

        payment_id = "PAY-" + uuid.uuid4().hex[:12].upper()
        source = self._weighted_choice(self._source_systems, self._source_weights)
        amount, currency = self._make_amount()
        payer_id = self._make_payer_id()
        payee_id = self._make_payer_id()
        card_token = self._make_card_token() if source in ("ACQUIRING", "MOBILE_APP") else None
        merchant = random.choice(MERCHANTS) if source != "BATCH_TRANSFER" else None

        events: list[PaymentEvent] = []
        current_ts = base_ts

        def make_event(event_type: str, internal_status: str, ts: datetime) -> PaymentEvent:
            return PaymentEvent(
                event_id=str(uuid.uuid4()),
                payment_id=payment_id,
                source_system=source,
                event_type=event_type,
                event_ts=int(ts.timestamp() * 1000),
                ingestion_ts=int(datetime.now(tz=timezone.utc).timestamp() * 1000),
                amount=amount,
                currency=currency,
                status=self._raw_status(source, internal_status),
                payer_id=payer_id,
                payee_id=payee_id,
                merchant_id=merchant[0] if merchant else None,
                card_token=card_token,
                metadata={"source_version": "1.0"},
            )

        # CREATED
        events.append(make_event("CREATED", "PENDING", current_ts))

        # AUTHORIZED or FAILED (85% / 15%)
        current_ts += timedelta(seconds=random.uniform(1, 10))
        if random.random() < 0.85:
            events.append(make_event("AUTHORIZED", "AUTHORIZED", current_ts))

            # CLEARED or FAILED (95% / 5%)
            current_ts += timedelta(seconds=random.uniform(60, 600))
            if random.random() < 0.95:
                events.append(make_event("CLEARED", "COMPLETED", current_ts))

                # REFUNDED (3%)
                if random.random() < 0.03:
                    current_ts += timedelta(minutes=random.uniform(5, 30))
                    events.append(make_event("REFUNDED", "REFUNDED", current_ts))
            else:
                events.append(make_event("FAILED", "FAILED", current_ts))
        else:
            events.append(make_event("FAILED", "FAILED", current_ts))

        return events

    # -- Специальные сценарии -------------------------------------------

    def generate_duplicate(self, original: PaymentEvent) -> PaymentEvent:
        """Дубль события: идентичный event_id, новый ingestion_ts."""
        dup = PaymentEvent(**original.to_dict())
        dup.ingestion_ts = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        return dup

    def generate_invalid_event(self, error_type: str = "null_payment_id") -> PaymentEvent:
        """Генерирует невалидное событие для проверки DLQ."""
        base = self.generate_payment_lifecycle()[0]
        if error_type == "null_payment_id":
            base.payment_id = None
        elif error_type == "unknown_currency":
            base.currency = "XXX"
        elif error_type == "invalid_amount":
            base.amount = "not-a-number"
        elif error_type == "unknown_source":
            base.source_system = "UNKNOWN_SYS"
        return base

    def generate_late_arrival(
        self,
        payment_id: str,
        source_system: str,
        delay_minutes: int = 25,
    ) -> PaymentEvent:
        """Генерирует опоздавшее событие (event_ts в прошлом)."""
        late_ts = datetime.now(tz=timezone.utc) - timedelta(minutes=delay_minutes)
        return PaymentEvent(
            event_id=str(uuid.uuid4()),
            payment_id=payment_id,
            source_system=source_system,
            event_type="PRE_AUTH",
            event_ts=int(late_ts.timestamp() * 1000),
            ingestion_ts=int(datetime.now(tz=timezone.utc).timestamp() * 1000),
            amount="500.00",
            currency="RUB",
            status=self._raw_status(source_system, "PENDING"),
            metadata={"late_arrival_test": "true"},
        )


# ---------------------------------------------------------------------------
# Kafka Producer (простая обёртка без внешних зависимостей в preview)
# ---------------------------------------------------------------------------

class KafkaPaymentProducer:
    """
    Обёртка над confluent_kafka.Producer для отправки PaymentEvent в Kafka.
    Использует JSON-сериализацию.
    """

    def __init__(self, bootstrap_servers: str, topic: str):
        try:
            from confluent_kafka import Producer
            self._producer = Producer({
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "lz4",
                "linger.ms": 5,
                "batch.size": 65536,
            })
            self._topic = topic
        except ImportError:
            print("[WARN] confluent_kafka не установлен. Запуск в dry-run режиме.")
            self._producer = None

    def send(self, event: PaymentEvent):
        key = event.payment_id.encode("utf-8")
        value = event.to_json().encode("utf-8")
        if self._producer:
            self._producer.produce(
                self._topic,
                key=key,
                value=value,
                on_delivery=self._delivery_callback,
            )
        else:
            print(f"[DRY-RUN] topic={self._topic} key={event.payment_id} "
                  f"event_type={event.event_type} ts={event.event_ts}")

    def flush(self):
        if self._producer:
            self._producer.flush()

    @staticmethod
    def _delivery_callback(err, msg):
        if err:
            print(f"[ERROR] Delivery failed: {err}")


# ---------------------------------------------------------------------------
# Профили нагрузки
# ---------------------------------------------------------------------------

def run_load_profile(
    generator: PaymentGenerator,
    producer: KafkaPaymentProducer,
    rate_per_sec: int,
    duration_sec: int,
    profile: str = "flat",
):
    """
    Запускает генерацию событий с заданным профилем.

    Профили:
      flat   — постоянная нагрузка rate_per_sec
      daily  — нагрузка по суточному профилю (HOURLY_LOAD_PROFILE)
      spike  — 5 мин baseline → 1 мин 10× → возврат
    """
    print(f"[INFO] Starting load profile={profile} rate={rate_per_sec}/s duration={duration_sec}s")
    start = time.monotonic()
    total_sent = 0
    interval = 1.0 / rate_per_sec

    while time.monotonic() - start < duration_sec:
        elapsed = time.monotonic() - start

        # Определяем текущий rate по профилю
        if profile == "daily":
            hour = datetime.now().hour
            factor = HOURLY_LOAD_PROFILE[hour] / 100.0
            current_rate = max(1, int(rate_per_sec * factor))
            current_interval = 1.0 / current_rate
        elif profile == "spike":
            spike_start, spike_end = 300, 360  # секунды 300–360
            if spike_start <= elapsed < spike_end:
                current_interval = interval / 10  # 10× нагрузка
            else:
                current_interval = interval
        else:
            current_interval = interval

        tick = time.monotonic()
        lifecycle_base = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        events = generator.generate_payment_lifecycle(base_ts=lifecycle_base)
        for event in events:
            producer.send(event)
            total_sent += 1

        sleep_time = current_interval - (time.monotonic() - tick)
        if sleep_time > 0:
            time.sleep(sleep_time)

        if total_sent % 10_000 == 0:
            print(f"[INFO] Sent {total_sent} events, elapsed {elapsed:.1f}s")

    producer.flush()
    print(f"[INFO] Done. Total events sent: {total_sent}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="NRT Payment Pipeline — Data Generator")
    parser.add_argument("--bootstrap-servers",
                        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default="payments.raw")
    parser.add_argument("--rate", type=int, default=200, help="Events per second")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    parser.add_argument("--profile", choices=["flat", "daily", "spike"], default="flat")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    parser.add_argument("--scenario", choices=["normal", "duplicates", "invalid", "late"],
                        default="normal", help="Special test scenario")
    args = parser.parse_args()

    gen = PaymentGenerator(seed=args.seed)
    producer = KafkaPaymentProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )

    if args.scenario == "duplicates":
        print("[INFO] Scenario: duplicates — sending each event twice")
        events = gen.generate_payment_lifecycle()
        for e in events:
            producer.send(e)
            time.sleep(0.1)
            producer.send(gen.generate_duplicate(e))
        producer.flush()

    elif args.scenario == "invalid":
        print("[INFO] Scenario: invalid events → DLQ")
        for error_type in ["null_payment_id", "unknown_currency", "invalid_amount", "unknown_source"]:
            event = gen.generate_invalid_event(error_type)
            producer.send(event)
        producer.flush()

    elif args.scenario == "late":
        print("[INFO] Scenario: late arrival")
        events = gen.generate_payment_lifecycle()
        payment_id = events[0].payment_id
        source = events[0].source_system
        for e in events:
            producer.send(e)
        time.sleep(2)
        late = gen.generate_late_arrival(payment_id, source, delay_minutes=25)
        producer.send(late)
        producer.flush()

    else:
        run_load_profile(gen, producer, args.rate, args.duration, args.profile)


if __name__ == "__main__":
    main()
