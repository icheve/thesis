> **Примечание.** Данный раздел описывает целевую *production*-архитектуру.
> Реализованный прототип отличается: JSON-сериализация (без Avro/Schema Registry), ZooKeeper-режим Kafka (не KRaft), hashmap state backend.
> Описание реализованного прототипа — в разделе 9.

# 3. Проектирование архитектуры

---

## 3.1. Схема потоков данных

### Компонентная диаграмма (C4 Container)

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                          NRT Payment Pipeline                                ║
║                                                                              ║
║  ┌──────────────┐    ┌──────────────────────────────────────────────────┐   ║
║  │   Debezium   │    │                  Apache Kafka                     │   ║
║  │  CDC Conn.   │───►│  topic: payments.raw       (partitions: 12)      │   ║
║  └──────────────┘    │  topic: payments.processed (partitions: 12)      │   ║
║         ▲            │  topic: payments.dlq        (partitions: 6)       │   ║
║         │            │  Schema Registry (provisioned; Avro — prod)       │   ║
║  ┌──────┴───────┐    └──────────────────┬───────────┬───────────────────┘   ║
║  │  PostgreSQL  │                       │           │                         ║
║  │  (source DB) │         ┌─────────────▼──┐   ┌───▼────────────────────┐  ║
║  └──────────────┘         │  Apache Flink  │   │   DLQ Consumer         │  ║
║                            │  Stream Job    │   │   (мониторинг/replay)  │  ║
║  ┌──────────────┐         │                │   └────────────────────────┘  ║
║  │    Kafka     │         │ - валидация     │                                ║
║  │  Producer    │─────────► - нормализация  │                                ║
║  │  (synthetic) │         │ - enrichment    │                                ║
║  └──────────────┘         │ - SCD merge     │                                ║
║                            └───────┬─────────┘                               ║
║                                    │                                          ║
║                                    ▼                                          ║
║                    ┌───────────────────────────────┐                         ║
║                    │          ClickHouse            │                         ║
║                    │  ┌─────────────────────────┐  │                         ║
║                    │  │ payment_current          │  │  ◄── текущее состояние  ║
║                    │  │ (ReplacingMergeTree)     │  │                         ║
║                    │  └─────────────────────────┘  │                         ║
║                    │  ┌─────────────────────────┐  │                         ║
║                    │  │ payment_history          │  │  ◄── вся история        ║
║                    │  │ (MergeTree, append-only) │  │                         ║
║                    │  └─────────────────────────┘  │                         ║
║                    └───────────────────────────────┘                         ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
         │                              │
         ▼                              ▼
  [Grafana Dashboard]         [BI / Антифрод / Отчётность]
  [Prometheus метрики]        (читают из ClickHouse напрямую
                               или из payments.processed)
```

---

### Топология Kafka-топиков

| Топик | Назначение | Partitions | Replication | Retention |
|---|---|---|---|---|
| `payments.raw` | Сырые события от источников | 12 | 3 | 7 дней |
| `payments.processed` | Обработанные события для downstream | 12 | 3 | 3 дня |
| `payments.dlq` | Невалидные / необработанные события | 6 | 3 | 30 дней |
| `payments.schema-changes` | CDC schema change events (Debezium) | 1 | 3 | навсегда |

**Ключ партиционирования:** `payment_id` — гарантирует, что все события одного платежа попадают в одну партицию и обрабатываются строго по порядку.

---

### Детальная схема потока обработки одного события

```
[Источник]
    │
    │  INSERT / UPDATE в payments таблице
    ▼
[Debezium CDC]
    │  Debezium envelope: {before, after, op, ts_ms, source}
    │  Ключ: {"payment_id": "..."}
    ▼
[Kafka: payments.raw]
    │  JSON-сообщение (SimpleStringSchema; Avro + Schema Registry — production-расширение, в прототипе не интегрировано)
    ▼
[Flink: PaymentSourceOperator]
    │  Десериализация JSON → PaymentEvent (PaymentEvent.from_json)
    ├─► Валидация схемы (not null, ranges, enum values)
    │       │
    │       ├─ FAIL ──► [Kafka: payments.dlq] + метрика validation_errors_total
    │       │
    │       └─ OK ──►
    ▼
[Flink: PaymentEnrichOperator]
    │  Lookup side-input: currency_rates, merchant_dict, status_map
    │  - нормализация суммы → RUB
    │  - нормализация статуса → internal enum
    │  - обогащение: merchant_name, category
    ▼
[Flink: PaymentHistoryOperator (stateful)]
    │  Keyed state по payment_id
    │  - загружает текущую версию из state
    │  - сравнивает хеш полей с предыдущей версией
    │  - если изменений нет → событие пропускается (дедупликация)
    │  - если есть изменения → новая версия: version++, effective_from = event_ts
    │    закрывает предыдущую версию: effective_to = event_ts - 1ms
    ▼
[Flink: SinkOperator]
    │
    ├──► [Kafka: payments.processed]    (для downstream real-time потребителей)
    │
    └──► [ClickHouse Sink]
              ├── UPSERT → payment_current   (ReplacingMergeTree по version)
              └── INSERT → payment_history   (append-only)
```

---

### Стратегии восстановления при сбоях

| Компонент | Сценарий сбоя | Стратегия восстановления |
|---|---|---|
| Debezium / Kafka Connect | Падение коннектора | Автоматический рестарт, resume с последнего offset в Kafka Connect |
| Kafka broker (1 из 3) | Недоступен один брокер | Replication factor=3, min.insync.replicas=2 — кластер продолжает работу |
| Flink Job | Падение task manager | Checkpoint каждые 30 с → restart с последнего checkpoint, replay из Kafka |
| ClickHouse | Временная недоступность | Flink буферизует write через AsyncSink, retry с backoff; при долгом сбое — replay из `payments.processed` |
| Schema Registry | Недоступен | Flink кеширует схемы локально; временная недоступность не блокирует обработку |

**Checkpoint-стратегия Flink:**
- Тип: incremental checkpoint (RocksDB state backend)
- Интервал: 30 секунд
- Retention: 3 последних checkpoint
- Хранение: локальная файловая система (dev) / S3-совместимое хранилище (prod)

---

## 3.2. Контракты данных

### Входящая схема (payments.raw) — контракт (JSON в dev; Avro — production-расширение)

```json
{
  "namespace": "ru.hse.thesis.payments",
  "type": "record",
  "name": "PaymentEvent",
  "doc": "Входящее платёжное событие от источника (CDC или прямой producer)",
  "fields": [
    {"name": "event_id",        "type": "string",  "doc": "UUID события, уникален для каждого сообщения"},
    {"name": "payment_id",      "type": "string",  "doc": "Бизнес-идентификатор платежа"},
    {"name": "source_system",   "type": "string",  "doc": "Код системы-источника (e.g. ACQUIRING, MOBILE_APP)"},
    {"name": "event_type",      "type": {"type": "enum", "name": "EventType",
                                  "symbols": ["CREATED","AUTHORIZED","CLEARED","REFUNDED","CANCELLED","FAILED"]},
                                  "doc": "Тип изменения статуса"},
    {"name": "event_ts",        "type": {"type": "long", "logicalType": "timestamp-millis"},
                                  "doc": "Время возникновения события на стороне источника (event time)"},
    {"name": "ingestion_ts",    "type": {"type": "long", "logicalType": "timestamp-millis"},
                                  "doc": "Время попадания события в Kafka (processing time)"},
    {"name": "amount",          "type": "string",  "doc": "Сумма в минорных единицах исходной валюты (bigdecimal as string)"},
    {"name": "currency",        "type": "string",  "doc": "ISO 4217 код валюты (e.g. RUB, USD, EUR)"},
    {"name": "status",          "type": "string",  "doc": "Статус в номенклатуре источника"},
    {"name": "payer_id",        "type": ["null", "string"], "default": null, "doc": "Токенизированный ID плательщика"},
    {"name": "payee_id",        "type": ["null", "string"], "default": null, "doc": "Токенизированный ID получателя"},
    {"name": "merchant_id",     "type": ["null", "string"], "default": null},
    {"name": "card_token",      "type": ["null", "string"], "default": null, "doc": "Токен карты (НЕ PAN)"},
    {"name": "metadata",        "type": {"type": "map", "values": "string"}, "default": {},
                                  "doc": "Дополнительные атрибуты, специфичные для источника"}
  ]
}
```

### Исходящая схема (payments.processed) — контракт (JSON в dev; Avro — production-расширение)

```json
{
  "namespace": "ru.hse.thesis.payments",
  "type": "record",
  "name": "PaymentProcessed",
  "doc": "Обработанное и нормализованное платёжное событие",
  "fields": [
    {"name": "event_id",          "type": "string"},
    {"name": "payment_id",        "type": "string"},
    {"name": "source_system",     "type": "string"},
    {"name": "event_type",        "type": "string",  "doc": "Нормализованный тип события"},
    {"name": "status_normalized", "type": "string",  "doc": "Статус согласно внутреннему справочнику"},
    {"name": "event_ts",          "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "ingestion_ts",      "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "processed_ts",      "type": {"type": "long", "logicalType": "timestamp-millis"},
                                    "doc": "Время завершения обработки в Flink"},
    {"name": "amount_original",   "type": "string",  "doc": "Исходная сумма в исходной валюте"},
    {"name": "currency_original", "type": "string"},
    {"name": "amount_rub",        "type": "string",  "doc": "Сумма в RUB по курсу на момент события"},
    {"name": "exchange_rate",     "type": "string",  "doc": "Применённый курс конвертации"},
    {"name": "payer_id",          "type": ["null", "string"], "default": null},
    {"name": "payee_id",          "type": ["null", "string"], "default": null},
    {"name": "merchant_id",       "type": ["null", "string"], "default": null},
    {"name": "merchant_name",     "type": ["null", "string"], "default": null},
    {"name": "merchant_category", "type": ["null", "string"], "default": null},
    {"name": "card_token",        "type": ["null", "string"], "default": null},
    {"name": "version",           "type": "long",    "doc": "Монотонно возрастающий номер версии платежа"},
    {"name": "is_duplicate",      "type": "boolean", "default": false,
                                    "doc": "Признак того, что событие было дублем и пропущено"}
  ]
}
```

### Схема Schema Registry

| Тема | Subject name | Compatibility |
|---|---|---|
| `payments.raw` | `payments.raw-value` | BACKWARD — новые схемы читают старые данные |
| `payments.processed` | `payments.processed-value` | BACKWARD |
| `payments.dlq` | `payments.dlq-value` | NONE — вольный формат для диагностики |

**Правила эволюции схем:**
- Добавление нового `nullable` поля с `default: null` — разрешено (backward compatible)
- Удаление поля — запрещено без bump версии `namespace`
- Изменение типа поля — запрещено
- Переименование поля — через добавление нового + deprecation старого

---

### Схема таблиц ClickHouse

#### `payment_current` — текущее состояние платежей

```sql
CREATE TABLE payment_current
(
    payment_id          String,
    source_system       String,
    event_type          String,
    status_normalized   LowCardinality(String),
    event_ts            DateTime64(3, 'UTC'),
    processed_ts        DateTime64(3, 'UTC'),
    amount_original     Decimal(18, 4),
    currency_original   LowCardinality(String),
    amount_rub          Decimal(18, 4),
    exchange_rate       Decimal(18, 6),
    payer_id            Nullable(String),
    payee_id            Nullable(String),
    merchant_id         Nullable(String),
    merchant_name       Nullable(String),
    merchant_category   LowCardinality(Nullable(String)),
    card_token          Nullable(String),
    version             UInt64,
    _sign               Int8    DEFAULT 1,  -- для CollapsingMergeTree при необходимости
    _updated_at         DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (payment_id, source_system)
SETTINGS index_granularity = 8192;
```

#### `payment_history` — полная история изменений

```sql
CREATE TABLE payment_history
(
    payment_id          String,
    source_system       String,
    version             UInt64,
    event_type          String,
    status_normalized   LowCardinality(String),
    event_ts            DateTime64(3, 'UTC'),
    effective_from      DateTime64(3, 'UTC'),
    effective_to        Nullable(DateTime64(3, 'UTC')),  -- NULL = текущая версия
    processed_ts        DateTime64(3, 'UTC'),
    amount_original     Decimal(18, 4),
    currency_original   LowCardinality(String),
    amount_rub          Decimal(18, 4),
    exchange_rate       Decimal(18, 6),
    payer_id            Nullable(String),
    payee_id            Nullable(String),
    merchant_id         Nullable(String),
    merchant_name       Nullable(String),
    merchant_category   LowCardinality(Nullable(String)),
    card_token          Nullable(String),
    event_id            String,   -- для дедупликации на уровне таблицы
    _ingestion_ts       DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_ts)
ORDER BY (payment_id, source_system, version)
TTL toDateTime(event_ts) + INTERVAL 3 YEAR DELETE
SETTINGS index_granularity = 8192;
```

**Point-in-time запрос (состояние платежа на момент T):**
```sql
SELECT *
FROM payment_history
WHERE payment_id = '...'
  AND effective_from <= '2025-06-01 12:00:00'
  AND (effective_to > '2025-06-01 12:00:00' OR effective_to IS NULL)
ORDER BY version DESC
LIMIT 1;
```

---

## 3.3. Качество, мониторинг и безопасность

### Dead Letter Queue (DLQ)

**Условия попадания события в DLQ:**

| Причина | Код ошибки | Описание |
|---|---|---|
| `SCHEMA_VALIDATION_ERROR` | E001 | Событие не соответствует ожидаемой JSON-схеме (в production — Avro-схеме) |
| `NULL_REQUIRED_FIELD` | E002 | Обязательное поле содержит null |
| `UNKNOWN_CURRENCY` | E003 | ISO-код валюты отсутствует в справочнике |
| `UNKNOWN_SOURCE_SYSTEM` | E004 | Код источника не зарегистрирован |
| `ENRICHMENT_TIMEOUT` | E005 | Lookup-запрос к справочнику превысил timeout |
| `SINK_WRITE_FAILED` | E006 | Запись в ClickHouse не удалась после N retry |

**Структура DLQ-сообщения:**
```json
{
  "original_topic": "payments.raw",
  "original_partition": 3,
  "original_offset": 12345,
  "error_code": "E001",
  "error_message": "Field 'payment_id' is null",
  "failed_at": "2025-06-01T12:00:00.000Z",
  "flink_job_id": "...",
  "original_payload_b64": "<base64-encoded original message>"
}
```

**Процедура replay из DLQ:**
1. Оператор анализирует причину ошибки (error_code)
2. Если причина устранима (E003, E004 — обновлён справочник) → повторная публикация в `payments.raw`
3. Если причина в данных источника (E001, E002) → заведение инцидента на стороне источника
4. Операция replay идемпотентна — повторная обработка не создаёт дублей

---

### Метрики и мониторинг

**Метрики Prometheus (экспортируются из Flink и Kafka):**

| Метрика | Тип | Описание | Алерт |
|---|---|---|---|
| `kafka_consumer_lag` | Gauge | Лаг консьюмера Flink по партициям | > 10 000 записей в течение 5 мин |
| `flink_e2e_latency_seconds` | Histogram | Время от event_ts до processed_ts | p99 > 30 с |
| `flink_events_processed_total` | Counter | Всего обработанных событий | — |
| `flink_validation_errors_total` | Counter | Число событий, ушедших в DLQ | rate > 1% от throughput |
| `flink_dlq_size` | Gauge | Число непрочитанных сообщений в DLQ | > 100 в течение 15 мин |
| `clickhouse_insert_errors_total` | Counter | Ошибки записи в ClickHouse | rate > 0 |
| `clickhouse_insert_latency_ms` | Histogram | Время batch-вставки в ClickHouse | p99 > 5 000 мс |

**Grafana dashboard — панели:**
1. **Overview:** throughput (events/sec), end-to-end latency (p50/p95/p99), error rate
2. **Kafka:** consumer lag по топикам и партициям, produce rate, DLQ size
3. **Flink:** taskmanager health, checkpoint duration, подтверждённые offset-ы
4. **ClickHouse:** insert rate, query latency, disk usage, partition sizes

---

### Structured Logging

Все компоненты пишут структурированные JSON-логи с обязательными полями:

```json
{
  "timestamp": "2025-06-01T12:00:00.123Z",
  "level": "INFO",
  "service": "flink-payment-processor",
  "job_id": "flink-job-uuid",
  "correlation_id": "event_id из входящего события",
  "payment_id": "...",
  "stage": "enrichment",
  "message": "Payment enriched successfully",
  "duration_ms": 2
}
```

`correlation_id` = `event_id` из входящего сообщения, сквозной через все этапы: источник → Kafka → Flink → ClickHouse.

---

### OpenTelemetry Трейсинг

```
Span: payment.ingest        [Debezium → Kafka]          ~2 ms
  └─ Span: payment.consume  [Kafka → Flink source]      ~5 ms
       └─ Span: payment.validate                        ~1 ms
            └─ Span: payment.enrich                     ~3 ms
                 └─ Span: payment.scd_merge             ~2 ms
                      └─ Span: payment.sink_write       [Flink → ClickHouse] ~10 ms
```

Трейсы экспортируются через OTLP в Jaeger (dev) или любой OTLP-совместимый backend (prod).

---

### Модель управления доступом (RBAC)

#### Kafka ACL

| Роль | Разрешения | Топики |
|---|---|---|
| `pipeline-producer` | WRITE | `payments.raw` |
| `pipeline-processor` | READ, WRITE | `payments.raw`, `payments.processed`, `payments.dlq` |
| `downstream-consumer` | READ | `payments.processed` |
| `dlq-operator` | READ, WRITE | `payments.dlq` |
| `admin` | ALL | все топики |

#### ClickHouse роли

| Роль | Таблицы | Права |
|---|---|---|
| `pipeline_writer` | `payment_current`, `payment_history` | INSERT |
| `analyst_read` | `payment_current`, `payment_history` | SELECT |
| `report_read` | `payment_history` | SELECT (с row policy: только completed статусы) |
| `admin` | все | ALL |

#### Row Policy (ClickHouse) для `report_read`:
```sql
CREATE ROW POLICY report_filter ON payment_history
    FOR SELECT USING status_normalized IN ('CLEARED', 'REFUNDED')
    TO report_read;
```

---

### Защита персональных данных (PII)

| Поле | Тип данных | Метод защиты | Хранится |
|---|---|---|---|
| Номер карты (PAN) | PII | **Не передаётся** в конвейер, только токен | Только `card_token` |
| ID плательщика | PII | Токенизация на стороне источника | `payer_id` = токен |
| ID получателя | PII | Токенизация на стороне источника | `payee_id` = токен |
| Имя владельца карты | PII | **Не передаётся** в конвейер | — |
| IP-адрес | Квази-PII | Не собирается в данном конвейере | — |

**Принцип:** конвейер работает исключительно с токенизированными идентификаторами. Raw PAN и персональные данные в конвейер не попадают и не хранятся.
