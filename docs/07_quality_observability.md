# 7. Проверка качества и наблюдаемость

---

## 7.1. Качество данных (Data Quality)

### Уровни проверок

Проверки качества реализованы на **трёх уровнях** конвейера:

| Уровень | Где | Тип проверки | Реакция на ошибку |
|---|---|---|---|
| **L1 — Схема** | Flink ValidatorOperator | Структурные (null, тип, диапазон) | DLQ |
| **L2 — Бизнес** | Flink EnricherOperator | Справочная целостность | Предупреждение / fallback |
| **L3 — Хранилище** | Post-load DQ checks (ClickHouse) | Консистентность и полнота | Алерт + отчёт |

---

### L1 — Структурные проверки (в реальном времени, ValidatorOperator)

| Правило | DQ-код | Описание |
|---|---|---|
| NOT_NULL | DQ-001 | `payment_id`, `source_system`, `event_type`, `amount`, `currency` не NULL |
| NOT_BLANK | DQ-002 | `payment_id` — непустая строка после trim |
| VALID_CURRENCY | DQ-003 | `currency` ∈ ISO 4217 разрешённого списка |
| VALID_SOURCE | DQ-004 | `source_system` ∈ зарегистрированных источников |
| POSITIVE_AMOUNT | DQ-005 | `amount` парсится как Decimal и > 0 |
| VALID_TIMESTAMP | DQ-006 | `event_ts` не в будущем (> +60с) и не старше 30 суток |

---

### L2 — Бизнес-проверки (в реальном времени, EnricherOperator)

| Правило | DQ-код | Описание | Реакция |
|---|---|---|---|
| KNOWN_STATUS | DQ-010 | `status` маппируется в STATUS_MAP для `source_system` | Логирование, fallback на raw value |
| EXCHANGE_RATE_EXISTS | DQ-011 | Курс для валюты найден в справочнике | Fallback на захардкоженный курс + алерт |
| MERCHANT_LOOKUP | DQ-012 | `merchant_id` найден в справочнике | NULL в поле, не DLQ |

---

### L3 — Пост-загрузочные проверки (расписание, ClickHouse)

> **Примечание:** в dev-прототипе L3-проверки запускаются вручную (SQL-запросами напрямую). Настройка планировщика (cron / Airflow / ClickHouse Scheduled Views) для запуска раз в 15 минут — production-расширение.

Запускаются как scheduled SQL-проверки раз в 15 минут:

```sql
-- DQ-020: Нет дублей по event_id в payment_history
SELECT
    count() AS total_rows,
    uniqExact(event_id) AS unique_events,
    total_rows - unique_events AS duplicates
FROM payments.payment_history
WHERE _ingestion_ts >= now() - INTERVAL 1 HOUR;
-- Критерий: duplicates = 0

-- DQ-021: Отсутствие разрывов в версиях платежа
-- Версии должны идти подряд: 1, 2, 3, ...
SELECT payment_id, source_system,
    groupArray(version) AS versions,
    max(version) - min(version) + 1 AS expected_count,
    count() AS actual_count
FROM payments.payment_history
WHERE _ingestion_ts >= now() - INTERVAL 1 HOUR
GROUP BY payment_id, source_system
HAVING expected_count != actual_count;
-- Критерий: результат пустой

-- DQ-022: Непрерывность effective_from / effective_to (нет перекрытий)
SELECT
    a.payment_id,
    a.version AS v1, a.effective_from AS ef1, a.effective_to AS et1,
    b.version AS v2, b.effective_from AS ef2
FROM payments.payment_history a
JOIN payments.payment_history b
    ON a.payment_id = b.payment_id
    AND a.source_system = b.source_system
    AND b.version = a.version + 1
WHERE a.effective_to != b.effective_from
  AND a._ingestion_ts >= now() - INTERVAL 1 HOUR;
-- Критерий: результат пустой

-- DQ-023: Ровно одна текущая версия на платёж (effective_to IS NULL)
SELECT payment_id, source_system, count() AS open_versions
FROM payments.payment_history
WHERE effective_to IS NULL
GROUP BY payment_id, source_system
HAVING open_versions > 1;
-- Критерий: результат пустой

-- DQ-024: Сумма в RUB > 0 для всех записей
SELECT count() AS invalid_amount_rows
FROM payments.payment_history
WHERE toDecimal64(amount_rub, 4) <= 0
  AND _ingestion_ts >= now() - INTERVAL 1 HOUR;
-- Критерий: invalid_amount_rows = 0

-- DQ-025: Покрытие: все events из payments.processed дошли до payment_history
-- (сравниваем count за последний час)
-- Запускается отдельно с учётом Kafka consumer lag
```

---

### Метрики качества (экспортируются в Prometheus)

| Метрика | Тип | Описание |
|---|---|---|
| `dq_duplicate_events_total` | Counter | Дубли, пойманные на L1/L2 |
| `dq_dlq_events_total` | Counter | События, ушедшие в DLQ |
| `dq_late_events_total` | Counter | Late arrivals |
| `dq_unknown_status_total` | Counter | Статусы без маппинга |
| `dq_exchange_rate_fallback_total` | Counter | Fallback на захардкоженный курс |
| `dq_post_load_check_failures` | Gauge | Число упавших L3-проверок за последний прогон |
| `dq_error_rate_percent` | Gauge | `dlq_total / total_processed * 100` — **вычисляется в Prometheus из `validation_errors_total / dq_events_total`; `dq_events_total` эмитируется ValidatorOperator** |

---

## 7.2. Метрики и мониторинг (Prometheus)

### Полный список метрик

#### Pipeline Throughput & Latency

| Метрика | Тип | Labels | Алерт |
|---|---|---|---|
| `pipeline_events_consumed_total` | Counter | `topic`, `partition` | — |
| `pipeline_events_processed_total` | Counter | `source_system` | — |
| `pipeline_e2e_latency_seconds` | Histogram | `source_system` | p99 > 30 с — **запланировано (production-расширение; в прототипе не эмитируется)** |
| `pipeline_processing_latency_ms` | Histogram | `stage` (validate/enrich/scd) | p99 > 5 000 мс |
| `pipeline_events_per_second` | Gauge | — | < 10 при baseline (возможный stoppage) |

#### Kafka

| Метрика | Тип | Labels | Алерт |
|---|---|---|---|
| `kafka_consumer_lag_sum` | Gauge | `consumer_group`, `topic` | > 10 000 в течение 5 мин |
| `kafka_dlq_messages_total` | Counter | — | rate > 0 |
| `kafka_dlq_queue_size` | Gauge | — | > 100 в течение 15 мин |

#### ClickHouse Sink

| Метрика | Тип | Labels | Алерт |
|---|---|---|---|
| `clickhouse_insert_total` | Counter | `table` | — |
| `clickhouse_insert_errors_total` | Counter | `table` | > 0 — **запланировано (production-расширение; в прототипе не эмитируется)** |
| `clickhouse_insert_latency_ms` | Histogram | `table` | p99 > 5 000 мс |
| `clickhouse_batch_size` | Histogram | `table` | — |
| `clickhouse_rows_inserted_total` | Counter | `table` | — |

#### State & Checkpointing (Flink JMX → Prometheus)

| Метрика | Тип | Алерт |
|---|---|---|
| `flink_jobmanager_job_lastCheckpointSize` | Gauge | — |
| `flink_jobmanager_job_lastCheckpointDuration` | Gauge | > 25 000 мс (приближается к интервалу) |
| `flink_taskmanager_job_task_numRecordsIn` | Counter | — |
| `flink_taskmanager_Status_JVM_Memory_Heap_Used` | Gauge | > 80% heap → OOM риск |

---

## 7.3. Alerts (Prometheus Alertmanager)

| Алерт | Severity | Условие | Описание |
|---|---|---|---|
| `PipelineHighE2ELatency` | critical | `p99(e2e_latency) > 30s` за 5 мин | SLA нарушен |
| `PipelineConsumerLagHigh` | warning | `kafka_consumer_lag > 10000` за 5 мин | Pipelne не успевает |
| `PipelineDLQGrowing` | warning | `kafka_dlq_queue_size > 100` за 15 мин | Накапливаются невалидные события |
| `PipelineClickHouseErrors` | critical | `clickhouse_insert_errors_total rate > 0` | Ошибки записи в хранилище |
| `PipelineFlinkCheckpointSlow` | warning | `checkpoint_duration > 25s` | Checkpoint занимает почти весь интервал |
| `PipelineNoEvents` | critical | `rate(events_processed_total[5m]) < 1` | Поток данных остановился |
| `PipelineDQCheckFailed` | warning | `dq_post_load_check_failures > 0` | Упала пост-загрузочная DQ-проверка |
| `PipelineMemoryPressure` | warning | `flink_heap_used_pct > 80%` | Риск OOM в TaskManager |

---

## 7.4. Structured Logging

Все компоненты пишут JSON-логи в stdout. Формат:

```json
{
  "timestamp": "2025-06-01T12:00:00.123Z",
  "level": "INFO",
  "service": "flink-payment-pipeline",
  "job_id": "abc-123",
  "task_name": "SCD Merge",
  "correlation_id": "uuid-of-event_id",
  "payment_id": "PAY-001",
  "stage": "scd_merge",
  "action": "new_version_created",
  "version": 3,
  "duration_ms": 2,
  "message": "New payment version created"
}
```

**Обязательные поля во всех логах:**

| Поле | Описание |
|---|---|
| `timestamp` | ISO 8601 с миллисекундами |
| `level` | DEBUG / INFO / WARN / ERROR |
| `service` | Имя компонента |
| `correlation_id` | `event_id` — сквозной идентификатор события |
| `payment_id` | Бизнес-ключ (если применимо) |
| `stage` | Этап конвейера |
| `message` | Человекочитаемое описание |

---

## 7.5. OpenTelemetry Трейсинг

Каждое событие порождает trace из вложенных spans:

```
Trace: payment.pipeline [~20ms total]
  ├─ Span: kafka.consume         [~2ms]   — чтение из Kafka
  ├─ Span: validate              [~1ms]   — ValidatorOperator
  ├─ Span: enrich                [~5ms]   — EnricherOperator
  │    ├─ Span: currency.lookup  [~2ms]   — запрос курса (кеш / ClickHouse)
  │    └─ Span: merchant.lookup  [~2ms]   — запрос мерчанта (кеш / ClickHouse)
  ├─ Span: scd.merge             [~3ms]   — ScdMergerOperator (state read/write)
  └─ Span: sink.write            [~9ms]   — ClickHouse INSERT (батч)
```

Атрибуты spans:
- `payment_id`, `source_system`, `event_type`, `version` (новый)
- `is_duplicate`, `is_late_arrival`
- `kafka.topic`, `kafka.partition`, `kafka.offset`

Экспорт: OTLP → Jaeger (dev) или любой OTLP-compatible backend.

---

## 7.6. Grafana Dashboard — описание панелей

### Row 1: Overview (Summary)

| Панель | Тип | Запрос |
|---|---|---|
| Events/sec (now) | Stat | `rate(pipeline_events_processed_total[1m])` |
| E2E Latency p99 | Stat + threshold (30s) | `histogram_quantile(0.99, pipeline_e2e_latency_seconds_bucket)` |
| Error rate % | Stat + threshold (1%) | `dq_error_rate_percent` |
| DLQ Queue Size | Stat + threshold (100) | `kafka_dlq_queue_size` |

### Row 2: Throughput

| Панель | Тип | Описание |
|---|---|---|
| Events processed over time | Time series | `rate(pipeline_events_processed_total[1m])` by source_system |
| Kafka Consumer Lag | Time series | `kafka_consumer_lag_sum` by topic |
| ClickHouse Insert Rate | Time series | `rate(clickhouse_rows_inserted_total[1m])` by table |

### Row 3: Latency

| Панель | Тип | Описание |
|---|---|---|
| E2E Latency heatmap | Heatmap | `pipeline_e2e_latency_seconds_bucket` |
| Latency by stage | Time series | `pipeline_processing_latency_ms` p50/p95/p99 by stage |
| ClickHouse Insert Latency | Time series | `clickhouse_insert_latency_ms` p99 |

### Row 4: Data Quality

| Панель | Тип | Описание |
|---|---|---|
| DLQ events/min | Time series | `rate(pipeline_events_dlq_total[1m])` |
| Late Arrivals/min | Time series | `rate(dq_late_events_total[1m])` |
| DQ Check Status | Table | Результаты последнего прогона L3-проверок |

### Row 5: Infrastructure

| Панель | Тип | Описание |
|---|---|---|
| Flink TaskManager JVM Heap | Time series | `flink_heap_used_pct` |
| Flink Checkpoint Duration | Time series | `flink_checkpoint_duration_ms` |
| ClickHouse Disk Usage | Gauge | Объём данных в ГБ по таблицам |
