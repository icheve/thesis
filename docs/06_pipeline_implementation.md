# 6. Реализация конвейера

---

## 6.1. Структура проекта

```
pipeline/
├── main.py                      ← точка входа, определение Flink job
├── config.py                    ← конфигурация (Kafka, ClickHouse, Flink)
├── models/
│   ├── __init__.py
│   ├── payment_event.py         ← входная модель (из Kafka payments.raw)
│   └── payment_processed.py     ← выходная модель (в Kafka payments.processed)
├── operators/
│   ├── __init__.py
│   ├── validator.py             ← валидация входящих событий
│   ├── enricher.py              ← обогащение: курсы валют, справочник мерчантов
│   └── scd_merger.py            ← stateful SCD Type 2 merge (keyed state)
├── sinks/
│   ├── __init__.py
│   └── clickhouse_sink.py       ← батчевая запись в ClickHouse
└── requirements.txt
```

---

## 6.2. Технологический стек Flink Job

| Аспект | Решение |
|---|---|
| Язык реализации | Python (PyFlink 1.19) |
| Модель выполнения | DataStream API — нативный потоковый режим |
| State backend | hashmap (dev) / RocksDB (prod) |
| Checkpoint интервал | 30 секунд |
| Watermark стратегия | BoundedOutOfOrdernessWatermark(10 мин) по `event_ts` |
| Параллелизм | Настраивается через `--parallelism` (default: 4) |
| Гарантии | At-least-once + идемпотентный sink = effectively exactly-once |
| Сериализация | JSON (SimpleStringSchema) — реализовано; Avro + Schema Registry — production-расширение (в прототипе не реализовано) |

---

## 6.3. Поток данных в Flink Job

```
KafkaSource(payments.raw)
    │
    ├── [WatermarkStrategy]  ← BoundedOutOfOrderness(10 min) по event_ts
    │
    └── [ValidatorOperator]  ← stateless flatMap
            │
            ├── INVALID ──► KafkaSink(payments.dlq)
            │
            └── VALID
                    │
                    └── [EnricherOperator]  ← stateless map + async lookup
                            │
                            └── keyBy(payment_id + source_system)
                                    │
                                    └── [ScdMergerOperator]  ← stateful KeyedProcessFunction
                                            │
                                            ├── DUPLICATE ──► skip (метрика)
                                            │
                                            └── NEW VERSION
                                                    │
                                                    ├── KafkaSink(payments.processed)
                                                    └── ClickHouseSink(payment_current + payment_history)
```

---

## 6.4. Конфигурация

Все параметры передаются через переменные окружения или файл `.env`.

| Переменная | Описание | Дефолт |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Адреса брокеров Kafka | `localhost:9092` |
| `KAFKA_INPUT_TOPIC` | Входной топик | `payments.raw` |
| `KAFKA_OUTPUT_TOPIC` | Выходной топик | `payments.processed` |
| `KAFKA_DLQ_TOPIC` | DLQ топик | `payments.dlq` |
| `KAFKA_CONSUMER_GROUP` | Consumer group id | `payment-pipeline-flink` |
| `CLICKHOUSE_HOST` | Хост ClickHouse | `localhost` |
| `CLICKHOUSE_PORT` | HTTP порт ClickHouse | `8123` |
| `CLICKHOUSE_DATABASE` | БД | `payments` |
| `CLICKHOUSE_USER` | Пользователь | `pipeline_writer` |
| `CLICKHOUSE_PASSWORD` | Пароль | — |
| `CLICKHOUSE_BATCH_SIZE` | Размер батча для вставки | `500` |
| `CLICKHOUSE_FLUSH_INTERVAL_MS` | Интервал принудительного сброса | `5000` |
| `FLINK_PARALLELISM` | Параллелизм job | `4` |
| `FLINK_CHECKPOINT_INTERVAL_MS` | Интервал checkpoint | `30000` |
| `FLINK_WATERMARK_DELAY_MIN` | Допустимое опоздание событий | `10` |

---

## 6.5. Описание ключевых компонентов

### ValidatorOperator

Stateless `FlatMapFunction`. Проверяет:
1. Обязательные поля: `payment_id`, `source_system`, `event_type`, `event_ts`, `amount`, `currency`
2. `payment_id` — не пустая строка
3. `currency` — ISO-код из разрешённого списка (RUB, USD, EUR, CNY, …)
4. `amount` — парсится как Decimal без ошибок, > 0
5. `source_system` — из зарегистрированных источников
6. `event_ts` — разумный диапазон (не в будущем, не старше 30 дней)

При ошибке: формирует DlqMessage с `error_code` и `original_payload_b64`, отправляет в DLQ.

### EnricherOperator

Stateless `MapFunction` с кешированными справочниками:
- **Конвертация валют:** загружает курсы из ClickHouse-таблицы `exchange_rates` при промахе кеша, TTL 1 час. Broadcast stream и hot reload — production-расширение, в прототипе не реализованы.
- **Справочник мерчантов:** аналогично, `merchant_dict` → `{merchant_id: (name, category)}`
- **Нормализация статуса:** lookup в `STATUS_MAP` по `(source_system, raw_status)`

Результат: `PaymentProcessed` с заполненными `amount_rub`, `exchange_rate`, `merchant_name`, `merchant_category`, `status_normalized`.

### ScdMergerOperator

Stateful `KeyedProcessFunction` — ядро конвейера.

**Keyed state (по `payment_id + source_system`):**
```
current_version_state: ValueState[dict]  ← сериализованное представление PaymentHistoryRow
    ← текущая (последняя) версия платежа

processed_events_state: MapState[event_id, timestamp]
    ← множество обработанных event_id (дедупликация), TTL = 24 ч
```

**Логика:**
1. Дедупликация: `event_id` ∈ `processed_events_state` → skip
2. Хеш-сравнение значимых полей: если хеш не изменился → skip (контентный дубль)
3. Закрытие предыдущей версии: `effective_to = event.event_ts`
4. Создание новой версии: `version++`, `effective_from = event.event_ts`, `effective_to = NULL`
5. Сохранение в state
6. emit в output (ClickHouse sink + Kafka sink)

**Обработка late arrivals:**  
В прототипе маршрутизация late arrivals в side output отключена (`ctx.timer_service()` недоступен в dev runtime); опоздавшие события обрабатываются основным потоком без маркировки. Сепарация в side output — production-расширение.

### ClickHouseSink

`RichSinkFunction` с async-буферизацией:
- `invoke()` только добавляет событие в in-memory буфер (non-blocking горячий путь)
- Фоновый поток (`_flush_loop`) делает poll каждую секунду; при достижении `BATCH_SIZE=500` или `FLUSH_INTERVAL_MS=5000` мс — HTTP INSERT к ClickHouse (JSONEachRow формат)
- Retry: экспоненциальный backoff, max 5 попыток; после исчерпания retries ошибка пробрасывается во Flink — job переходит в FAILED и перезапускается с последнего checkpoint
- `close()` выполняет принудительный финальный flush перед завершением оператора
- Разделение горячего пути (invoke) и I/O (фоновый поток) исключает блокировку Flink-слотов при HTTP-вставках; критично при parallelism > 4 (см. §9.3.8)

---

## 6.6. Развёртывание (Docker Compose)

Минимальная dev-среда поднимается командой:

```bash
docker compose up -d
```

Включает: Kafka (ZooKeeper-режим), ClickHouse, Flink (JobManager + TaskManager), Grafana + Prometheus.

Запуск pipeline job:

```bash
# Запуск job через flink-job-submitter (Docker Compose запускает автоматически)
docker compose up -d

# Ручной запуск в Flink cluster
flink run -py pipeline/main.py -pyfs pipeline/ \
  -D parallelism.default=4 \
  -D execution.checkpointing.interval=30s
```
