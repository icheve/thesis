# Runbook: NRT Payment Pipeline

Операционный справочник для дежурных инженеров и DevOps-команды.

---

## Быстрый старт

```bash
# Поднять всю инфраструктуру
docker compose up -d

# Проверить статус сервисов
docker compose ps

# Запустить Flink job (если не запускается автоматически)
docker compose exec flink-jobmanager \
    flink run -py /opt/flink/pipeline/main.py \
    --parallelism 4
```

**Ключевые URL:**

| Сервис | URL |
|--------|-----|
| Flink Web UI | http://localhost:8082 |
| Kafka UI | http://localhost:8080 |
| Grafana | http://localhost:3000 (admin / admin_secret) |
| Prometheus | http://localhost:9090 |
| ClickHouse HTTP | http://localhost:8123 |

---

## Мониторинг

### Основные дашборды Grafana

- **Pipeline Overview** — статус job, события/сек, consumer lag
- **Latency** — e2e latency p50/p95/p99, processing latency по стадиям
- **Data Quality** — DLQ rate, error codes, DQ-проверки
- **Infrastructure** — Flink heap, ClickHouse CPU/memory, Kafka disk

### Критические алерты (требуют реакции ≤ 15 мин)

| Алерт | Метрика | Порог |
|-------|---------|-------|
| `PipelineHighE2ELatency` | `pipeline_e2e_latency_seconds` (gauge) | > 30 с |
| Нет роста offsets / consumer lag не меняется | `kafka_consumergroup_lag` / Flink Web UI | lag стоит / статус FAILED |
| `PipelineClickHouseInsertErrors` | `rate(clickhouse_insert_errors_total[2m])` | > 0 |

---

## Диагностика распространённых проблем

### 1. Flink job не запущен / в состоянии FAILED

**Симптомы:** Flink Web UI показывает FAILED, consumer lag не меняется, нет роста offsets в Kafka.

**Диагностика:**
```bash
# Посмотреть логи JobManager
docker compose logs flink-jobmanager --tail=100

# Посмотреть логи TaskManager
docker compose logs flink-taskmanager --tail=100

# Проверить последний checkpoint
curl http://localhost:8082/jobs/<JOB_ID>/checkpoints
```

**Действия:**
```bash
# Перезапустить job из последнего checkpoint
# Актуальный путь: http://localhost:8082/jobs/<JOB_ID>/checkpoints → "external_path"
docker compose exec flink-jobmanager \
    flink run -py /opt/flink/pipeline/main.py \
    --parallelism 4 \
    -s file:///flink-checkpoints/<latest_checkpoint_dir>

# Если checkpoint недоступен — запустить с нуля (возможна переработка)
docker compose exec flink-jobmanager \
    flink run -py /opt/flink/pipeline/main.py --parallelism 4
```

---

### 2. Высокий consumer lag (алерт `PipelineConsumerLagHigh`)

**Симптомы:** lag > 10 000 в группе `payment-pipeline-flink`, latency растёт.

**Диагностика:**
```bash
# Текущий lag по всем партициям
docker compose exec kafka \
    kafka-consumer-groups --bootstrap-server kafka:9092 \
    --group payment-pipeline-flink --describe

# Загрузка TaskManager
curl http://localhost:8082/taskmanagers
```

**Действия:**
1. Проверить загрузку TaskManager CPU/Heap в Grafana.
2. Если Flink работает нормально, но не успевает — увеличить parallelism:
```bash
# Отменить job
docker compose exec flink-jobmanager flink cancel <JOB_ID>

# Запустить с большим parallelism (из checkpoint)
docker compose exec flink-jobmanager \
    flink run -py /opt/flink/pipeline/main.py \
    --parallelism 8 -s <checkpoint_path>
```
3. Если проблема в исходном потоке (всплеск) — ждать, lag сойдёт сам.

---

### 3. Ошибки вставки в ClickHouse (алерт `PipelineClickHouseInsertErrors`)

**Симптомы:** `clickhouse_insert_errors_total` > 0, в логах Flink `ClickHouseInsertError`.

**Диагностика:**
```bash
# Проверить доступность ClickHouse
curl http://localhost:8123/?query=SELECT+1

# Логи ClickHouse
docker compose logs clickhouse --tail=50

# Проверить место на диске
docker compose exec clickhouse df -h
```

**Действия:**
```bash
# Если нет места — очистить временные файлы ClickHouse
docker compose exec clickhouse \
    clickhouse-client --query \
    "OPTIMIZE TABLE payments.payment_history FINAL SETTINGS mutations_sync=1"

# Перезапустить ClickHouse (данные не потеряются — Flink заретраит)
docker compose restart clickhouse
```

---

### 4. DLQ растёт (алерт `PipelineDLQGrowing`)

**Симптомы:** `kafka_consumergroup_lag{topic="payments.dlq"}` > 0 — consumer lag по DLQ-топику растёт.

**Диагностика:**
```bash
# Посмотреть несколько сообщений из DLQ (DLQ пишется в Kafka, не в ClickHouse)
docker compose exec kafka \
    kafka-console-consumer --bootstrap-server kafka:9092 \
    --topic payments.dlq --max-messages 10 --from-beginning

# Статистика по error_code из DLQ-топика (через kafka-console-consumer + jq)
docker compose exec kafka \
    kafka-console-consumer --bootstrap-server kafka:9092 \
    --topic payments.dlq --from-beginning --timeout-ms 5000 | \
    docker compose exec -T flink-jobmanager python3 -c \
    "import sys,json,collections; msgs=[json.loads(l) for l in sys.stdin if l.strip()]; print(collections.Counter(m.get('error_code') for m in msgs).most_common())" 2>/dev/null
```

**Действия по error_code:**

| Код | Причина | Действие |
|-----|---------|---------|
| E001 | NULL в обязательном поле | Проверить источник / схему |
| E002 | Пустая строка в обязательном поле | Проверить upstream |
| E003 | Невалидная валюта | Добавить валюту в `ALLOWED_CURRENCIES` в config.py |
| E004 | Невалидный source_system | Добавить систему в `ALLOWED_SOURCE_SYSTEMS` |
| E005 | Отрицательная/нулевая сумма | Анализ данных источника |
| E006 | Невалидный timestamp | Проверить timezone/формат на источнике |

---

### 5. Flink checkpoint занимает > 25 с

**Симптомы:** алерт `PipelineFlinkCheckpointSlow`.

**Диагностика:**
```bash
# История checkpoint'ов
curl http://localhost:8082/jobs/<JOB_ID>/checkpoints | python -m json.tool

# Размер state backend (hashmap в dev, filesystem checkpoints)
docker compose exec flink-taskmanager du -sh /flink-checkpoints/
```

**Действия:**
```bash
# Вариант 1: Увеличить managed memory TaskManager
# В docker-compose.yml увеличить taskmanager.memory.process.size до 4096m

# Вариант 2: Уменьшить state (снизить TTL MapState с 24h до 12h в scd_merger.py)

# Вариант 3: Включить локальное восстановление (уже включено по умолчанию в Flink 1.19)
# state.backend.local-recovery: true
```

---

### 6. Flink TaskManager OOM

**Симптомы:** TaskManager перезапускается, в логах `java.lang.OutOfMemoryError`.

**Действия:**
```bash
# 1. Увеличить heap в docker-compose.yml:
# taskmanager.memory.process.size: 4096m

# 2. Проверить и уменьшить batch size в ClickHouse sink (pipeline/config.py):
# CLICKHOUSE_BATCH_SIZE=250 (вместо 500)

# 3. Перезапустить с новыми настройками
docker compose up -d flink-taskmanager
```

---

## Плановые операционные задачи

### Еженедельно

```bash
# Принудительная оптимизация ClickHouse (дедупликация ReplacingMergeTree)
docker compose exec clickhouse clickhouse-client --query \
    "OPTIMIZE TABLE payments.payment_current FINAL"

# Проверка DQ
python tests/test_data_quality.py

# Проверка объёма данных
docker compose exec clickhouse clickhouse-client --query \
    "SELECT
         table,
         formatReadableSize(sum(bytes_on_disk)) AS disk_size,
         sum(rows) AS rows
     FROM system.parts
     WHERE database = 'payments' AND active
     GROUP BY table"
```

### При изменении нагрузки / квартально

```bash
# Валидация SLA: нарастающая нагрузка 200 → 1000 ev/s (3 ступени × 2 мин)
# Результат — в results/load_report_ramp.json (baseline не перезаписывается)
python tests/load_test.py --scenario ramp --rps-start 200 --rps-peak 1000 --ramp-steps 3 --step-duration 120 --output results/load_report_ramp.json

# SLA: e2e p99 ≤ 30 с до ~480 ev/s; при 1000 ev/s события буферизуются в Kafka
# (горизонтальное масштабирование TaskManager восстанавливает SLA)
```

### Ежемесячно

```bash
# Проверка retention (TTL-удаление старых данных)
docker compose exec clickhouse clickhouse-client --query \
    "SELECT min(event_ts), max(event_ts), count()
     FROM payments.payment_history"

# Ротация Prometheus данных (автоматически через --storage.tsdb.retention.time=15d)
```

---

## Процедура полного восстановления (Disaster Recovery)

### Сценарий: полная потеря ClickHouse данных

1. Остановить Flink job (чтобы не писать в пустую БД):
```bash
docker compose exec flink-jobmanager flink cancel <JOB_ID>
```

2. Пересоздать таблицы:
```bash
docker compose exec clickhouse clickhouse-client < infra/clickhouse/init.sql
```

3. Запустить Flink с offset `earliest` для replay из Kafka:
```bash
# В pipeline/main.py заменить KafkaOffsetsInitializer.latest()
# на KafkaOffsetsInitializer.earliest() и пересобрать образ:
docker compose build flink-jobmanager flink-taskmanager flink-job-submitter
docker compose up -d flink-jobmanager flink-taskmanager flink-job-submitter
```

4. Дождаться обработки всего лога (consumer lag = 0).

5. Запустить DQ-проверки:
```bash
python tests/test_data_quality.py
```

**Важно:** Kafka хранит данные 3 дня (`payments.raw`). При потере данных старше 3 дней RPO ≠ 0. Для production необходимо настроить репликацию ClickHouse или регулярные snapshot'ы.

---

## Контакты и эскалация

| Уровень | Когда | Контакт |
|---------|-------|---------|
| L1 (on-call) | Любой critical-алерт | Канал #pipeline-alerts |
| L2 (lead engineer) | Проблема не решена за 30 мин | Telegram: @platform_lead |
| L3 (vendor) | Баги Flink / ClickHouse | GitHub Issues соответствующих проектов |
