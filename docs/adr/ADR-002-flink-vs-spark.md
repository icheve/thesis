# ADR-002: Выбор потокового движка — Flink vs Spark Structured Streaming vs Kafka Streams

| Поле | Значение |
|------|---------|
| **Дата** | 2026-01-20 |
| **Статус** | Принято |
| **Авторы** | Команда проекта |

## Контекст

Для реализации SCD Type 2 merge с сохранением состояния на ключе требуется потоковый движок с:
- Stateful обработкой на уровне ключа (per-key state)
- Точным управлением временем событий (event time, watermarks)
- Гарантиями exactly-once (или effectively-exactly-once)
- Python API (требование проекта)

## Рассматриваемые варианты

### Weighted Decision Matrix

| Критерий | Вес | Flink 1.19 | Spark SS 3.5 | Kafka Streams |
|----------|-----|-----------|-------------|---------------|
| Per-key stateful API | 0.25 | 5 | 3 | 4 |
| Event time / watermarks | 0.20 | 5 | 4 | 2 |
| Python API качество | 0.15 | 4 | 5 | 1 |
| Exactly-once гарантии | 0.15 | 5 | 4 | 4 |
| Латентность (мс) | 0.15 | 5 | 3 | 5 |
| Зрелость / документация | 0.10 | 5 | 5 | 4 |
| **Итог** | | **4.80** | **3.85** | **3.15** |

### Apache Flink

**Плюсы:**
- `KeyedProcessFunction` — идеальная абстракция для SCD merge: вызов `process_element` гарантированно изолирован по ключу
- `ValueState` / `MapState` с TTL — управление состоянием без внешнего кэша
- Точное управление watermarks (`BoundedOutOfOrderness`)
- Задержка обработки < 100 мс (sub-second latency)
- PyFlink DataStream API стабилен начиная с 1.16

**Минусы:**
- Python API менее богат, чем Java/Scala API
- Необходимость писать Java-адаптеры для некоторых операций (сериализация)
- Сложнее в развёртывании, чем Kafka Streams

### Spark Structured Streaming

**Плюсы:**
- Отличный Python API (PySpark)
- Богатая экосистема ML / batch
- Micro-batch mode прост в отладке

**Минусы:**
- Micro-batch latency: минимум ~500 мс, реально 1–5 с для `processing-time` trigger
- `mapGroupsWithState` / `flatMapGroupsWithState` — сложнее, чем Flink `KeyedProcessFunction`, и имеет ограничения на тип состояния
- Watermark semantics менее гибкие

### Kafka Streams

**Плюсы:**
- Нативная интеграция с Kafka
- Минимальная операционная сложность (библиотека, а не отдельный кластер)
- Очень низкая латентность

**Минусы:**
- Только Java/Scala API (нет Python)
- KTable / KStream state stores менее гибкие, чем Flink state
- Сложнее реализовать SCD merge c side outputs

## Решение

**Выбран Apache Flink 1.19** (PyFlink DataStream API).

Ключевые причины:
1. `KeyedProcessFunction` обеспечивает изоляцию состояния по `payment_id` без дополнительной синхронизации.
2. Нативная поддержка side outputs (`OutputTag`) — необходима для маршрутизации в DLQ и late events.
3. Чекпоинты RocksDB обеспечивают exactly-once семантику состояния при отказах.
4. Задержка pipeline ~2–3 с (p50), что значительно ниже SLA 30 с.

## Последствия

- PyFlink накладывает ограничение: `ValueState` хранит `dict`, а не кастомные Python-объекты (проблема сериализации). Реализованы helper-функции `_dict_to_history_row()` / `_history_row_to_dict()`.
- Для запуска Flink требуется отдельный кластер (JobManager + ≥1 TaskManager) — добавлена операционная сложность.
- При горизонтальном масштабировании достаточно увеличить `--parallelism` без изменения кода.
