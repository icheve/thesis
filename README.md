# NRT Payment Pipeline

Дипломный проект — Ичёв Евгений Вадимович, ВШЭ, 2026  
Научный руководитель — Заигрин Вадим Валерьевич

Разработка нагруженного в режиме близкого к реальному времени конвейера данных поставки платёжной информации, с сохранением истории платежей

**SLA:** p99 end-to-end latency ≤ 30 секунд. Ёмкость 1 TaskManager (parallelism=4) — ~480 событий/сек (TC-C01). После перевода ClickHouse sink на async-буферизацию горизонтальное масштабирование **верифицировано**: 2 TM (parallelism=8) обрабатывают 500 событий/сек с p99 = 14 с (TC-C06, SLA ✅). Потолок одноузлового ClickHouse — ~500 событий/сек INSERT-записи (TC-C07).

---

## Стек

| Компонент | Технология |
|-----------|-----------|
| Брокер сообщений | Apache Kafka 3.x |
| Потоковая обработка | Apache Flink 1.19 (PyFlink) |
| State backend | hashmap (dev) / RocksDB (prod) |
| Аналитическое хранилище | ClickHouse 24.x |
| Схемы | JSON (прототип); Avro + Schema Registry — production-расширение |
| Мониторинг | Prometheus + Grafana |
| Оркестрация (dev) | Docker Compose |

---

## Быстрый старт

> **Номинальная конфигурация по умолчанию — 2 TaskManagers, parallelism=8** (`flink-taskmanager` + `flink-taskmanager-2`).
> Для воспроизведения baseline-стенда (1 TM, parallelism=4): задайте `FLINK_PARALLELISM=4` в `.env`, затем:
> ```bash
> docker compose up -d && docker compose stop flink-taskmanager-2
> ```

```bash
# 1. Скопировать конфигурацию окружения
# cp .env.example .env          # Linux/macOS
copy .env.example .env      # Windows PowerShell

# 2. Поднять весь стек (Kafka, ClickHouse, Flink, мониторинг)
docker compose up -d

# 3. Проверить cтатус всех контейнеров
docker compose ps
```

**Ключевые URL после запуска:**

| Сервис | URL |
|--------|-----|
| Flink Web UI | http://localhost:8082 |
| Kafka UI | http://localhost:8080 |
| Grafana | http://localhost:3000 (admin / admin_secret) |
| Prometheus | http://localhost:9090 |
| ClickHouse HTTP | http://localhost:8123 |

---

## Запуск тестов

```bash
pip install -r tests/requirements.txt

# Экспортировать пароль ClickHouse (совпадает с dev-стендом из .env.example)
# export CLICKHOUSE_PASSWORD=pipeline_secret          # Linux/macOS
$env:CLICKHOUSE_PASSWORD = "pipeline_secret"     # Windows PowerShell

# Функциональные тесты (требуют запущенного стека)
pytest tests/test_functional.py -v

# Нагрузочный тест (базовый, 200 ev/s × 5 мин) — результат пишется в results/load_report.json
python tests/load_test.py --scenario baseline --rps 200 --duration 300 --output results/load_report.json

# DQ-тесты
pytest tests/test_data_quality.py -v
```

---

## Структура репозитория

```
pipeline/        # PyFlink job: config, модели, операторы, sinks
generator/       # Генератор тестовых платёжных событий
infra/           # DDL ClickHouse (init.sql)
monitoring/      # Prometheus scrape config + правила алертов
tests/           # Функциональные, DQ и нагрузочные тесты
results/         # Результаты нагрузочного тестирования (JSON)
docs/            # Документация: требования, архитектура, ADR, runbook
docker-compose.yml
```

---

## Документация

> **Актуальная реализация прототипа** описана в разделах [06](docs/06_pipeline_implementation.md), [08](docs/08_testing.md), [09](docs/09_final.md). Разделы 02, 03, 05 описывают целевую production-архитектуру.

- [docs/01_requirements.md](docs/01_requirements.md) — функциональные и нефункциональные требования
- [docs/02_approach.md](docs/02_approach.md) — обзор подходов, выбор архитектуры
- [docs/03_architecture.md](docs/03_architecture.md) — компонентная архитектура, потоки данных
- [docs/04_history_storage.md](docs/04_history_storage.md) — модель хранения SCD Type 2
- [docs/05_data_preparation.md](docs/05_data_preparation.md) — генератор данных
- [docs/06_pipeline_implementation.md](docs/06_pipeline_implementation.md) — реализация пайплайна
- [docs/07_quality_observability.md](docs/07_quality_observability.md) — DQ-проверки, мониторинг
- [docs/08_testing.md](docs/08_testing.md) — стратегия тестирования, тест-кейсы
- [docs/09_final.md](docs/09_final.md) — итоги, результаты измерений, ограничения
- [docs/10_literature_review.md](docs/10_literature_review.md) — обзор литературы
- [docs/runbook.md](docs/runbook.md) — операционный runbook
- [docs/adr/](docs/adr/) — Architecture Decision Records (ADR-001 … ADR-004)

---
