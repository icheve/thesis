# Шаг 8. Тестирование и нагрузочное тестирование

## 8.1 Стратегия тестирования

Тестирование охватывает три уровня, соответствующих стандарту пирамиды тестирования: модульное (unit), интеграционное и системное (сквозное + нагрузочное).

```
         ┌────────────────────────────┐
         │   Нагрузочное / E2E         │   < 10% кейсов, медленные
         │  (load_test.py, TC-C01..03) │
         ├────────────────────────────┤
         │   Интеграционные (pytest)   │   ~ 30% кейсов
         │  (test_functional.py)       │
         ├────────────────────────────┤
         │   Unit-тесты (pytest)       │   ~ 60% кейсов, быстрые
         │  (test_units.py, DQ-tests)  │
         └────────────────────────────┘
```

### Тестовое окружение

| Параметр | Значение |
|----------|---------|
| Развёртывание | `docker compose up -d` (см. `docker-compose.yml`) |
| Kafka | `localhost:29092` (внешний listener) |
| ClickHouse | `localhost:8123` (HTTP API) |
| Flink Web UI | `localhost:8082` |
| Prometheus | `localhost:9090` |
| Grafana | `localhost:3000` |

Перед запуском тестов убедиться, что все сервисы здоровы:

```bash
docker compose ps
docker compose exec kafka kafka-topics \
    --bootstrap-server kafka:9092 --list
```

---

## 8.2 Функциональные тест-кейсы

Тест-кейсы разбиты на три группы:
- **Группа A** — базовые функциональные сценарии
- **Группа B** — граничные случаи и SCD-логика
- **Группа C** — нагрузочные сценарии

### Группа A — Базовые функциональные сценарии

#### TC-A01: Успешная обработка нового платежа

| Поле | Значение |
|------|---------|
| **ID** | TC-A01 |
| **Цель** | Новое событие проходит полный pipeline и появляется в ClickHouse |
| **Предусловия** | Таблицы payment_current и payment_history пусты (или не содержат тестовый payment_id) |

**Входные данные:**
```json
{
  "event_id": "evt-a01-001",
  "payment_id": "pay-a01-001",
  "status": "INITIATED",
  "amount": 1500.00,
  "currency": "RUB",
  "source_system": "MOBILE_APP",
  "event_type": "PAYMENT_CREATED",
  "merchant_id": "mrc-001",
  "payer_id": "usr-001",
  "event_ts": "<текущее время в ms>",
  "version": 1
}
```

**Шаги:**
1. Отправить событие в топик `payments.raw`
2. Подождать ≤ 30 секунд (SLA)
3. Запросить ClickHouse

**Ожидаемый результат:**

```sql
-- В payment_current:
SELECT * FROM payments.payment_current FINAL WHERE payment_id = 'pay-a01-001';
-- Должна быть 1 строка: status='INITIATED', is_current=1, version=1

-- В payment_history:
SELECT * FROM payments.payment_history WHERE payment_id = 'pay-a01-001';
-- Должна быть 1 строка: is_current=1, effective_to='2099-12-31...'

-- amount_rub должен быть > 0 (обогащение курсом)
```

**Проверки:**
- [ ] Запись появилась в `payment_current` в течение 30s
- [ ] Запись появилась в `payment_history`
- [ ] `amount_rub > 0` (обогащение курсом)
- [ ] `merchant_name` не пустое
- [ ] `payer_id` принят как токен источника (PII-маскировка в Flink не реализована в прототипе — TODO)
- [ ] Для валидного события сообщений в Kafka `payments.dlq` нет

---

#### TC-A02: Переход статуса (SCD Type 2 — создание новой версии)

| Поле | Значение |
|------|---------|
| **ID** | TC-A02 |
| **Цель** | При изменении значимого поля создаётся новая версия, предыдущая закрывается |

**Входные данные:** два события для одного payment_id:
1. `version=1, status=INITIATED`
2. `version=2, status=PROCESSING` (отправить через 2s после первого)

**Ожидаемый результат:**

```sql
-- В payment_history должны быть 2 строки:
SELECT version, status, is_current, effective_from, effective_to
  FROM payments.payment_history
 WHERE payment_id = 'pay-a02-001'
 ORDER BY version;
-- version=1: is_current=0, effective_to = effective_from версии 2
-- version=2: is_current=1, effective_to = '2099-12-31...'

-- В payment_current FINAL только 1 строка (последняя версия):
SELECT count() FROM payments.payment_current FINAL
 WHERE payment_id = 'pay-a02-001';
-- = 1, status = 'PROCESSING'
```

**Проверки:**
- [ ] `payment_history` содержит ровно 2 строки
- [ ] Версия 1 имеет `is_current=0`
- [ ] Версия 2 имеет `is_current=1`
- [ ] `effective_to` версии 1 = `effective_from` версии 2 (непрерывность)

---

#### TC-A03: Дедупликация — повторная отправка того же события

| Поле | Значение |
|------|---------|
| **ID** | TC-A03 |
| **Цель** | Повторное событие с тем же event_id не создаёт дублей |

**Входные данные:** одно и то же событие отправить в `payments.raw` дважды с паузой 5s.

**Ожидаемый результат:**
```sql
SELECT count() FROM payments.payment_history WHERE payment_id = 'pay-a03-001';
-- = 1  (не 2)
```

**Проверки:**
- [ ] В `payment_history` ровно 1 строка
- [ ] В `payment_current` ровно 1 строка (FINAL)
- [ ] В Kafka `payments.dlq` нет сообщений для данного payment_id (дубль отброшен L2-дедупликацией Flink)

---

#### TC-A04: Валидация — невалидное событие уходит в DLQ

| Поле | Значение |
|------|---------|
| **ID** | TC-A04 |
| **Цель** | Событие с недопустимой валютой попадает в DLQ и не записывается в ClickHouse |
> **Ограничение прототипа:** Flink записывает DLQ в Kafka-топик `payments.dlq`, а не в ClickHouse. Проверка факта попадания в DLQ требует Kafka-consumer helper. Тест TC-A04 (`test_in_dlq`) помечен `xfail` в `tests/test_functional.py` — это известное ограничение dev-прототипа. Что проверяется: отсутствие записи в ClickHouse для невалидного `payment_id`.
**Входные данные:**
```json
{
  "event_id": "evt-a04-001",
  "payment_id": "pay-a04-001",
  "status": "INITIATED",
  "amount": 100.0,
  "currency": "ZZZ",
  "source_system": "MOBILE_APP",
  "event_type": "PAYMENT_CREATED",
  "merchant_id": "mrc-001",
  "payer_id": "usr-001",
  "event_ts": "<текущее время>",
  "version": 1
}
```

**Проверки:**
- [ ] Запись попадает в `payments.dlq` (Kafka-топик) с `error_code='E003'`
- [ ] В `payment_current` нет записи для `pay-a04-001`
- [ ] В `payment_history` нет записи

---

### Группа B — Граничные случаи

#### TC-B01: Полный жизненный цикл платежа

| Поле | Значение |
|------|---------|
| **ID** | TC-B01 |
| **Цель** | Платёж проходит все 3 статуса: INITIATED→PROCESSING→COMPLETED |

**Входные данные:** 3 события, version=1,2,3, с паузой 1s.

**Ожидаемый результат:**
```sql
SELECT version, status FROM payments.payment_history
 WHERE payment_id = 'pay-b01-001'
 ORDER BY version;
-- 3 строки: INITIATED, PROCESSING, COMPLETED
-- Только последняя — is_current=1
```

---

#### TC-B02: Идемпотентность контентного хэша

| Поле | Значение |
|------|---------|
| **ID** | TC-B02 |
| **Цель** | Два события с разным event_id, но одинаковыми полями не создают дубля |

**Входные данные:**
- `event_id=evt-b02-001, payment_id=pay-b02-001, version=2, status=PROCESSING`
- `event_id=evt-b02-002, payment_id=pay-b02-001, version=2, status=PROCESSING`

**Ожидаемый результат:** В `payment_history` для `pay-b02-001` и `version=2` — 1 строка.

---

#### TC-B03: Отрицательная сумма — валидационная ошибка

| Поле | Значение |
|------|---------|
| **ID** | TC-B03 |
| **Цель** | Событие с `amount=-1.0` уходит в DLQ с кодом E001 |
> **Ограничение прототипа:** аналогично TC-A04 — `test_in_dlq` помечен `xfail` в `tests/test_functional.py` (DLQ пишется в Kafka, не в ClickHouse). Что проверяется: отсутствие записи в ClickHouse для невалидного `payment_id`.
---

#### TC-B04: Пустой payer_id — поле опциональное, событие обрабатывается

| Поле | Значение |
|------|---------|
| **ID** | TC-B04 |
| **Цель** | Событие с пустым `payer_id` успешно обрабатывается — поле опциональное, DLQ не ожидается |

---

#### TC-B05: Статус не меняется (нет новой версии)

| Поле | Значение |
|------|---------|
| **ID** | TC-B05 |
| **Цель** | Если содержимое не изменилось (контентный хэш совпадает), новая версия не создаётся |

**Входные данные:**
- `version=1, status=INITIATED, amount=100.0`
- `version=1 (повтор с другим event_id, но все значимые поля одинаковы)`

**Ожидаемый результат:** В `payment_history` 1 строка (не 2).

---

#### TC-B06: Нулевая сумма — валидационная ошибка

| Поле | Значение |
|------|---------|
| **ID** | TC-B06 |
| **Цель** | Событие с `amount=0.0` уходит в DLQ с кодом E001 |

---

#### TC-B07: Поздно прибывшее событие (late arrival) — **xfail**

| Поле | Значение |
|------|---------|
| **ID** | TC-B07 |
| **Цель** | Событие с `event_ts` старше watermark должно маршрутизироваться в `payments.late` |
| **Статус** | ⚠️ **xfail** — маршрутизация отключена в Beam PyFlink 1.19 runtime (`ctx.timer_service()` не поддерживается) |

**Входные данные:** событие с `event_ts = текущее_время - 15 минут` (watermark = 10 мин).

**Фактическое поведение:** событие обрабатывается в основном потоке (не в `payments.late`), `effective_from` заполняется корректно из `event_ts`. Топик `payments.late` создан, но не заполняется.

---

### Группа C — Нагрузочные сценарии

#### TC-C01: Базовая нагрузка (SLA-тест)

| Параметр | Значение |
|----------|---------|
| **ID** | TC-C01 |
| **Нагрузка** | 200 событий/сек в течение 5 минут |
| **SLA** | p99 e2e latency ≤ 30s |
| **Метрика** | `pipeline_e2e_latency_seconds` из Prometheus |

**Критерии прохождения:**
- p99 latency ≤ 30s
- Количество событий в DLQ (Kafka `payments.dlq`) < 1% от общего потока
- Нет потерь (сравнить отправлено vs получено в ClickHouse)

---

#### TC-C02: Пиковая нагрузка (stress-тест)

| Параметр | Значение |
|----------|---------|
| **ID** | TC-C02 |
| **Нагрузка** | Ramp: 200 → 600 → 1000 событий/сек (по 2 минуты) + 10× прогон (2000 событий/сек) |
| **SLA** | p99 ≤ 30s при нагрузке ≤ ~480 ев/с (1 TaskManager); при > 480 ев/с SLA по latency нарушается, данные не теряются (Kafka-буфер) |

**Критерии:**
- При перегрузке pipeline не теряет события (буферизация в Kafka)
- После снижения нагрузки consumer lag возвращается к 0 в течение 5 минут

---

#### TC-C03: Сценарий восстановления (failover)

| Параметр | Значение |
|----------|---------|
| **ID** | TC-C03 |
| **Цель** | Проверить RPO=0 и RTO≤5min при падении Flink TaskManager |

**Шаги:**
1. Запустить нагрузку 200 событий/сек
2. Убить TaskManager: `docker compose stop flink-taskmanager`
3. Подождать 30s
4. Запустить TaskManager: `docker compose start flink-taskmanager`
5. Убедиться, что Flink восстановился из checkpoint
6. Проверить полноту данных

**Критерии:**
- RTO ≤ 5 минут (Flink job возобновляет работу)
- RPO = 0 (нет потерянных событий — все буферизованы в Kafka)
- Consumer lag возвращается к 0

---

## 8.3 Матрица покрытия требований

| Требование | Тест-кейсы |
|-----------|-----------|
| FR-01: Приём событий | TC-A01, TC-C01 |
| FR-02: Валидация | TC-A04, TC-B03, TC-B06 |
| FR-03: Обогащение | TC-A01 (amount_rub, merchant_name) |
| FR-04: SCD Type 2 | TC-A02, TC-B01, TC-B05 |
| FR-05: Дедупликация | TC-A03, TC-B02 |
| FR-06: DLQ | TC-A04, TC-B03, TC-B06 |
| FR-07: PII-маскирование | TC-A01 (payer_id — TODO, не реализовано; конвейер работает с токенами источника) |
| FR-08: Late arrivals | TC-B07 (— xfail; маршрутизация отключена в Beam runtime) |
| NFR-01: Latency ≤ 30s p99 | TC-C01 |
| NFR-02: Throughput ≥ 200 ev/s (эфф. ёмкость ~480 ev/s, 1 TM) | TC-C02 |
| NFR-03: RTO ≤ 5min, RPO=0 | TC-C03 |

---

## 8.4 Команды запуска тестов

### Подготовка окружения

```bash
# Поднять инфраструктуру
docker compose up -d

# Дождаться готовности Flink (опрос Web UI)
while ! curl -s http://localhost:8082/jobs | grep -q '"status":"RUNNING"'; do
    echo "Waiting for Flink job..."; sleep 5
done
echo "Flink job is RUNNING"
```

### Функциональные тесты

```bash
# Установить зависимости
pip install -r tests/requirements.txt

# Запустить все функциональные тесты
pytest tests/test_functional.py -v

# Только конкретную группу
pytest tests/test_functional.py -v -k "TC_A"
pytest tests/test_functional.py -v -k "TC_B"

# С подробным выводом при ошибках
pytest tests/test_functional.py -v --tb=short
```

### DQ-тесты

```bash
pytest tests/test_data_quality.py -v

# Standalone отчёт (без pytest)
python tests/test_data_quality.py
```

### Нагрузочные тесты

```bash
# TC-C01: базовая нагрузка 200 ev/s × 5 мин
python tests/load_test.py --scenario baseline --rps 200 --duration 300

# TC-C02: нарастающая нагрузка
python tests/load_test.py --scenario ramp --rps-start 200 --rps-peak 2000 \
    --ramp-steps 3 --step-duration 120

# TC-C03: failover (ручной шаг по инструкции выше)
python tests/load_test.py --scenario baseline --rps 200 --duration 600 &
# (в другом терминале: docker compose stop/start flink-taskmanager)
```

### Сборка отчёта

```bash
# Метрики из Prometheus (после нагрузочного теста)
python tests/load_test.py --report \
    --prometheus http://localhost:9090 \
    --output results/load_report.json
```

---

## 8.5 Интерпретация результатов

### Нормальные показатели при 200 ev/s

| Метрика | Ожидаемое значение |
|---------|--------------------|
| e2e latency p50 | < 3s |
| e2e latency p99 | < 15s |
| Consumer lag | < 1000 сообщений |
| Flink checkpoint duration | < 5s |
| ClickHouse insert latency | < 500ms |
| DLQ rate | < 0.1% |
| Flink JVM heap | < 60% |

### Признаки деградации

| Симптом | Причина | Действие |
|---------|---------|---------|
| Latency p99 > 30s | Consumer lag растёт | Увеличить parallelism Flink |
| Checkpoint duration > 25s | Большой state в RocksDB | Увеличить state backend memory |
| CH INSERT errors | ClickHouse перегружен или недоступен | Проверить CH health, очередь буферов |
| DLQ rate > 1% | Изменилась схема источника | Проанализировать error_code в DLQ |
| JVM heap > 80% | Утечка памяти или большие окна | Проверить TTL ValueState, уменьшить batch size |
