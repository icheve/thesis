# 4. Проектирование хранения истории платежей

---

## 4.1. Модель истории

### Выбор паттерна: SCD Type 2 + Append-only Event Log

Для хранения истории платежей используется **гибридный подход**:

| Таблица | Движок ClickHouse | Паттерн | Назначение | Основные запросы |
|---|---|---|---|---|
| `payment_current` | `ReplacingMergeTree(version)` | Последняя версия | Текущее состояние каждого платежа | Фильтрация, агрегации по актуальным данным |
| `payment_history` | `ReplacingMergeTree(processed_at)` | SCD Type 2 + идемпотентный append | Полная история всех версий | Point-in-time, аудит, восстановление состояния |

**Обоснование гибридного подхода:**
- Чисто append-only log делает дорогими запросы «текущее состояние» (нужен `GROUP BY + max(version)`)
- Чистый SCD Type 2 без event log усложняет replay и аудит
- Два представления одних данных: `payment_history` — источник истины, `payment_current` — оптимизированное представление для аналитики

---

### Жизненный цикл платежа — пример

```
Событие 1: CREATED     (ts: 10:00:00)
Событие 2: AUTHORIZED  (ts: 10:00:03)
Событие 3: CLEARED     (ts: 10:05:00)
Событие 4: REFUNDED    (ts: 11:30:00)
```

**Состояние `payment_history` после обработки всех событий:**

| payment_id | version | event_type | status_normalized | effective_from | effective_to |
|---|---|---|---|---|---|
| PAY-001 | 1 | CREATED | PENDING | 10:00:00.000 | 10:00:03.000 |
| PAY-001 | 2 | AUTHORIZED | AUTHORIZED | 10:00:03.000 | 10:05:00.000 |
| PAY-001 | 3 | CLEARED | COMPLETED | 10:05:00.000 | 11:30:00.000 |
| PAY-001 | 4 | REFUNDED | REFUNDED | 11:30:00.000 | NULL |

**`effective_to = NULL`** означает текущую (последнюю) версию.

> **Point-in-time запрос (корректный вариант).**  
> `payment_history` — append-only лог: при создании новой версии старая строка *не удаляется*, а добавляется новая строка с `effective_to = event_ts_новой_версии`. Поэтому прямой фильтр `WHERE effective_to IS NULL` даёт верный результат только в случае, если гарантировано ровно одна открытая строка на платёж. Для надёжного запроса «текущее состояние на момент T» или «последняя версия» используйте:
>
> ```sql
> -- Текущая (последняя) версия каждого платежа:
> SELECT *
> FROM payments.payment_history
> WHERE (payment_id, version) IN (
>     SELECT payment_id, max(version)
>     FROM payments.payment_history
>     GROUP BY payment_id
> );
>
> -- Состояние платежа на момент времени T:
> SELECT *
> FROM payments.payment_history
> WHERE payment_id = 'PAY-001'
>   AND effective_from <= T
>   AND (effective_to > T OR effective_to IS NULL)
> ORDER BY version DESC
> LIMIT 1;
> ```
>
> Реализация — **SCD Type 2 поверх `ReplacingMergeTree(processed_at)`**. При at-least-once replay одна и та же строка может быть вставлена повторно с новым `processed_at`; движок оставляет вариант с наибольшим `processed_at` при merge или `SELECT FINAL`. Это обеспечивает идемпотентность записи без UPDATE.

**Состояние `payment_current` (ReplacingMergeTree, по max version):**

| payment_id | version | status_normalized | event_ts |
|---|---|---|---|
| PAY-001 | 4 | REFUNDED | 11:30:00.000 |

---

### Поля модели истории — детальное описание

| Поле | Тип | Nullable | Описание |
|---|---|---|---|
| `payment_id` | String | Нет | Бизнес-ключ платежа из системы-источника |
| `source_system` | String | Нет | Код источника (ACQUIRING, MOBILE_APP, …) |
| `version` | UInt64 | Нет | Монотонно возрастающий счётчик версий платежа |
| `event_id` | String | Нет | UUID входящего события (для дедупликации) |
| `event_type` | String | Нет | Нормализованный тип: CREATED, AUTHORIZED, … |
| `status_normalized` | LowCardinality(String) | Нет | Статус во внутренней номенклатуре |
| `event_ts` | DateTime64(3) | Нет | Event time — время возникновения события в источнике |
| `effective_from` | DateTime64(3) | Нет | С какого момента версия действительна (= event_ts) |
| `effective_to` | DateTime64(3) | Да | До какого момента (NULL = текущая версия) |
| `processed_ts` | DateTime64(3) | Нет | Processing time — время обработки в Flink |
| `amount_original` | Decimal(18,4) | Нет | Сумма в исходной валюте |
| `currency_original` | String | Нет | ISO 4217 исходной валюты |
| `amount_rub` | Decimal(18,4) | Нет | Сумма в RUB по курсу на moment event_ts |
| `exchange_rate` | Decimal(18,6) | Нет | Применённый курс конвертации |
| `payer_id` | String | Да | Токенизированный ID плательщика |
| `payee_id` | String | Да | Токенизированный ID получателя |
| `merchant_id` | String | Да | Идентификатор мерчанта |
| `merchant_name` | String | Да | Название мерчанта (из справочника) |
| `merchant_category` | LowCardinality(String) | Да | MCC-категория мерчанта |
| `card_token` | String | Да | Токен платёжной карты (не PAN) |
| `_ingestion_ts` | DateTime64(3) | Нет | Технический: время вставки строки в ClickHouse |

---

## 4.2. Ключи, дедупликация и идемпотентность

### Натуральный и суррогатный ключи

| Тип ключа | Состав | Уникальность | Применение |
|---|---|---|---|
| **Натуральный бизнес-ключ** | `(payment_id, source_system)` | Уникален в рамках источника | Идентификация платежа |
| **Ключ версии** | `(payment_id, source_system, version)` | Уникален строки истории | ORDER BY в `payment_history` |
| **Ключ события** | `event_id` (UUID) | Глобально уникален | Дедупликация на уровне таблицы |

> Суррогатный UUID-ключ для строки истории не вводится намеренно: натуральный ключ `(payment_id, source_system, version)` достаточен и обеспечивает корректный ORDER BY в ClickHouse.

---

### Уровни дедупликации

Дедупликация реализована на **трёх уровнях** для обеспечения идемпотентности:

#### Уровень 1 — Kafka (брокер)
- Продюсер использует `enable.idempotence=true` + `acks=all`
- Ключ сообщения = `payment_id` → одно событие не может быть записано дважды в одну партицию при сбое продюсера
- Ограничение: защищает только от дублей в рамках одной producer-сессии

#### Уровень 2 — Flink (stateful processing)
- Flink хранит в keyed state по `payment_id` множество обработанных `event_id` (с TTL = 24 ч)
- При получении события проверяется: `event_id ∈ processed_event_ids`
- Если да → событие помечается `is_duplicate=true` и не записывается в sink
- Ограничение: state сбрасывается при превышении TTL (защита от долгосрочных дублей — на уровне 3)

```
Алгоритм (Flink):
  state: Map<payment_id, Set<event_id>>  (TTL: 24h)

  on receive event(e):
    if e.event_id ∈ state[e.payment_id]:
      emit(e with is_duplicate=true)  → не пишем в ClickHouse
      return
    state[e.payment_id].add(e.event_id)
    process(e)
```

#### Уровень 3 — ClickHouse (storage)

> **Реализация:** `payment_history` использует `ReplacingMergeTree(processed_at)`. При повторной вставке одной и той же строки (тот же `(payment_id, source_system, version)` в ключе сортировки) движок оставит вариант с наибольшим `processed_at` при фоновом merge или явном `SELECT FINAL`. Это обеспечивает идемпотентность на уровне хранилища как третий рубеж защиты при at-least-once replay из Kafka. `field_hash` сохраняется в строке `payment_history` и используется в L2-дедупликации Flink для контентного сравнения без повторного обращения к ClickHouse.

- `payment_current` — `ReplacingMergeTree(version)`: при merge оставляет строку с максимальным `version` на ключ `(payment_id, source_system)`.
- В аналитических запросах, где важна консистентность, добавляйте `FINAL`: `SELECT ... FROM payment_history FINAL WHERE ...`.

---

### Алгоритм SCD Type 2 Merge в Flink

Полный псевдокод оператора `PaymentHistoryOperator`:

```
Входные данные:
  event             — текущее входящее событие
  state[payment_id] — состояние последней версии платежа в Flink keyed state

Алгоритм:

1. Загрузить из state текущую версию: current = state.get(event.payment_id)

2. ДЕДУПЛИКАЦИЯ:
   if current != null AND event.event_id ∈ current.processed_event_ids:
     → пропустить, emit(is_duplicate=true)
     → RETURN

3. СРАВНЕНИЕ:
   Вычислить хеш значимых полей события:
     hash_new = hash(event.status, event.amount, event.event_type)
   if current != null:
     hash_current = current.field_hash
     if hash_new == hash_current:
       → событие не несёт новой информации (дубль по содержимому)
       → обновить processed_event_ids в state
       → RETURN

4. ЗАКРЫТИЕ ПРЕДЫДУЩЕЙ ВЕРСИИ:
   if current != null:
     closed_row = current.copy()
     closed_row.effective_to = event.event_ts
     emit(closed_row) → запись обновлённой строки в payment_history

5. СОЗДАНИЕ НОВОЙ ВЕРСИИ:
   new_version = (current == null) ? 1 : current.version + 1
   new_row = PaymentHistoryRow {
     payment_id      = event.payment_id,
     source_system   = event.source_system,
     version         = new_version,
     event_id        = event.event_id,
     event_type      = event.event_type,
     status_normalized = normalize(event.status),
     event_ts        = event.event_ts,
     effective_from  = event.event_ts,
     effective_to    = NULL,           ← текущая версия
     processed_ts    = now(),
     amount_original = event.amount,
     ...прочие поля...,
     field_hash      = hash_new
   }

6. ОБНОВЛЕНИЕ STATE:
   state.update(new_row)
   state.processed_event_ids.add(event.event_id)

7. EMIT:
   emit(new_row) → INSERT в payment_history (новая строка)
   emit(new_row) → UPSERT в payment_current (заменяет старую)
```

---

### Обработка Late Arrivals (опоздавших событий)

**Проблема:** событие с `event_ts = T-5min` приходит после событий с `event_ts = T` и `T+2min`. Уже записанная история некорректна — в ней нет этой версии.

**Watermark-стратегия в Flink:**
- Watermark = max(event_ts) - 10 min (допустимое опоздание)
- События с event_ts < watermark считаются опоздавшими и в production-архитектуре должны направляться в side output `late_events` → Kafka topic `payments.late` для отдельной обработки

> **⚠️ Ограничение текущей реализации (Beam PyFlink runtime):** маршрутизация опоздавших событий через side output отключена — `ctx.timer_service()` не поддерживается в Beam-рантайме PyFlink 1.19. Все события, включая опоздавшие, обрабатываются в основном потоке. Это **приемлемо** для прототипа, поскольку ScdMergerOperator всё равно выставляет `effective_from` из `event_ts` оригинального события (не из processing time), корректно сохраняя хронологию истории. Топик `payments.late` создан в Kafka, но в текущей версии не заполняется. Для production необходим переход на native Flink (Java/Scala) или отдельная batch-процедура коррекции.

**Алгоритм коррекции истории для late arrival:**

```
Получено опоздавшее событие L с event_ts = T_late:

1. Найти в payment_history все версии платежа с payment_id = L.payment_id

2. Найти «место вставки»:
   prev_version = max версии, где effective_from <= T_late
   next_version = min версии, где effective_from > T_late

3. ВСТАВИТЬ новую версию между ними:
   - Новой версии назначается version = next_version.version (существующие версии сдвигаются)
     ИЛИ альтернативно: используется дробная нумерация / пересчёт version для всех последующих
   - Закрыть предыдущую версию: prev_version.effective_to = T_late
   - Открыть новую: new_row.effective_from = T_late, effective_to = next_version.effective_from

4. Обновить payment_current если new_row — самая последняя по event_ts
```

> **Практическое ограничение:** пересчёт версий в ClickHouse требует DELETE + re-INSERT строк, что не является его сильной стороной. Поэтому для опоздавших событий допустима **альтернативная стратегия**: вставка late-версии в отдельный раздел `payment_history_late` с флагом `is_late=true`, и объединение в аналитических запросах через UNION ALL с корректным ORDER BY.

---

## 4.3. Партиционирование и Retention

### Стратегия партиционирования

**Ключ партиционирования:** `toYYYYMM(event_ts)` — партиция по году и месяцу события.

**Обоснование:**
- Типичные аналитические запросы фильтруют по временному диапазону → partition pruning отсекает нерелевантные партиции
- TTL-удаление работает на уровне партиции → эффективное удаление старых данных целыми блоками
- Размер партиции: ~500 МБ/мес (при baseline 200 событий/сек × 86400 с × 30 дней) — управляемый размер

**Дополнительные индексы в ClickHouse:**

```sql
-- Для быстрого поиска по merchant_id (при фильтрации транзакций мерчанта)
ALTER TABLE payment_history ADD INDEX idx_merchant merchant_id TYPE bloom_filter GRANULARITY 4;

-- Для быстрого поиска по payer_id
ALTER TABLE payment_history ADD INDEX idx_payer payer_id TYPE bloom_filter GRANULARITY 4;
```

---

### Политика хранения (Retention)

| Уровень данных | Хранилище | Срок | Механизм |
|---|---|---|---|
| **Горячие** (последние 3 месяца) | `payment_history` в ClickHouse (SSD) | 3 месяца | — |
| **Тёплые** (3–12 месяцев) | `payment_history` в ClickHouse (HDD tier) | 12 месяцев | ClickHouse storage policy: JBOD/Hot-Warm |
| **Холодные** (1–3 года) | `payment_history` в ClickHouse (HDD) с TTL | 3 года | TTL + автоматическое удаление |
| **Архивные** (> 3 лет) | Выгрузка в Parquet на S3 / object storage | Без срока | Cron-задача раз в месяц |

**TTL-правило в ClickHouse:**
```sql
-- Удаление строк старше 3 лет
ALTER TABLE payment_history MODIFY TTL toDateTime(event_ts) + INTERVAL 3 YEAR DELETE;

-- Перемещение на «холодный» диск через 12 месяцев (storage policy)
ALTER TABLE payment_history MODIFY TTL
    toDateTime(event_ts) + INTERVAL 12 MONTH TO VOLUME 'cold',
    toDateTime(event_ts) + INTERVAL 3 YEAR DELETE;
```

---

### Capacity Planning

**Допущения:**
- Средняя нагрузка: 200 событий/сек
- Пиковая нагрузка: 1 000 событий/сек (5× burst в течение 1 ч/день)
- Среднее число версий на платёж: ~3 (CREATED → AUTHORIZED → CLEARED)
- Средний размер строки `payment_history` (сжатая): ~300 байт (LZ4)

**Расчёт объёма:**

| Период | Событий | Строк в history | Объём (сжатый) |
|---|---|---|---|
| 1 сутки | 200 × 86 400 = 17,3 млн | ~17,3 млн | ~5,2 ГБ |
| 1 месяц | ~519 млн | ~519 млн | ~156 ГБ |
| 1 год | ~6,3 млрд | ~6,3 млрд | ~1,9 ТБ |
| 3 года (max retention) | ~18,9 млрд | ~18,9 млрд | ~5,7 ТБ |

> ClickHouse достигает степени сжатия 5–10× для временных рядов. Приведённые цифры — для коэффициента сжатия ~6× относительно несжатых ~1,8 КБ/строку.

**Kafka Retention:**

| Топик | Retention | Объём/сутки | Итого |
|---|---|---|---|
| `payments.raw` | 7 дней | ~8 ГБ | ~56 ГБ |
| `payments.processed` | 3 дня | ~10 ГБ | ~30 ГБ |
| `payments.dlq` | 30 дней | ~0,05 ГБ | ~1,5 ГБ |

**Flink State (RocksDB):**
- Keyed state size ≈ (число уникальных платежей в окне 24ч) × (размер состояния одного платежа)
- ~5 млн активных платежей/сутки × ~2 КБ/платёж = ~10 ГБ state (управляемо для RocksDB)

---

### Индексы и оптимизация запросов

**Типичные аналитические запросы и их оптимизация:**

```sql
-- 1. Текущее состояние платежей за сегодня (payment_current)
SELECT payment_id, status_normalized, amount_rub
FROM payment_current FINAL          -- FINAL для дедупликации ReplacingMergeTree
WHERE event_ts >= today()
  AND status_normalized = 'PENDING';
-- Оптимизация: partition pruning по event_ts, ORDER BY (payment_id, source_system)

-- 2. Point-in-time: состояние платежа на момент T
SELECT *
FROM payment_history
WHERE payment_id = 'PAY-001'
  AND source_system = 'ACQUIRING'
  AND effective_from <= '2025-06-01 12:00:00'
  AND (effective_to > '2025-06-01 12:00:00' OR effective_to IS NULL)
ORDER BY version DESC
LIMIT 1;
-- Оптимизация: поиск по primary key (payment_id, source_system, version) — O(log n)

-- 3. Все платежи мерчанта за месяц (агрегат)
SELECT merchant_id, count(), sum(amount_rub)
FROM payment_history
WHERE event_ts BETWEEN '2025-06-01' AND '2025-06-30'
  AND merchant_id = 'MCH-500'
  AND effective_to IS NULL          -- только последние версии
GROUP BY merchant_id;
-- Оптимизация: partition pruning по event_ts + bloom_filter по merchant_id

-- 4. Полная история изменений платежа
SELECT version, event_type, status_normalized, effective_from, effective_to, amount_rub
FROM payment_history
WHERE payment_id = 'PAY-001'
  AND source_system = 'ACQUIRING'
ORDER BY version ASC;
```
