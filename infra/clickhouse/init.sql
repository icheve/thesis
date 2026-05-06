-- =============================================================
--  ClickHouse DDL для NRT Payment Pipeline
--  Выполняется автоматически при первом старте контейнера
-- =============================================================

CREATE DATABASE IF NOT EXISTS payments;

-- -----------------------------------------------------------
-- Пользователь для pipeline создаётся скриптом 00-create-user.sh,
-- который читает пароль из переменной окружения CLICKHOUSE_PIPELINE_PASSWORD.
-- -----------------------------------------------------------

-- -----------------------------------------------------------
-- Хранение текущего состояния платежа (ReplacingMergeTree)
-- Дедуплицируется по version при OPTIMIZE / FINAL запросах
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments.payment_current
(
    payment_id          String,
    version             UInt64,
    source_system       LowCardinality(String),
    event_type          LowCardinality(String),
    status_normalized   LowCardinality(String),
    event_ts            DateTime64(3),
    processed_ts        DateTime64(3),
    amount_original     String,
    currency_original   LowCardinality(String),
    amount_rub          String,
    exchange_rate       String,
    payer_id            String,
    payee_id            String,
    merchant_id         String,
    merchant_name       String,
    merchant_category   LowCardinality(String),
    card_token          String,
    -- is_current всегда 1: актуальность гарантируется ReplacingMergeTree(version) + FINAL
    is_current          UInt8 DEFAULT 1
)
ENGINE = ReplacingMergeTree(version)
-- PARTITION BY tuple(): нет партиционирования — необходимо, чтобы ReplacingMergeTree
-- дедуплицировал платёж со сменой event_ts через границу партиции (payment апрель → май).
PARTITION BY tuple()
ORDER BY (payment_id, source_system)
SETTINGS index_granularity = 8192;


-- -----------------------------------------------------------
-- Append-only история всех версий (SCD Type 2)
-- ReplacingMergeTree(processed_at): при дублях (AT_LEAST_ONCE replay)
-- ClickHouse оставляет строку с максимальным processed_at.
-- Для гарантированно дедуплицированного чтения используйте FINAL.
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments.payment_history
(
    payment_id          String,
    version             UInt64,
    event_id            String,
    source_system       LowCardinality(String),
    event_type          LowCardinality(String),
    status_normalized   LowCardinality(String),
    event_ts            DateTime64(3),
    effective_from      DateTime64(3),
    effective_to        Nullable(DateTime64(3)),
    processed_ts        DateTime64(3),
    amount_original     String,
    currency_original   LowCardinality(String),
    amount_rub          String,
    exchange_rate       String,
    payer_id            String,
    payee_id            String,
    merchant_id         String,
    merchant_name       String,
    merchant_category   LowCardinality(String),
    card_token          String,
    is_current          UInt8 DEFAULT 0,
    field_hash          String DEFAULT '',
    processed_at        DateTime64(3) DEFAULT now64()
)
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(event_ts)
-- source_system в ключе предотвращает схлопывание версий платежей с одинаковым payment_id
-- из разных систем-источников.
ORDER BY (payment_id, source_system, version)
TTL toDate(event_ts) + INTERVAL 3 YEAR DELETE
SETTINGS index_granularity = 8192;

-- Bloom filter для быстрого поиска payment_id
ALTER TABLE payments.payment_history ADD INDEX IF NOT EXISTS bf_payment_id payment_id TYPE bloom_filter GRANULARITY 4;


-- -----------------------------------------------------------
-- Dead Letter Queue
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments.payment_dlq
(
    event_id        String,
    payload         String,          -- исходное JSON-тело
    error_code      LowCardinality(String),
    error_message   String,
    source_topic    LowCardinality(String),
    received_at     DateTime64(3) DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(received_at)
ORDER BY (received_at, event_id)
TTL toDate(received_at) + INTERVAL 30 DAY DELETE;


-- -----------------------------------------------------------
-- Курсы валют (справочник)
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments.exchange_rates
(
    currency    LowCardinality(String),
    rate_to_eur Float64,
    rate_to_rub Float64,
    valid_from  DateTime64(3),
    valid_to    DateTime64(3) DEFAULT '2099-12-31 00:00:00'
)
ENGINE = ReplacingMergeTree(valid_from)
ORDER BY (currency, valid_from)
SETTINGS index_granularity = 8192;

-- Начальные курсы (rate_to_rub: единиц RUB за 1 единицу валюты)
INSERT INTO payments.exchange_rates (currency, rate_to_eur, rate_to_rub, valid_from) VALUES ('EUR', 1.0, 99.0, now64()), ('USD', 0.92, 91.0, now64()), ('GBP', 1.17, 116.0, now64()), ('RUB', 0.0101, 1.0, now64()), ('CNY', 0.127, 12.5, now64()), ('TRY', 0.028, 2.8, now64());

-- -----------------------------------------------------------
-- Справочник мерчантов
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments.merchant_dict
(
    merchant_id         String,
    merchant_name       String,
    merchant_category   LowCardinality(String),
    valid_from          DateTime64(3) DEFAULT now64()
)
ENGINE = ReplacingMergeTree(valid_from)
ORDER BY (merchant_id)
SETTINGS index_granularity = 8192;

INSERT INTO payments.merchant_dict (merchant_id, merchant_name, merchant_category) VALUES ('MCH-001', 'Пятёрочка', 'GROCERY'), ('MCH-002', 'Перекрёсток', 'GROCERY'), ('MCH-003', 'Wildberries', 'ECOMMERCE'), ('MCH-004', 'Ozon', 'ECOMMERCE'), ('MCH-005', 'РЖД', 'TRANSPORT'), ('MCH-006', 'Аэрофлот', 'TRANSPORT'), ('MCH-007', 'McDonald''s', 'FOOD'), ('MCH-008', 'Burger King', 'FOOD'), ('MCH-009', 'Детский мир', 'RETAIL'), ('MCH-010', 'М.Видео', 'ELECTRONICS');
