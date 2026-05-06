#!/bin/bash
# Создаёт пользователя pipeline_writer с паролем из переменной окружения.
# Запускается автоматически при первом старте ClickHouse-контейнера
# (docker-entrypoint-initdb.d обрабатывает *.sh скрипты через bash).
#
# Переменная CLICKHOUSE_PIPELINE_PASSWORD передаётся из docker-compose
# через секцию environment и берётся из .env / Docker secrets в production.

set -eu

: "${CLICKHOUSE_PIPELINE_PASSWORD:?CLICKHOUSE_PIPELINE_PASSWORD is required}"

clickhouse-client --multiquery <<SQL
CREATE DATABASE IF NOT EXISTS payments;
CREATE USER IF NOT EXISTS pipeline_writer
    IDENTIFIED WITH plaintext_password BY '${CLICKHOUSE_PIPELINE_PASSWORD}';
GRANT INSERT, SELECT ON payments.* TO pipeline_writer;
SQL
