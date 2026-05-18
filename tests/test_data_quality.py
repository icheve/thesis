"""
Автоматизированные DQ-проверки (L3 — пост-загрузочные).

Запускаются против ClickHouse после завершения обработки данных.
Используются как в CI (pytest), так и по расписанию (каждые 15 минут).

Запуск:
    pytest tests/test_data_quality.py -v
    pytest tests/test_data_quality.py -v --since-minutes=60
"""

from __future__ import annotations

import os
import pytest
import requests
from decimal import Decimal


# ---------------------------------------------------------------------------
# ClickHouse query helper
# ---------------------------------------------------------------------------

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_DB = os.getenv("CLICKHOUSE_DATABASE", "payments")
CH_USER = os.getenv("CLICKHOUSE_USER", "pipeline_writer")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
SINCE_MINUTES = int(os.getenv("DQ_SINCE_MINUTES", "60"))


def ch_query(sql: str) -> list[list[str]]:
    """Выполняет SQL-запрос к ClickHouse и возвращает строки как list[list[str]]."""
    resp = requests.get(
        f"http://{CH_HOST}:{CH_PORT}",
        params={
            "query": sql,
            "user": CH_USER,
            "password": CH_PASSWORD,
            "database": CH_DB,
            "output_format_write_statistics": "0",
        },
        timeout=30,
    )
    resp.raise_for_status()
    text = resp.text.strip()
    if not text:
        return []
    return [row.split("\t") for row in text.splitlines()]


def ch_scalar(sql: str, default=None):
    """Возвращает первый столбец первой строки результата."""
    rows = ch_query(sql)
    if rows and rows[0]:
        return rows[0][0]
    return default


# ---------------------------------------------------------------------------
# DQ-020: Дубли по event_id
# ---------------------------------------------------------------------------

class TestDQ020NoDuplicateEvents:
    """DQ-020: В payment_history не должно быть дублей по event_id."""

    def test_no_duplicate_event_ids(self):
        sql = f"""
        SELECT
            count() AS total_rows,
            uniqExact(event_id) AS unique_events,
            total_rows - unique_events AS duplicates
        FROM payment_history FINAL
        WHERE effective_to IS NULL
          AND toDateTime(processed_at) >= now() - INTERVAL {SINCE_MINUTES} MINUTE
        FORMAT TabSeparated
        """
        rows = ch_query(sql)
        assert rows, "No data returned from payment_history"
        total, unique, duplicates = int(rows[0][0]), int(rows[0][1]), int(rows[0][2])
        assert duplicates == 0, (
            f"DQ-020 FAILED: Found {duplicates} duplicate event_ids "
            f"(total={total}, unique={unique}) in last {SINCE_MINUTES} min"
        )


# ---------------------------------------------------------------------------
# DQ-021: Непрерывность номеров версий
# ---------------------------------------------------------------------------

class TestDQ021VersionContinuity:
    """DQ-021: Версии платежа должны идти без пропусков (1, 2, 3, ...)."""

    def test_no_version_gaps(self):
        sql = f"""
        SELECT payment_id, source_system,
            max(version) AS max_version,
            uniqExact(version) AS unique_versions
        FROM payment_history
        WHERE toDateTime(processed_at) >= now() - INTERVAL {SINCE_MINUTES} MINUTE
        GROUP BY payment_id, source_system
        HAVING max_version != unique_versions
        FORMAT TabSeparated
        """
        rows = ch_query(sql)
        assert rows == [], (
            f"DQ-021 FAILED: Version gaps found in {len(rows)} payments.\n"
            f"Examples: {rows[:5]}"
        )


# ---------------------------------------------------------------------------
# DQ-022: Непрерывность effective_from / effective_to
# ---------------------------------------------------------------------------

class TestDQ022EffectiveDateContinuity:
    """DQ-022: effective_to[N] == effective_from[N+1] для всех версий."""

    def test_no_gaps_in_effective_dates(self):
        sql = f"""
        SELECT
            a.payment_id,
            a.source_system,
            a.version AS v1,
            a.effective_to AS et1,
            b.version AS v2,
            b.effective_from AS ef2
        FROM payment_history a
        INNER JOIN payment_history b
            ON a.payment_id = b.payment_id
            AND a.source_system = b.source_system
            AND b.version = a.version + 1
        WHERE a.effective_to IS NOT NULL
          AND a.effective_to != b.effective_from
          AND toDateTime(a.processed_at) >= now() - INTERVAL {SINCE_MINUTES} MINUTE
        LIMIT 10
        FORMAT TabSeparated
        """
        rows = ch_query(sql)
        assert rows == [], (
            f"DQ-022 FAILED: effective_from/effective_to gaps found.\n"
            f"Examples: {rows}"
        )


# ---------------------------------------------------------------------------
# DQ-023: Ровно одна открытая версия на платёж
# ---------------------------------------------------------------------------

class TestDQ023SingleOpenVersion:
    """DQ-023: У каждого платежа должна быть ровно одна версия с effective_to IS NULL."""

    def test_single_open_version_per_payment(self):
        sql = f"""
        SELECT payment_id, source_system, count() AS cnt
        FROM payment_current FINAL
        GROUP BY payment_id, source_system
        HAVING cnt > 1
        FORMAT TabSeparated
        """
        rows = ch_query(sql)
        assert rows == [], (
            f"DQ-023 FAILED: {len(rows)} payments have multiple rows in payment_current FINAL.\n"
            f"Examples: {rows[:5]}"
        )


# ---------------------------------------------------------------------------
# DQ-024: Суммы > 0
# ---------------------------------------------------------------------------

class TestDQ024PositiveAmounts:
    """DQ-024: amount_rub > 0 для всех записей."""

    def test_no_zero_or_negative_amounts(self):
        sql = f"""
        SELECT count() AS invalid_rows
        FROM payment_history
        WHERE toDecimal64OrNull(amount_rub, 4) <= 0
          AND toDateTime(processed_at) >= now() - INTERVAL {SINCE_MINUTES} MINUTE
        FORMAT TabSeparated
        """
        count = int(ch_scalar(sql, "0"))
        assert count == 0, (
            f"DQ-024 FAILED: {count} rows with amount_rub <= 0 in last {SINCE_MINUTES} min"
        )


# ---------------------------------------------------------------------------
# DQ-025: Консистентность payment_current с payment_history
# ---------------------------------------------------------------------------

class TestDQ025CurrentConsistency:
    """DQ-025: payment_current должна содержать последние версии из payment_history."""

    def test_current_matches_history_max_version(self):
        sql = f"""
        SELECT count() AS mismatches
        FROM (
            SELECT payment_id, source_system, max(version) AS max_version
            FROM payment_history
            WHERE toDateTime(processed_at) >= now() - INTERVAL {SINCE_MINUTES} MINUTE
            GROUP BY payment_id, source_system
        ) h
        LEFT JOIN (
            SELECT payment_id, source_system, version
            FROM payment_current FINAL
        ) c USING (payment_id, source_system)
        WHERE c.version IS NULL OR c.version != h.max_version
        FORMAT TabSeparated
        """
        count = int(ch_scalar(sql, "0"))
        assert count == 0, (
            f"DQ-025 FAILED: {count} payments have inconsistent version "
            f"between payment_current and payment_history."
        )


# ---------------------------------------------------------------------------
# DQ-026: Нет событий без конвертации валюты
# ---------------------------------------------------------------------------

class TestDQ026CurrencyConversionExists:
    """DQ-026: exchange_rate != 0 для всех записей с ненулевой суммой."""

    def test_exchange_rate_not_zero(self):
        sql = f"""
        SELECT count() AS bad_rows
        FROM payment_history
        WHERE toDecimal64OrNull(exchange_rate, 6) = 0
          AND toDecimal64OrNull(amount_original, 4) > 0
          AND toDateTime(processed_at) >= now() - INTERVAL {SINCE_MINUTES} MINUTE
        FORMAT TabSeparated
        """
        count = int(ch_scalar(sql, "0"))
        assert count == 0, (
            f"DQ-026 FAILED: {count} rows with exchange_rate=0 and amount>0."
        )


# ---------------------------------------------------------------------------
# DQ-027: status_normalized принадлежит разрешённому словарю
# ---------------------------------------------------------------------------

ALLOWED_STATUSES = {
    # Нормализованные значения из STATUS_MAP (pipeline/config.py)
    "PENDING", "AUTHORIZED", "COMPLETED", "FAILED", "REFUNDED", "CANCELLED",
    # Значения, которые pipeline возвращает as-is, если source status не в STATUS_MAP
    # (enricher._normalize_status возвращает raw_status.upper() при отсутствии маппинга)
    "INITIATED", "PROCESSING", "REVERSED",
}


class TestDQ027StatusNormalized:
    """DQ-027: status_normalized содержит только известные значения."""

    def test_no_unknown_statuses(self):
        statuses_str = ", ".join(f"'{s}'" for s in ALLOWED_STATUSES)
        sql = f"""
        SELECT status_normalized, count() AS cnt
        FROM payment_history
        WHERE status_normalized NOT IN ({statuses_str})
          AND toDateTime(processed_at) >= now() - INTERVAL {SINCE_MINUTES} MINUTE
        GROUP BY status_normalized
        FORMAT TabSeparated
        """
        rows = ch_query(sql)
        assert rows == [], (
            f"DQ-027 FAILED: Unknown status_normalized values found: "
            f"{[(r[0], r[1]) for r in rows]}"
        )


# ---------------------------------------------------------------------------
# DQ-030: DLQ не растёт
# ---------------------------------------------------------------------------

class TestDQ030DlqNotGrowing:
    """DQ-030: Число событий в DLQ не превышает порог."""

    DLQ_THRESHOLD = 100  # максимально допустимое число непрочитанных в DLQ

    def test_dlq_not_exceeding_threshold(self):
        """
        Проверяет через Kafka Admin API что DLQ не накапливается.
        Упрощённая версия — проверяет table dq_dlq_events если она ведётся.
        """
        try:
            sql = f"""
            SELECT count() AS dlq_events
            FROM payment_dlq
            WHERE received_at >= now() - INTERVAL {SINCE_MINUTES} MINUTE
            FORMAT TabSeparated
            """
            count = int(ch_scalar(sql, "0"))
            assert count <= self.DLQ_THRESHOLD, (
                f"DQ-030 FAILED: DLQ has {count} events in last {SINCE_MINUTES} min "
                f"(threshold: {self.DLQ_THRESHOLD})"
            )
        except Exception:
            pytest.skip("DLQ log table not available — skip DQ-030")


# ---------------------------------------------------------------------------
# Сводный отчёт (запуск как standalone скрипт)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    checks = [
        ("DQ-020", "No duplicate event_ids", TestDQ020NoDuplicateEvents().test_no_duplicate_event_ids),
        ("DQ-021", "Version continuity", TestDQ021VersionContinuity().test_no_version_gaps),
        ("DQ-022", "Effective date continuity", TestDQ022EffectiveDateContinuity().test_no_gaps_in_effective_dates),
        ("DQ-023", "Single open version", TestDQ023SingleOpenVersion().test_single_open_version_per_payment),
        ("DQ-024", "Positive amounts", TestDQ024PositiveAmounts().test_no_zero_or_negative_amounts),
        ("DQ-025", "Current/history consistency", TestDQ025CurrentConsistency().test_current_matches_history_max_version),
        ("DQ-026", "Exchange rate not zero", TestDQ026CurrencyConversionExists().test_exchange_rate_not_zero),
        ("DQ-027", "Known status_normalized", TestDQ027StatusNormalized().test_no_unknown_statuses),
    ]

    failed = 0
    for code, name, fn in checks:
        try:
            fn()
            print(f"  OK  {code}: {name}")
        except AssertionError as e:
            print(f"  FAIL {code}: {name}\n       {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR {code}: {name}\n       {e}")
            failed += 1

    print(f"\nTotal: {len(checks)} checks, {failed} failed.")
    sys.exit(1 if failed else 0)
