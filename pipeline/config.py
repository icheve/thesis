"""Конфигурация NRT Payment Pipeline."""

import os


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _int_env(key: str, default: int) -> int:
    return int(os.environ.get(key, default))


class KafkaConfig:
    BOOTSTRAP_SERVERS: str = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    INPUT_TOPIC: str = _env("KAFKA_INPUT_TOPIC", "payments.raw")
    OUTPUT_TOPIC: str = _env("KAFKA_OUTPUT_TOPIC", "payments.processed")
    DLQ_TOPIC: str = _env("KAFKA_DLQ_TOPIC", "payments.dlq")
    CONSUMER_GROUP: str = _env("KAFKA_CONSUMER_GROUP", "payment-pipeline-flink")


class ClickHouseConfig:
    HOST: str = _env("CLICKHOUSE_HOST", "localhost")
    PORT: int = _int_env("CLICKHOUSE_PORT", 8123)
    DATABASE: str = _env("CLICKHOUSE_DATABASE", "payments")
    USER: str = _env("CLICKHOUSE_USER", "pipeline_writer")
    PASSWORD: str = _env("CLICKHOUSE_PASSWORD", "")
    BATCH_SIZE: int = _int_env("CLICKHOUSE_BATCH_SIZE", 500)
    FLUSH_INTERVAL_MS: int = _int_env("CLICKHOUSE_FLUSH_INTERVAL_MS", 5_000)
    MAX_RETRIES: int = _int_env("CLICKHOUSE_MAX_RETRIES", 5)
    RETRY_BASE_DELAY_MS: int = _int_env("CLICKHOUSE_RETRY_BASE_DELAY_MS", 200)

    @classmethod
    def base_url(cls) -> str:
        return f"http://{cls.HOST}:{cls.PORT}"


class FlinkConfig:
    PARALLELISM: int = _int_env("FLINK_PARALLELISM", 4)
    CHECKPOINT_INTERVAL_MS: int = _int_env("FLINK_CHECKPOINT_INTERVAL_MS", 30_000)
    CHECKPOINT_TIMEOUT_MS: int = _int_env("FLINK_CHECKPOINT_TIMEOUT_MS", 60_000)
    WATERMARK_DELAY_MINUTES: int = _int_env("FLINK_WATERMARK_DELAY_MIN", 10)
    STATE_TTL_HOURS: int = _int_env("FLINK_STATE_TTL_HOURS", 24)
    JOB_NAME: str = _env("FLINK_JOB_NAME", "NRT-Payment-Pipeline")


# Разрешённые ISO 4217 коды валют
ALLOWED_CURRENCIES: frozenset[str] = frozenset({
    "RUB", "USD", "EUR", "CNY", "GBP", "CHF", "JPY", "AED", "TRY", "KZT",
})

# Зарегистрированные системы-источники
ALLOWED_SOURCE_SYSTEMS: frozenset[str] = frozenset({
    "ACQUIRING", "MOBILE_APP", "INTERNET_BANKING", "BATCH_TRANSFER",
})

# Допустимые типы событий
ALLOWED_EVENT_TYPES: frozenset[str] = frozenset({
    "CREATED", "AUTHORIZED", "CLEARED", "REFUNDED", "CANCELLED", "FAILED", "PRE_AUTH",
})

# Маппинг статусов source_system → internal status
STATUS_MAP: dict[str, dict[str, str]] = {
    "ACQUIRING": {
        "NEW": "PENDING",
        "AUTH_OK": "AUTHORIZED",
        "SETTLED": "COMPLETED",
        "DECLINED": "FAILED",
        "REVERSED": "REFUNDED",
        "VOID": "CANCELLED",
    },
    "MOBILE_APP": {
        "processing": "PENDING",
        "authorized": "AUTHORIZED",
        "done": "COMPLETED",
        "failed": "FAILED",
        "refund": "REFUNDED",
        "cancelled": "CANCELLED",
    },
    "INTERNET_BANKING": {
        "INIT": "PENDING",
        "CONFIRMED": "AUTHORIZED",
        "EXECUTED": "COMPLETED",
        "ERROR": "FAILED",
        "REVERSED": "REFUNDED",
        "CANCELED": "CANCELLED",
    },
    "BATCH_TRANSFER": {
        "QUEUED": "PENDING",
        "PROCESSED": "COMPLETED",
        "REJECTED": "FAILED",
    },
}
