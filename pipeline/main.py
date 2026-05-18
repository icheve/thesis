"""
NRT Payment Pipeline — точка входа Flink job.

Запуск (dev, локально):
    python pipeline/main.py

Запуск в Flink cluster:
    flink run -py pipeline/main.py -pyfs pipeline/ -D parallelism.default=4
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import timedelta

from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import MapFunction

from pipeline.config import FlinkConfig, KafkaConfig
from pipeline.models.payment_event import PaymentEvent, DlqMessage
from pipeline.models.payment_processed import PaymentHistoryRow, PaymentProcessed
from pipeline.operators.enricher import EnricherOperator
from pipeline.operators.scd_merger import ScdMergerOperator
from pipeline.operators.validator import ValidationResult, ValidatorOperator
from pipeline.sinks.clickhouse_sink import ClickHouseSink

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","service":"flink-payment-pipeline","message":"%(message)s"}',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Flink function adapters
# ---------------------------------------------------------------------------

class PaymentEventDeserializer(MapFunction):
    """Десериализует JSON-строку из Kafka в PaymentEvent."""

    def map(self, value: str) -> PaymentEvent:
        return PaymentEvent.from_json(value)


class EnricherMapFunction(MapFunction):
    """Адаптер EnricherOperator для Flink MapFunction."""

    def __init__(self):
        self._enricher: EnricherOperator | None = None

    def open(self, runtime_context) -> None:
        self._enricher = EnricherOperator()

    def map(self, event: PaymentEvent) -> PaymentProcessed:
        return self._enricher.enrich(event)


class ClickHouseRichSink(MapFunction):
    """Адаптер ClickHouseSink для Flink MapFunction (write-through)."""

    def __init__(self):
        self._sink: ClickHouseSink | None = None
        self._insert_errors = None

    def open(self, runtime_context) -> None:
        metrics = runtime_context.get_metrics_group()
        self._insert_errors = metrics.counter("clickhouse_insert_errors_total")
        self._sink = ClickHouseSink()
        self._sink.open()

    def map(self, row: PaymentHistoryRow) -> PaymentHistoryRow:
        try:
            self._sink.invoke(row)
        except RuntimeError:
            if self._insert_errors:
                self._insert_errors.inc()
            raise
        return row

    def close(self) -> None:
        if self._sink:
            self._sink.close()


class DlqSerialiser(MapFunction):
    """Сериализует DlqMessage в JSON-строку."""

    def map(self, msg: DlqMessage) -> str:
        return msg.to_json()


class ProcessedSerialiser(MapFunction):
    """Сериализует PaymentProcessed в JSON-строку."""

    def map(self, row: PaymentHistoryRow) -> str:
        # Для Kafka.processed эмитируем компактное представление
        import json
        return json.dumps({
            "payment_id": row.payment_id,
            "source_system": row.source_system,
            "version": row.version,
            "event_type": row.event_type,
            "status_normalized": row.status_normalized,
            "event_ts": row.event_ts,
            "effective_from": row.effective_from,
            "effective_to": row.effective_to,
            "amount_rub": row.amount_rub,
            "merchant_category": row.merchant_category,
        }, ensure_ascii=False)


class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        if isinstance(value, PaymentEvent):
            return value.event_ts
        return record_timestamp


# ---------------------------------------------------------------------------
# Построение и запуск Flink job
# ---------------------------------------------------------------------------

def build_job(env: StreamExecutionEnvironment, parallelism: int, start_offset: str = "latest"):
    """Строит топологию Flink DataStream."""

    env.set_parallelism(parallelism)

    # --- Checkpoint ---
    env.enable_checkpointing(FlinkConfig.CHECKPOINT_INTERVAL_MS, CheckpointingMode.AT_LEAST_ONCE)
    env.get_checkpoint_config().set_checkpoint_timeout(FlinkConfig.CHECKPOINT_TIMEOUT_MS)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    # --- Source: Kafka payments.raw ---
    offsets = (
        KafkaOffsetsInitializer.earliest()
        if start_offset == "earliest"
        else KafkaOffsetsInitializer.latest()
    )
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KafkaConfig.BOOTSTRAP_SERVERS)
        .set_topics(KafkaConfig.INPUT_TOPIC)
        .set_group_id(KafkaConfig.CONSUMER_GROUP)
        .set_starting_offsets(offsets)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # --- Watermark Strategy: BoundedOutOfOrderness по event_ts ---
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_minutes(FlinkConfig.WATERMARK_DELAY_MINUTES))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    raw_stream = (
        env
        .from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka payments.raw")
        .map(PaymentEventDeserializer(), output_type=Types.PICKLED_BYTE_ARRAY())
        .assign_timestamps_and_watermarks(watermark_strategy)
        .name("Deserialize + Watermark")
    )

    # --- Validation ---
    # ValidatorOperator returns ValidationResult (is_valid, event/dlq_msg)
    # Split via filter to avoid side outputs (not supported in PyFlink 1.19 Beam runtime)
    validation_results = (
        raw_stream
        .map(ValidatorOperator(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("Validate")
    )

    # DLQ branch → Kafka payments.dlq
    (
        validation_results
        .filter(lambda r: not r.is_valid)
        .map(lambda r: r.dlq_msg)
        .map(DlqSerialiser(), output_type=Types.STRING())
        .sink_to(
            KafkaSink.builder()
            .set_bootstrap_servers(KafkaConfig.BOOTSTRAP_SERVERS)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(KafkaConfig.DLQ_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )
        .name("Sink: payments.dlq")
    )

    # Valid events branch
    validated = (
        validation_results
        .filter(lambda r: r.is_valid)
        .map(lambda r: r.event, output_type=Types.PICKLED_BYTE_ARRAY())
    )

    # --- Enrichment ---
    enriched = (
        validated
        .map(EnricherMapFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("Enrich")
    )

    # --- SCD Merge (stateful, keyed по payment_id+source_system) ---
    merged = (
        enriched
        .key_by(lambda e: f"{e.payment_id}|{e.source_system}")
        .process(ScdMergerOperator())
        .name("SCD Merge")
    )

    # --- Sink: ClickHouse (payment_history + payment_current) ---
    merged.map(ClickHouseRichSink()).name("Sink: ClickHouse")

    # --- Sink: Kafka payments.processed (для downstream real-time consumers) ---
    (
        merged
        .map(ProcessedSerialiser(), output_type=Types.STRING())
        .sink_to(
            KafkaSink.builder()
            .set_bootstrap_servers(KafkaConfig.BOOTSTRAP_SERVERS)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(KafkaConfig.OUTPUT_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )
        .name("Sink: payments.processed")
    )


def main():
    parser = argparse.ArgumentParser(description="NRT Payment Pipeline — Flink Job")
    parser.add_argument("--parallelism", type=int, default=FlinkConfig.PARALLELISM)
    parser.add_argument("--profile", choices=["dev", "prod"], default="dev")
    parser.add_argument(
        "--start-offset",
        choices=["latest", "earliest"],
        default="latest",
        help="Kafka starting offset: 'latest' (normal run) or 'earliest' (full replay from Kafka retention).",
    )
    args = parser.parse_args()

    logger.info("Starting NRT Payment Pipeline job", extra={
        "parallelism": args.parallelism,
        "kafka_bootstrap": KafkaConfig.BOOTSTRAP_SERVERS,
        "input_topic": KafkaConfig.INPUT_TOPIC,
        "start_offset": args.start_offset,
    })

    env = StreamExecutionEnvironment.get_execution_environment()

    if args.profile == "dev":
        # В dev-режиме: локальное выполнение, меньший parallelism
        env.set_parallelism(min(args.parallelism, 2))
    else:
        env.set_parallelism(args.parallelism)

    build_job(env, args.parallelism, args.start_offset)

    logger.info("Executing Flink job: %s", FlinkConfig.JOB_NAME)
    env.execute(FlinkConfig.JOB_NAME)


if __name__ == "__main__":
    main()
