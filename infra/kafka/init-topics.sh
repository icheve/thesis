#!/bin/bash
set -e
echo "Creating Kafka topics..."
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic payments.raw --partitions 12 --replication-factor 1 --config retention.ms=259200000
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic payments.processed --partitions 6 --replication-factor 1 --config retention.ms=86400000
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic payments.dlq --partitions 2 --replication-factor 1 --config retention.ms=2592000000
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic payments.late --partitions 2 --replication-factor 1 --config retention.ms=2592000000
echo "Done."
