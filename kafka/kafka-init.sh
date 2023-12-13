#!/bin/bash
# Kafka topic creation commands
kafka-topics.sh --create --topic views --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2 || true
kafka-topics.sh --create --topic purchases --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2 || true

# Call the original entrypoint script to start Kafka
exec start-kafka.sh