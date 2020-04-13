#!/usr/bin/env bash

# Start Kafka
echo "Starting Kafka and monitoring..."
docker-compose up -d zookeeper zookeeper kafka kafka-jmx-exporter prometheus grafana graf-db