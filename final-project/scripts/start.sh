#!/usr/bin/env bash

echo "Stopping previous running..."
./scripts/stop.sh

echo "Starting Kafka and monitoring..."
docker-compose up -d zookeeper zookeeper kafka kafka-jmx-exporter prometheus grafana graf-db

echo "Starting Flink Cluster..."
~/Desktop/programs/flink/flink-1.10.0/bin/start-cluster.sh

echo "Deploying Flink jobs..."
flink run input-adapter/target/scala-2.11/input-adapter_1.0.0.jar & \
flink run pmml-job/target/scala-2.11/pmml-job_1.0.0.jar & \
flink run output-adapter/target/scala-2.11/output-adapter_1.0.0.jar &

echo "Starting spark job..."
#docker-compose build spark-tweet-job
docker-compose up -d spark-tweet-job

echo "Starting generator..."
#docker-compose build generator
docker-compose up -d generator
