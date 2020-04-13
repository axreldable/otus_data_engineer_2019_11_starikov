#!/usr/bin/env bash

echo "Stopping Flink Cluster..."
~/Desktop/programs/flink/flink-1.10.0/bin/stop-cluster.sh

echo "Stopping docker..."
docker-compose down --volumes
