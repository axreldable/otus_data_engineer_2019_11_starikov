#!/usr/bin/env bash

# start Kafka
rm -r /usr/local/var/lib/kafka-logs/*
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
kafka-server-start /usr/local/etc/kafka/server.properties
kafka-topics --zookeeper localhost:2181 --delete --topic 'ml-stream-.*'

# start Flink cluster
~/Desktop/programs/flink/flink-1.10.0/bin/start-cluster.sh

# stop Flink cluster
~/Desktop/programs/flink/flink-1.10.0/bin/stop-cluster.sh

# build
sbt assembly

# run tweet-generator
./generator/download_data.sh
java -jar generator/target/scala-2.11/generator_1.0.0.jar

# run flink jobs
flink run input-adapter/target/scala-2.11/input-adapter_1.0.0.jar & \
flink run flink-tweet-job/target/scala-2.11/flink-tweet-job_1.0.0.jar & \
flink run output-adapter/target/scala-2.11/output-adapter_1.0.0.jar
