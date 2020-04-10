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
flink run spark-tweet-job/target/scala-2.11/spark-tweet-job_1.0.0.jar & \
flink run output-adapter/target/scala-2.11/output-adapter_1.0.0.jar

flink run -c io.radicalbit.examples.QuickEvaluateKmeans \
flink-jpmml-examples/target/scala-2.11/flink-jpmml-examples-assembly-0.7.0-SNAPSHOT.jar \
--model /Users/axreldable/Desktop/projects/otus/data-engineer/flink-jpmml/flink-jpmml-assets/src/main/resources/kmeans.xml \
--output ./result
