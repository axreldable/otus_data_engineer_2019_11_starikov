#!/usr/bin/env bash

set -eu

SPATH=$(dirname $0)

SPARK_SUBMIT=spark-submit

${SPARK_SUBMIT} \
    --class "ru.star.SparkTweetJob" \
	--master "local[2]" \
	--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 \
	${SPATH}/target/scala-2.11/spark-tweet-job_1.0.0.jar