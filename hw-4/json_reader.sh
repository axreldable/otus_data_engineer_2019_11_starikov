#!/usr/bin/env bash

SPATH=$(dirname $0)
export SPARK_SUBMIT=/usr/local/Cellar/apache-spark/2.4.0/libexec/bin/spark-submit

${SPARK_SUBMIT} \
  --master local[*] \
  --class ru.star.JsonReader \
${SPATH}/target/scala-2.11/hw-4_1.0.0.jar \
  ${SPATH}/src/main/resources/winemag-data-130k-v2.json
