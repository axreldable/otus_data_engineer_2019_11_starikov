#!/usr/bin/env bash

SPATH=$(dirname $0)
export SPARK_SUBMIT=/usr/local/Cellar/apache-spark/2.4.5/libexec/bin/spark-submit

PROJECT_NAME=hw-5
VERSION=1.0.0

${SPARK_SUBMIT} \
  --master local[*] \
  --class ru.star.BostonCrimesMap \
  ${SPATH}/target/scala-2.11/${PROJECT_NAME}_${VERSION}.jar \
  --crime-path ${SPATH}/src/main/resources/crime.csv \
  --offense-codes-path ${SPATH}/src/main/resources/offense_codes.csv \
  --result-output-folder ${SPATH}/src/main/resources/crime_result_calc
