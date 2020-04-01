#!/usr/bin/env bash

set -eu

SPATH=$(dirname $0)

VERSION=1.0.0

flink run ${SPATH}/target/scala-2.11/output-adapter_${VERSION}.jar
