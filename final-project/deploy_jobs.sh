#!/usr/bin/env bash

set -eu

./input-adapter/deploy.sh & \
./flink-tweet-job/deploy.sh & \
./output-adapter/deploy.sh &
