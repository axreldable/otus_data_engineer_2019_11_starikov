#!/usr/bin/env bash

./input-adapter/deploy.sh & \
./flink-tweet-job/deploy.sh & \
./output-adapter/deploy.sh &
