#!/usr/bin/env bash

SPATH=$(dirname $0)

DATA_DIR=/tmp/data

mkdir -p ${DATA_DIR} && \
wget http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip -O ${DATA_DIR}/sentiment.zip && \
cd ${DATA_DIR} && unzip sentiment.zip
