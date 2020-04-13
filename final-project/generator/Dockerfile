FROM openjdk:8

ARG DATA_DIR=/tmp/data

RUN mkdir -p ${DATA_DIR} && \
    wget http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip -O ${DATA_DIR}/sentiment.zip && \
    cd ${DATA_DIR} && unzip sentiment.zip

COPY ./target /usr/bin/app
WORKDIR /usr/bin/app
ENTRYPOINT java -jar scala-2.11/generator_1.0.0.jar --bootstrap-servers kafka:9093