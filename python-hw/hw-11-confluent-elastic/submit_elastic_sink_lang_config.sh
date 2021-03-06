#!/bin/bash

HEADER="Content-Type: application/json"
DATA=$( cat << EOF
{
  "name": "elasticsearch-ksql-lang",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "consumer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor",
    "topics": "WIKILANG",
    "topic.index.map": "WIKILANG:wikilang",
    "connection.url": "http://elasticsearch:9200",
    "type.name": "wikichange_short",
    "key.ignore": true,
    "key.converter.schema.registry.url": "https://schemaregistry:8085",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "https://schemaregistry:8085",
    "value.converter.schema.registry.ssl.truststore.location": "/etc/kafka/secrets/kafka.client.truststore.jks",
    "value.converter.schema.registry.ssl.truststore.password": "confluent",
    "value.converter.schema.registry.ssl.keystore.location": "/etc/kafka/secrets/kafka.client.keystore.jks",
    "value.converter.schema.registry.ssl.keystore.password": "confluent",
    "schema.ignore": true
  }
}
EOF
)

docker-compose exec connect curl -X POST -H "${HEADER}" --data "${DATA}" --cert /etc/kafka/secrets/connect.certificate.pem --key /etc/kafka/secrets/connect.key --tlsv1.2 --cacert /etc/kafka/secrets/snakeoil-ca-1.crt https://connect:8083/connectors
