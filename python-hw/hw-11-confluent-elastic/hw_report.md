# Отчет по ДЗ Elastic

## 1. Создайте KSQL Stream WIKILANG

1. Посмотрите какие топики есть сейчас в системе, и на основе того, 
в котором вы видите максимальный объем данных создайте stream по имени WIKILANG 
который фильтрует правки только в разделах национальных языков, 
кроме английского (поле channel вида #ru.wikipedia), 
который сделали не боты. Stream должен содержать следующие поля: createdat, channel, username, wikipage, diffurl
```sql
CREATE STREAM WIKILANG
  WITH (PARTITIONS=2,REPLICAS=2) AS
SELECT createdat, channel, username, wikipage, diffurl FROM wikipedia
WHERE channel <> '#en.wikipedia' AND isbot <> true;
```

## 2. Мониторинг WIKILANG
2. После 1-2 минут работы откройте Confluent Control Center и сравните пропускную способность топиков 
WIKILANG и WIKIPEDIANOBOT, какие числа вы видите?
```
 Topic name      | Bytes/sec produced | Bytes/sec consumed 
-----------------------------------------------------------
 WIKILANG        | 619                | 619   
 WIKIPEDIANOBOT  | 1199               | 1199   
-----------------------------------------------------------
```

3. В KSQL CLI получите текущую статистику вашего стрима: describe extended WIKILANG;
```
ksql> describe extended wikilang;
Name                 : WIKILANG
Type                 : STREAM
Key field            : 
Key format           : STRING
Timestamp field      : Not set - using <ROWTIME>
Value format         : AVRO
Kafka topic          : WIKILANG (partitions: 2, replication: 2)
 Field     | Type                      
---------------------------------------
 ROWTIME   | BIGINT           (system) 
 ROWKEY    | VARCHAR(STRING)  (system) 
 CREATEDAT | BIGINT                    
 CHANNEL   | VARCHAR(STRING)           
 USERNAME  | VARCHAR(STRING)           
 WIKIPAGE  | VARCHAR(STRING)           
 DIFFURL   | VARCHAR(STRING)           
---------------------------------------
Queries that write from this STREAM
-----------------------------------
CSAS_WIKILANG_7 : CREATE STREAM WIKILANG WITH (KAFKA_TOPIC='WIKILANG', PARTITIONS=2, REPLICAS=2) AS SELECT
  WIKIPEDIA.CREATEDAT "CREATEDAT",
  WIKIPEDIA.CHANNEL "CHANNEL",
  WIKIPEDIA.USERNAME "USERNAME",
  WIKIPEDIA.WIKIPAGE "WIKIPAGE",
  WIKIPEDIA.DIFFURL "DIFFURL"
FROM WIKIPEDIA WIKIPEDIA
WHERE ((WIKIPEDIA.CHANNEL <> '#en.wikipedia') AND (WIKIPEDIA.ISBOT <> true))
EMIT CHANGES;
For query topology and execution plan please run: EXPLAIN <QueryId>
Local runtime statistics
------------------------
messages-per-sec:      2.55   total-messages:      1151     last-message: 2020-03-09T13:20:14.073Z
(Statistics of the local KSQL server interaction with the Kafka topic WIKILANG)
```

4. В KSQL CLI получите текущую статистику WIKIPEDIANOBOT: describe extended WIKIPEDIANOBOT;
```
ksql> describe extended wikipedianobot;
Name                 : WIKIPEDIANOBOT
Type                 : STREAM
Key field            : 
Key format           : STRING
Timestamp field      : Not set - using <ROWTIME>
Value format         : AVRO
Kafka topic          : WIKIPEDIANOBOT (partitions: 2, replication: 2)
 Field         | Type                      
-------------------------------------------
 ROWTIME       | BIGINT           (system) 
 ROWKEY        | VARCHAR(STRING)  (system) 
 CREATEDAT     | BIGINT                    
 WIKIPAGE      | VARCHAR(STRING)           
 CHANNEL       | VARCHAR(STRING)           
 USERNAME      | VARCHAR(STRING)           
 COMMITMESSAGE | VARCHAR(STRING)           
 BYTECHANGE    | INTEGER                   
 DIFFURL       | VARCHAR(STRING)           
 ISNEW         | BOOLEAN                   
 ISMINOR       | BOOLEAN                   
 ISBOT         | BOOLEAN                   
 ISUNPATROLLED | BOOLEAN                   
-------------------------------------------
Queries that write from this STREAM
-----------------------------------
CSAS_WIKIPEDIANOBOT_2 : CREATE STREAM WIKIPEDIANOBOT WITH (KAFKA_TOPIC='WIKIPEDIANOBOT', PARTITIONS=2, REPLICAS=2) 
AS SELECT *
FROM WIKIPEDIA WIKIPEDIA
WHERE (WIKIPEDIA.ISBOT <> true)
EMIT CHANGES;
For query topology and execution plan please run: EXPLAIN <QueryId>
Local runtime statistics
------------------------
messages-per-sec:      4.54   total-messages:      4411     last-message: 2020-03-09T13:21:02.86Z
(Statistics of the local KSQL server interaction with the Kafka topic WIKIPEDIANOBOT)
```

## 3. Добавьте данные из стрима WIKILANG в ElasticSearch
5. Добавьте mapping - запустите скрипт set_elasticsearch_mapping_lang.sh
```
starikovaleksei1992@cdh-otus-hw-elastic:~/cp-demo$ sudo ./set_elasticsearch_mapping_lang.sh
./set_elasticsearch_mapping_lang.sh: line 77: warning: here-document at line 41 delimited by end-of-file (wanted `EOF')
{
  "acknowledged" : true,
  "shards_acknowledged" : true,
  "index" : "wikilang"
}
```

6. Добавьте Kafka Connect - запустите submit_elastic_sink_lang_config.sh
```
starikovaleksei1992@cdh-otus-hw-elastic:~/cp-demo$ sudo ./submit_elastic_sink_lang_config.sh
{"name":"elasticsearch-ksql-lang","config":{"connector.class":"io.confluent.connect.elasticsearch.ElasticsearchSinkConnector","consumer.interceptor.classes":"io.confluent.monitoring.cli
ents.interceptor.MonitoringConsumerInterceptor","topics":"WIKILANG","topic.index.map":"WIKILANG:wikilang","connection.url":"http://elasticsearch:9200","type.name":"wikichange_short","ke
y.ignore":"true","key.converter.schema.registry.url":"https://schemaregistry:8085","value.converter":"io.confluent.connect.avro.AvroConverter","value.converter.schema.registry.url":"htt
ps://schemaregistry:8085","value.converter.schema.registry.ssl.truststore.location":"/etc/kafka/secrets/kafka.client.truststore.jks","value.converter.schema.registry.ssl.truststore.pass
word":"confluent","value.converter.schema.registry.ssl.keystore.location":"/etc/kafka/secrets/kafka.client.keystore.jks","value.converter.schema.registry.ssl.keystore.password":"conflue
nt","schema.ignore":"true","name":"elasticsearch-ksql-lang"},"tasks":[],"type":"sink"}
```

7. Добавьте index-pattern - Kibana UI -> Management -> Index patterns -> Create Index Pattern -> Index name or pattern: wikilang -> кнопка Create
![create_index_wikilang](https://github.com/axreldable/otus_data_engineer_2019_11_starikov/blob/master/python-hw/hw-11-confluent-elastic/images/create_index_wikilang.png)

8. Используя полученные знания и документацию ответьте на вопросы:
a) Опишите что делает каждая из этих операций?
б) Зачем Elasticsearch нужен mapping чтобы принять данные?
в) Что дает index-pattern?

## 4. Создайте отчет "Топ10 национальных разделов" на базе индекса wikilang
9. Kibana UI -> Visualize -> + -> Data Table -> выберите индекс wikilang

10. Select bucket type -> Split Rows, Aggregation -> Terms, Field -> CHANNEL.keyword, Size -> 10, нажмите кнопку Apply changes (выглядит как кнопка Play)
![channels_count_visualizing](https://github.com/axreldable/otus_data_engineer_2019_11_starikov/blob/master/python-hw/hw-11-confluent-elastic/images/channels_count_visualizing.png)

12. Что вы увидели в отчете?

13. Нажав маленьку круглую кнопку со стрелкой вверх под отчетом, вы сможете запросить не только таблицу, но и запрос на Query DSL которым он получен.
Приложите тело запроса к заданию.
