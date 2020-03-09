# Отчет по ДЗ Elastic

## 1. Создайте KSQL Stream WIKILANG

1. Посмотрите какие топики есть сейчас в системе, и на основе того, 
в котором вы видите максимальный объем данных создайте stream по имени WIKILANG 
который фильтрует правки только в разделах национальных языков, 
кроме английского (поле channel вида #ru.wikipedia), 
который сделали не боты. Stream должен содержать следующие поля: createdat, channel, username, wikipage, diffurl
```sql
CREATE STREAM WIKILANG
  WITH (kafka_topic='WIKILANG') AS
SELECT createdat, channel, username, wikipage, diffurl FROM WIKIPEDIA
WHERE WIKIPEDIA.channel <> '#en.wikipedia' AND WIKIPEDIA.ISBOT <> true;
```
```sql
CREATE STREAM WIKILANG
  WITH (PARTITIONS=2,REPLICAS=2) AS
SELECT createdat, channel, username, wikipage, diffurl FROM wikipedia
WHERE channel <> '#en.wikipedia' AND isbot <> true;
```
```sql
CREATE STREAM wikipedianobot WITH (PARTITIONS=2,REPLICAS=2) AS SELECT * FROM wikipedia WHERE isbot <> true;
```
```sql
CREATE STREAM WIKILANG
  WITH (kafka_topic='WIKILANG') AS
SELECT * FROM WIKIPEDIA
WHERE WIKIPEDIA.channel <> '#en.wikipedia' AND WIKIPEDIA.ISBOT <> true;
```
```sql
CREATE STREAM WIKILANG
  (createdat BIGINT,
   channel VARCHAR,
   username VARCHAR,
   wikipage VARCHAR,
   diffurl VARCHAR)
  WITH (KAFKA_TOPIC='wikipedia.parsed',
        VALUE_FORMAT='DELIMITED');
```
```sql
CREATE STREAM WIKIPEDIANOBOT2 WITH (KAFKA_TOPIC='WIKIPEDIANOBOT2', PARTITIONS=2, REPLICAS=2) 
AS SELECT *
FROM WIKIPEDIA WIKIPEDIA
WHERE (WIKIPEDIA.ISBOT <> true)
EMIT CHANGES;
```
```sql
CREATE STREAM WIKILANG WITH (KAFKA_TOPIC='WIKILANG_TOPIC', PARTITIONS=2, REPLICAS=2) AS SELECT
  WIKIPEDIA.CREATEDAT "CREATEDAT",
  WIKIPEDIA.CHANNEL "CHANNEL",
  WIKIPEDIA.USERNAME "USERNAME",
  WIKIPEDIA.WIKIPAGE "WIKIPAGE",
  WIKIPEDIA.DIFFURL "DIFFURL"
FROM WIKIPEDIA WIKIPEDIA
WHERE ((WIKIPEDIA.CHANNEL = '#ru.wikipedia') AND (WIKIPEDIA.ISBOT <> true))
EMIT CHANGES;
```

CREATE STREAM pageviews
  (viewtime BIGINT,
   userid VARCHAR,
   pageid VARCHAR)
  WITH (KAFKA_TOPIC='pageviews',
        VALUE_FORMAT='DELIMITED')
  EMIT CHANGES;

## 2. Мониторинг WIKILANG
2. После 1-2 минут работы откройте Confluent Control Center и сравните пропускную способность топиков 
WIKILANG и WIKIPEDIANOBOT, какие числа вы видите?
```
 Topic name      | Bytes/sec produced | Bytes/sec consumed 
-----------------------------------------------------------
 WIKILANG        | 713                | 0   
 WIKIPEDIANOBOT  | 1444               | 1444   
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
messages-per-sec:      3.03   total-messages:      1807     last-message: 2020-03-05T18:32:25.752Z
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
messages-per-sec:      7.76   total-messages:      5354     last-message: 2020-03-05T18:33:27.337Z
(Statistics of the local KSQL server interaction with the Kafka topic WIKIPEDIANOBOT)
```

## 3. Добавьте данные из стрима WIKILANG в ElasticSearch
5. Добавьте mapping - запустите скрипт set_elasticsearch_mapping_lang.sh
```
starikovaleksei1992@cdh-otus-hw-elastic:~/otus_data_engineer_2019_11_starikov/python-hw/hw-11-elastic-search$ sudo 
./set_elasticsearch_mapping_lang.sh 
./set_elasticsearch_mapping_lang.sh: line 59: warning: here-document at line 32 delimited by end-of-file (wanted `E
OF')
{
  "acknowledged" : true,
  "shards_acknowledged" : true,
  "index" : "wikilang"
}
```

6. Добавьте Kafka Connect - запустите submit_elastic_sink_lang_config.sh
```
starikovaleksei1992@cdh-otus-hw-elastic:~/cp-demo$ sudo ./submit_elastic_sink_lang_config.sh 
{"name":"elasticsearch-ksql-lang","config":{"connector.class":"io.confluent.connect.elasticsearch.ElasticsearchSink
Connector","consumer.interceptor.classes":"io.confluent.monitoring.clients.interceptor.MonitoringConsumerIntercepto
r","topics":"WIKILANG","topic.index.map":"WIKILANG:wikilang","connection.url":"http://elasticsearch:9200","type.nam
e":"wikichange_short","key.ignore":"true","key.converter.schema.registry.url":"https://schemaregistry:8085","value.
converter":"io.confluent.connect.avro.AvroConverter","value.converter.schema.registry.url":"https://schemaregistry:
8085","schema.ignore":"true","name":"elasticsearch-ksql-lang"},"tasks":[],"type":"sink"}
```

7. Добавьте index-pattern - Kibana UI -> Management -> Index patterns -> Create Index Pattern -> Index name or pattern: wikilang -> кнопка Create
[Скриншот создания]

8. Используя полученные знания и документацию ответьте на вопросы:
a) Опишите что делает каждая из этих операций?
б) Зачем Elasticsearch нужен mapping чтобы принять данные?
в) Что дает index-pattern?

## 4. Создайте отчет "Топ10 национальных разделов" на базе индекса wikilang
9. Kibana UI -> Visualize -> + -> Data Table -> выберите индекс wikilang

10. Select bucket type -> Split Rows, Aggregation -> Terms, Field -> CHANNEL.keyword, Size -> 10, нажмите кнопку Apply changes (выглядит как кнопка Play)

11. Сохраните визуализацию под удобным для вас именем

12. Что вы увидели в отчете?

13. Нажав маленьку круглую кнопку со стрелкой вверх под отчетом, вы сможете запросить не только таблицу, но и запрос на Query DSL которым он получен.
Приложите тело запроса к заданию.
