# 1. Отчет по ДЗ Elastic

1. Посмотрите какие топики есть сейчас в системе, и на основе того, в котором вы видите максимальный объем данных создайте stream по имени WIKILANG который фильтрует правки только в разделах национальных языков, кроме английского (поле channel вида #ru.wikipedia), который сделали не боты. Stream должен содержать следующие поля: createdat, channel, username, wikipage, diffurl
```sql
CREATE STREAM WIKILANG_TEST
  WITH (kafka_topic='WIKILANG_TEST_TOPIC') AS
SELECT createdat, channel, username, wikipage, diffurl FROM WIKIPEDIA
WHERE WIKIPEDIA.channel = '#ru.wikipedia' AND WIKIPEDIA.ISBOT <> true;
```

2. После 1-2 минут работы откройте Confluent Control Center и сравните пропускную способность топиков WIKILANG и WIKIPEDIANOBOT, какие числа вы видите?
```
 Topic name          | Bytes/sec produced | Bytes/sec consumed 
-----------------------------------------------------------------
 WIKILANG_TEST_TOPIC | 61                 | 0   
 WIKIPEDIANOBOT      | 1981               | 1981   
-----------------------------------------------------------------
```

3. В KSQL CLI получите текущую статистику вашего стрима: describe extended WIKILANG_TEST;
```
ksql> describe extended WIKILANG_TEST;
Name                 : WIKILANG_TEST
Type                 : STREAM
Key field            : 
Key format           : STRING
Timestamp field      : Not set - using <ROWTIME>
Value format         : AVRO
Kafka topic          : WIKILANG_TEST_TOPIC (partitions: 2, replication: 2)
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
CSAS_WIKILANG_TEST_7 : CREATE STREAM WIKILANG_TEST WITH (KAFKA_TOPIC='WIKILANG_TEST_TOPIC', PARTITIONS=2, REPLICAS=
2) AS SELECT
  WIKIPEDIA.CREATEDAT "CREATEDAT",
  WIKIPEDIA.CHANNEL "CHANNEL",
  WIKIPEDIA.USERNAME "USERNAME",
  WIKIPEDIA.WIKIPAGE "WIKIPAGE",
  WIKIPEDIA.DIFFURL "DIFFURL"
FROM WIKIPEDIA WIKIPEDIA
WHERE ((WIKIPEDIA.CHANNEL = '#ru.wikipedia') AND (WIKIPEDIA.ISBOT <> true))
EMIT CHANGES;
For query topology and execution plan please run: EXPLAIN <QueryId>
Local runtime statistics
------------------------
messages-per-sec:      0.30   total-messages:       242     last-message: 2020-02-29T14:10:40.036Z
```

4. В KSQL CLI получите текущую статистику WIKIPEDIANOBOT: describe extended WIKIPEDIANOBOT;
```
describe extended WIKIPEDIANOBOT;
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
messages-per-sec:      5.35   total-messages:     62193     last-message: 2020-02-29T15:03:43.308Z
```
