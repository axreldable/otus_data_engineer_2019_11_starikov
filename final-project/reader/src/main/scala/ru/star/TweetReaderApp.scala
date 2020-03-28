package ru.star

import java.util.{Collections, Properties}

import com.typesafe.scalalogging._
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer, OffsetAndMetadata}

object TweetReaderApp extends App with StrictLogging {
  logger.info("Initializing FlowProducer, sleeping for 30 seconds to let Kafka startup")
  Thread.sleep(30000)

  val props = new Properties()

  props.put("bootstrap.servers", "kafka:9092")
  props.put("client.id", "producer")
  props.put("group.id", "producer")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("acks", "all")
  props.put("metadata.max.age.ms", "10000")
  props.put("auto.commit.interval.ms", "10000")
  props.put("consumer.timeout.ms", "500")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(Collections.singletonList("tweet-topic-1"))

  logger.info("Kafka consumer initialized")

  val giveUp = 100
  var noRecordsCount = 0

  while (true) {
    val consumerRecords: ConsumerRecords[String, String] = consumer.poll(1000)
    import java.util.Collections

    import scala.collection.JavaConversions._
    for (partition <- consumerRecords.partitions) {
      val partitionRecords = consumerRecords.records(partition)
      for (record <- partitionRecords) {
        println("Message:" + record.offset + ": " + record.value)
      }
      val lastOffset = partitionRecords.get(partitionRecords.size - 1).offset
      consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)))
    }
  }
  consumer.close()
  logger.info("DONE")
}
