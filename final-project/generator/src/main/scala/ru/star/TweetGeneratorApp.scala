package ru.star

import java.util.Properties

import com.typesafe.scalalogging._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object TweetGeneratorApp extends App with StrictLogging {
  logger.info("Initializing FlowProducer, sleeping for 30 seconds to let Kafka startup")
  Thread.sleep(30000)

  val props = new Properties()

  props.put("bootstrap.servers", "kafka:9092")
  props.put("client.id", "producer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")
  props.put("metadata.max.age.ms", "10000")

  val producer = new KafkaProducer[String, String](props)
  producer.flush()

  logger.info("Kafka producer initialized")

  var msgCounter = 0

  val bufferedSource = Source.fromFile("/tmp/data/training.1600000.processed.noemoticon.csv")
  for (line <- bufferedSource.getLines) {
    Thread.sleep(1000)
    msgCounter += 1

    val data = new ProducerRecord[String, String]("tweet-topic-1", line)

    producer.send(data)

    if (msgCounter % 1 == 0) {
      logger.info(s"New messages came, total: $msgCounter messages")
      logger.info(s"$line")
    }
  }

  bufferedSource.close
}
