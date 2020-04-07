package ru.star

import java.util.Properties

import com.typesafe.scalalogging._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object TweetGeneratorApp extends App with StrictLogging {
  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
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
    val tweet = line.split(",").last
    val message1 = s"type-1,$tweet"
    val message2 = s"type-2,$tweet"
    val message3 = s"type-3,$tweet"
    println(message1)
    Thread.sleep(1000)

    val targetTopic = "ml-stream-input-adapter-message-in"
    producer.send(new ProducerRecord[String, String](targetTopic, message1))
    producer.send(new ProducerRecord[String, String](targetTopic, message2))
    producer.send(new ProducerRecord[String, String](targetTopic, message3))

    msgCounter += 1
    if (msgCounter % 1 == 0) {
      logger.info(s"New messages came, total: $msgCounter messages")
    }
  }

  bufferedSource.close
}
