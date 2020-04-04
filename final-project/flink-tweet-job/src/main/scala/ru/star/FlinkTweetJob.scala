package ru.star

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object FlinkTweetJob extends App {
  println("flink-tweet-job started.")

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val kafkaConsumerProperties = new Properties()
  kafkaConsumerProperties.put("bootstrap.servers", "kafka:9093")
  println("kafkaConsumerProperties", kafkaConsumerProperties)

  val eventConsumer = new FlinkKafkaConsumer[String](
    "flink-ml-in", new SimpleStringSchema(), kafkaConsumerProperties
  )

  val eventProducer = new FlinkKafkaProducer[String](
    "output-adapter-message-in", new SimpleStringSchema(), kafkaConsumerProperties
  )

  env
    .addSource(eventConsumer)
    .map(message => {
      println(s"Precessing '$message' in flink-tweet-job.")
      message
    })
    .map(message => s"type-1,$message,0")
    .addSink(eventProducer)

  env.execute("flink-tweet-job")
}
