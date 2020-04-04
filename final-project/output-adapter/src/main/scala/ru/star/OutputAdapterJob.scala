package ru.star

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object OutputAdapterJob extends App {
  println("output-adapter started.")

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val kafkaConsumerProperties = new Properties()
  kafkaConsumerProperties.put("bootstrap.servers", "kafka:9093")
  println("kafkaConsumerProperties", kafkaConsumerProperties)

  val messageConsumer = new FlinkKafkaConsumer[String](
    "output-adapter-message-in", new SimpleStringSchema(), kafkaConsumerProperties
  )

  val eventConsumer = new FlinkKafkaConsumer[String](
    "output-adapter-message-event-in", new SimpleStringSchema(), kafkaConsumerProperties
  )

  val configConsumer = new FlinkKafkaConsumer[String](
    "output-adapter-message-config-in", new SimpleStringSchema(), kafkaConsumerProperties
  )

  val eventProducer = new FlinkKafkaProducer[String](
    "output-adapter-out", new SimpleStringSchema(), kafkaConsumerProperties
  )

  env
    .addSource(messageConsumer)
    .map(message => {
      println(s"Precessing '$message' in output-adapter.")
      message
    }).addSink(eventProducer)

  env.execute("output-adapter")
}
