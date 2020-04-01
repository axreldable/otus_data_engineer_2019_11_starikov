package ru.star

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object OutputAdapterJob extends App {
  println("input-adapter started.")

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val kafkaConsumerProperties = new Properties()
  kafkaConsumerProperties.put("bootstrap.servers", "localhost:9092")
  println("kafkaConsumerProperties", kafkaConsumerProperties)

  val eventConsumer = new FlinkKafkaConsumer[String](
    "output-adapter-in", new SimpleStringSchema(), kafkaConsumerProperties
  )

  val eventProducer = new FlinkKafkaProducer[String](
    "output-adapter-out", new SimpleStringSchema(), kafkaConsumerProperties
  )

  env
    .addSource(eventConsumer)
    .map(message => {
      println(s"Precessing '$message' in output-adapter.")
      message
    }).addSink(eventProducer)

  env.execute()
}
