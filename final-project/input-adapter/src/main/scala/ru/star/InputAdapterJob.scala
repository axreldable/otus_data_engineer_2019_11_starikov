package ru.star

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object InputAdapterJob extends App {
  println("input-adapter started.")

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val params: Parameters = Parameters(args)
  println("params", params)

  val kafkaConsumerProperties = new Properties()
  kafkaConsumerProperties.put("bootstrap.servers", "kafka:9093")
  println("kafkaConsumerProperties", kafkaConsumerProperties)

  val eventConsumer = new FlinkKafkaConsumer[String](
    "input-adapter-in", new SimpleStringSchema(), kafkaConsumerProperties
  )

  val eventProducer = new FlinkKafkaProducer[String](
    "input-adapter-out", new SimpleStringSchema(), kafkaConsumerProperties
  )

  env
    .addSource(eventConsumer)
    .map(message => {
      println(s"Precessing '$message' in input-adapter.")
      message
    }).addSink(eventProducer)

  env.execute("input-adapter")
}
