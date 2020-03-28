package ru.star

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object InputAdapterJob extends App {
  println("input-adapter started.")

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val params: Parameters = Parameters(args)
  println("params", params)

  val eventConsumer = new FlinkKafkaConsumer[String](
    "input-event-in", new SimpleStringSchema(), params.kafkaConsumerProperties
  )
  val eventProducer = new FlinkKafkaProducer[String](
    "input-event-out", new SimpleStringSchema(), params.kafkaConsumerProperties
  )

  InputAdapterBuilder(
    env = env,
    eventSource = eventConsumer,
    eventSink = eventProducer
  ).build()

  env.execute()
}
