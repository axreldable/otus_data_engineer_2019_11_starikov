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
  kafkaConsumerProperties.put("bootstrap.servers", "localhost:9092")
  println("kafkaConsumerProperties", kafkaConsumerProperties)

  val eventConsumer = new FlinkKafkaConsumer[String](
    "tweet-topic-1", new SimpleStringSchema(), kafkaConsumerProperties
  )

  val eventProducer = new FlinkKafkaProducer[String](
    "input-event-out", new SimpleStringSchema(), kafkaConsumerProperties
  )
  //
  //  InputAdapterBuilder(
  //    env = env,
  //    eventSource = eventConsumer,
  //    eventSink = eventProducer
  //  ).build()

  //  case class WordWithCount(word: String, count: Long)
  //
  //  val text = env.readTextFile("/Users/axreldable/Desktop/projects/otus/data-engineer/otus_data_engineer_2019_11_starikov/final-project/input-adapter/src/main/scala/ru/star/InputAdapterJob.scala")
  //
  //  text.flatMap { w => w.split("\\s") }
  //    .map { w => println("!" + w) }

  env
    .addSource(eventConsumer)
    .map(message => {
      println(s"Precessing '$message' in input-adapter.")
      message
    }).addSink(eventProducer)

  env.execute()
}
