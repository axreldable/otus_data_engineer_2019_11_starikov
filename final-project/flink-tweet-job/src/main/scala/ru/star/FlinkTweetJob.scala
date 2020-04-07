package ru.star

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object FlinkTweetJob extends App {
  println("flink-tweet-job started.")

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val params = FlinkTweetParams(args)
  println("params", params)

  val messageConsumer = new FlinkKafkaConsumer[String](
    params.inputTopic, new SimpleStringSchema(), params.kafkaConsumerProperties
  )

  val eventProducer = new FlinkKafkaProducer[String](
    params.outputTopic, new SimpleStringSchema(), params.kafkaProducerProperties
  )

  env
    .addSource(messageConsumer)
    .map(message => {
      println(s"Precessing '$message' in flink-tweet-job.")
      message
    })
    .map(message => s"type-1,$message,0")
    .addSink(eventProducer)

  env.execute("flink-tweet-job")
}
