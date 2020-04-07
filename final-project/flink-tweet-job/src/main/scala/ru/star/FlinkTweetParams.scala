package ru.star

import java.util.Properties

import pureconfig._
//import pureconfig.generic.auto._
import pureconfig.generic.auto._

final case class InputKafka(bootstrapServers: String, inputTopic: String)

final case class OutputKafka(bootstrapServers: String, outputTopic: String)

final case class FlinkTweetParameters(inputKafka: InputKafka, outputKafka: OutputKafka)

final case class FlinkTweetParams(kafkaConsumerProperties: Properties,
                                  kafkaProducerProperties: Properties,
                                  inputTopic: String, outputTopic: String)

object FlinkTweetParams {
  def apply(inputArgs: Array[String]): FlinkTweetParams = {
    val config = ConfigSource.default.loadOrThrow[FlinkTweetParameters]

    val kafkaConsumerProperties = new Properties()
    kafkaConsumerProperties.put("bootstrap.servers", config.inputKafka.bootstrapServers)

    val kafkaProducerProperties = new Properties()
    kafkaProducerProperties.put("bootstrap.servers", config.outputKafka.bootstrapServers)

    FlinkTweetParams(kafkaConsumerProperties, kafkaProducerProperties,
      config.inputKafka.inputTopic, config.outputKafka.outputTopic)
  }
}
