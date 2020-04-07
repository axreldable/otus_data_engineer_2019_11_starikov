package ru.star

import java.util.Properties

import pureconfig._
//import pureconfig.generic.auto._
import pureconfig.generic.auto._

final case class InputKafka(bootstrapServers: String, inputTopic: String)

final case class OutputKafka(bootstrapServers: String, outputTopic: String)

final case class SparkTweetJobParams(inputKafka: InputKafka, outputKafka: OutputKafka,
                                     modelPath: String, checkpointLocation: String)

object SparkTweetJobParams {
  def apply(inputArgs: Array[String]): SparkTweetJobParams = {
    ConfigSource.default.loadOrThrow[SparkTweetJobParams]
  }
}
