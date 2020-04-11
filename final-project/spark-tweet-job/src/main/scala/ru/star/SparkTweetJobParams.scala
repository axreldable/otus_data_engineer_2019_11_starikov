package ru.star

import pureconfig._
//import pureconfig.generic.auto._
import pureconfig.generic.auto._

final case class KafkaConfig(bootstrapServers: String, inputTopic: String, outputTopic: String)

final case class SparkTweetJobParams(kafkaConfig: KafkaConfig, modelPath: String, checkpointLocation: String)

object SparkTweetJobParams {
  def apply(inputArgs: Array[String]): SparkTweetJobParams = {
    ConfigSource.default.loadOrThrow[SparkTweetJobParams]
  }
}
