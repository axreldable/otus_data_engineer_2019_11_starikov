package ru.star

import java.util.Properties

import pureconfig._
//import pureconfig.generic.auto._
import pureconfig.generic.auto._

final case class KafkaConfig(bootstrapServers: String)

final case class PmmlJobParameters(kafkaConfig: KafkaConfig,
                                   modelConfigPath: String)

final case class PmmlJobParams(kafkaProperties: Properties,
                               modelConfigPath: String)

object PmmlJobParams {
  def apply(inputArgs: Array[String]): PmmlJobParams = {
    val config = ConfigSource.default.loadOrThrow[PmmlJobParameters]

    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", config.kafkaConfig.bootstrapServers)

    PmmlJobParams(kafkaProperties, config.modelConfigPath)
  }
}
