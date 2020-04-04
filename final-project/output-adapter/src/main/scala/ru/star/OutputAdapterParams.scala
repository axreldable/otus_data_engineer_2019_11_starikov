package ru.star

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import pureconfig._
//import pureconfig.generic.auto._
import pureconfig.generic.auto._

final case class InputKafka(bootstrapServers: String)

final case class OutputKafka(bootstrapServers: String)

final case class OutputAdapterParameters(inputKafka: InputKafka, outputKafka: OutputKafka,
                                        eventConfigPath: String)

final case class OutputAdapterParams(kafkaConsumerProperties: Properties,
                                    kafkaProducerProperties: Properties,
                                    eventConfigPath: String)

object OutputAdapterParams {
  def apply(inputArgs: Array[String]): OutputAdapterParams = {
    val config = ConfigSource.default.loadOrThrow[OutputAdapterParameters]

    val kafkaConsumerProperties = new Properties()
    kafkaConsumerProperties.put("bootstrap.servers", config.inputKafka.bootstrapServers)

    val kafkaProducerProperties = new Properties()
    kafkaProducerProperties.put("bootstrap.servers", config.outputKafka.bootstrapServers)

    OutputAdapterParams(kafkaConsumerProperties, kafkaProducerProperties, config.eventConfigPath)
  }
}
