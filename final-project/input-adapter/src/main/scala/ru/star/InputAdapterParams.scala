package ru.star

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import pureconfig._
//import pureconfig.generic.auto._
import pureconfig.generic.auto._

final case class InputKafka(bootstrapServers: String)

final case class OutputKafka(bootstrapServers: String)

final case class InputAdapterParameters(inputKafka: InputKafka, outputKafka: OutputKafka,
                                        eventConfigPath: String)

final case class InputAdapterParams(kafkaConsumerProperties: Properties,
                                    kafkaProducerProperties: Properties,
                                    eventConfigPath: String)

object InputAdapterParams {
  def apply(inputArgs: Array[String]): InputAdapterParams = {
    val config = ConfigSource.default.loadOrThrow[InputAdapterParameters]

    val kafkaConsumerProperties = new Properties()
    kafkaConsumerProperties.put("bootstrap.servers", config.inputKafka.bootstrapServers)

    val kafkaProducerProperties = new Properties()
    kafkaProducerProperties.put("bootstrap.servers", config.outputKafka.bootstrapServers)

    InputAdapterParams(kafkaConsumerProperties, kafkaProducerProperties, config.eventConfigPath)
  }
}
