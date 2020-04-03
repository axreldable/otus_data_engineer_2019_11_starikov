package ru.star

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import pureconfig._
//import pureconfig.generic.auto._
import pureconfig.generic.auto._

final case class InputKafka(bootstrapServers: String)

final case class OutputKafka(bootstrapServers: String)

final case class TypeTransformation(targetTopic: String, transformFunction: String) extends Serializable

final case class EventConfig(transformConfig: Map[String, TypeTransformation], stringTypes: List[String],
                             internalEventTypes: List[String]) extends Serializable with LazyLogging {
  def targetTopicFromType(messageType: String): String = {
    this.transformConfig.get(messageType) match {
      case Some(typeTransformation) => typeTransformation.targetTopic
      case _ =>
        logger.error(s"Failed to find messageType=$messageType in config! Will use input-adapter-error.")
        "input-adapter-error"
    }
  }
}

final case class InputAdapterParameters(inputKafka: InputKafka, outputKafka: OutputKafka,
                                        eventConfig: EventConfig)

final case class InputAdapterParams(kafkaConsumerProperties: Properties,
                                    kafkaProducerProperties: Properties,
                                    eventConfig: EventConfig)

object InputAdapterParams {
  def apply(inputArgs: Array[String]): InputAdapterParams = {
    val config = ConfigSource.default.loadOrThrow[InputAdapterParameters]

    val kafkaConsumerProperties = new Properties()
    kafkaConsumerProperties.put("bootstrap.servers", config.inputKafka.bootstrapServers)

    val kafkaProducerProperties = new Properties()
    kafkaProducerProperties.put("bootstrap.servers", config.outputKafka.bootstrapServers)

    InputAdapterParams(kafkaConsumerProperties, kafkaProducerProperties, config.eventConfig)
  }
}
