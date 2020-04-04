package ru.star.utils

import com.typesafe.scalalogging.LazyLogging
import ru.star.models.{ConfiguredEvent, ConfiguredMessage, InternalEvent, OutputAdapterConfig}

object MessageWorker extends LazyLogging {
  def mapWithConfig(inMessage: String, outputAdapterConfig: OutputAdapterConfig): ConfiguredMessage = {
    val (messageType, message, prediction) = spitMessage(inMessage)
    ConfiguredMessage(message, prediction, outputAdapterConfig.getEventConfig(messageType))
  }

  def mapWithConfig(inEvent: InternalEvent, outputAdapterConfig: OutputAdapterConfig): ConfiguredEvent = {
    ConfiguredEvent(inEvent, outputAdapterConfig.getEventConfig("default"))
  }

  def stringMessageFrom(configuredMessage: ConfiguredMessage): String = {
    val eventConfig = configuredMessage.config
    val transformedMessage = transformMessage(configuredMessage.message, eventConfig.transformFunction)
    val prediction = configuredMessage.prediction

    Array(eventConfig.targetTopic, transformedMessage, prediction).mkString(",")
  }

  def stringMessageFrom(configuredEvent: ConfiguredEvent): String = {
    val eventConfig = configuredEvent.config
    val transformedMessage = transformMessage(configuredEvent.event.message, eventConfig.transformFunction)
    // todo: implement it
    val prediction = "0"

    Array(eventConfig.targetTopic, transformedMessage, prediction).mkString(",")
  }

  private def transformMessage(message: String, transformFunctionName: String): String = {
    val transformFunction = Transformations.getByName(transformFunctionName)
    transformFunction(message)
  }

  private def spitMessage(message: String): (String, String, String) = {
    message.split(",") match {
      case Array(messageType, message, prediction) => (messageType, message, prediction)
      case _ =>
        logger.info(s"Failed to split message=$message. Use default type.")
        ("default", message, "")
    }
  }

  def getTopic(message: String): String = {
    message.split(",") match {
      case Array(topic, message, prediction) => topic
      case _ => throw new RuntimeException(s"Failed to find target topic in string type message!")
    }
  }

  def getMessage(message: String): String = {
    message.split(",") match {
      case Array(topic, message, prediction) => Array(message, prediction).mkString(",")
      case _ => throw new RuntimeException(s"Failed to find message in string type message!")
    }
  }
}
