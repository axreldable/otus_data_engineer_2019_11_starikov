package ru.star.utils

import com.typesafe.scalalogging.LazyLogging
import ru.star.models.{ConfiguredEvent, ConfiguredMessage, InternalEvent, OutputAdapterConfig}

object MessageWorker extends LazyLogging {
  def mapWithConfig(inMessage: String, outputAdapterConfig: OutputAdapterConfig): ConfiguredMessage = {
    val (messageType, message, prediction) = spitMessage(inMessage, outputAdapterConfig.separator)
    ConfiguredMessage(message, prediction, outputAdapterConfig.getEventConfig(messageType))
  }

  def mapWithConfig(inEvent: InternalEvent, outputAdapterConfig: OutputAdapterConfig): ConfiguredEvent = {
    ConfiguredEvent(inEvent, outputAdapterConfig.getEventConfig(inEvent.messageType))
  }

  def stringMessageFrom(configuredMessage: ConfiguredMessage): (String, String) = {
    val eventConfig = configuredMessage.config
    val transformedMessage = transformMessage(configuredMessage.message, eventConfig.transformFunction)
    val prediction = configuredMessage.prediction

    (eventConfig.targetTopic, Array(transformedMessage, prediction).mkString(eventConfig.separator))
  }

  def stringMessageFrom(configuredEvent: ConfiguredEvent): (String, String) = {
    val eventConfig = configuredEvent.config
    val transformedMessage = transformMessage(configuredEvent.event.message, eventConfig.transformFunction)
    val prediction = configuredEvent.event.prediction.toString

    (eventConfig.targetTopic, Array(transformedMessage, prediction).mkString(eventConfig.separator))
  }

  private def transformMessage(message: String, transformFunctionName: String): String = {
    val transformFunction = Transformations.getByName(transformFunctionName)
    transformFunction(message)
  }

  private def spitMessage(message: String, separator: String): (String, String, String) = {
    message.split(separator) match {
      case Array(messageType, message, prediction) => (messageType, message, prediction)
      case _ =>
        logger.info(s"Failed to split message=$message. Use default type.")
        ("default", message, "")
    }
  }
}
