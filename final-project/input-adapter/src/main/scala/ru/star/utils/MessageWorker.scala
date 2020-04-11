package ru.star.utils

import com.typesafe.scalalogging.LazyLogging
import ru.star.models.{ConfiguredMessage, InputAdapterConfig, InternalEvent}

object MessageWorker extends LazyLogging {
  def mapWithConfig(inMessage: String, inputAdapterConfig: InputAdapterConfig): ConfiguredMessage = {
    val (messageType, message) = spitTypeMessage(inMessage, inputAdapterConfig.separator)
    ConfiguredMessage(message, inputAdapterConfig.getEventConfig(messageType))
  }

  def internalEventFrom(configuredMessage: ConfiguredMessage): (String, InternalEvent) = {
    val eventConfig = configuredMessage.config

    val transformFunction = Transformations.getEventTransformation(eventConfig.transformFunction)
    val internalEvent = transformFunction(eventConfig.modelId.get, configuredMessage.message)

    (eventConfig.targetTopic, internalEvent)
  }

  def stringMessageFrom(configuredMessage: ConfiguredMessage): (String, String) = {
    val eventConfig = configuredMessage.config

    val transformFunction = Transformations.getStringTransformation(eventConfig.transformFunction)
    val transformedMessage = transformFunction(configuredMessage.message)

    (eventConfig.targetTopic, transformedMessage)
  }

  private def spitTypeMessage(message: String, separator: String): (String, String) = {
    message.split(separator) match {
      case Array(messageType, message) => (messageType, message)
      case _ =>
        logger.info(s"Failed to split event in message=$message with separator=$separator. Use default type.")
        ("default", message)
    }
  }
}
