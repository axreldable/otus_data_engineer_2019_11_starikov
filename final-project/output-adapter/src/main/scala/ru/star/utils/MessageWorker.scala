package ru.star.utils

import com.typesafe.scalalogging.LazyLogging
import ru.star.models._

object MessageWorker extends LazyLogging {
  def mapWithConfig(inMessage: String, outputAdapterConfig: OutputAdapterConfig): ConfiguredMessage = {
    val (messageType, message, prediction) = spitMessage(inMessage, outputAdapterConfig.separator)
    ConfiguredMessage(message, prediction, outputAdapterConfig.getEventConfig(messageType))
  }

  def mapWithConfig(inEvent: InternalEvent, outputAdapterConfig: OutputAdapterConfig): ConfiguredEvent = {
    ConfiguredEvent(inEvent, outputAdapterConfig.getEventConfig(inEvent.messageType))
  }

  def stringMessageFrom(configuredMessage: ConfiguredMessage): (String, String) = {
    createTargetEvent(configuredMessage.config, configuredMessage.message, configuredMessage.prediction)
  }

  def stringMessageFrom(configuredEvent: ConfiguredEvent): (String, String) = {
    createTargetEvent(configuredEvent.config, configuredEvent.event.message, configuredEvent.event.prediction.toString)
  }

  def createTargetEvent(config: EventConfig, message: String, prediction: String): (String, String) = {
    val transformedMessage = transform(message, config.transformFunctionEvent)
    val transformedPrediction = transform(prediction, config.transformFunctionPredict)

    (config.targetTopic, Array(transformedMessage, transformedPrediction).mkString(config.separator))
  }

  private def transform(message: String, transformFunctionName: String): String = {
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
