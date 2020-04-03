package ru.star

import com.typesafe.scalalogging.LazyLogging

object MessageWorker extends LazyLogging {
  def mapWithConfig(inMessage: String, inputAdapterConfig: InputAdapterConfig): (String, EventConfig) = {
    val (messageType, message) = spitTypeMessage(inMessage)
    (message, inputAdapterConfig.getEventConfig(messageType))
  }

  def internalEventFrom(message: String, eventConfig: EventConfig): InternalEvent = {
    val transformedMessage = transformMessage(message, eventConfig.transformFunction)

    InternalEvent(transformedMessage, eventConfig.targetTopic)
  }

  def stringMessageFrom(message: String, eventConfig: EventConfig): String = {
    val transformedMessage = transformMessage(message, eventConfig.transformFunction)

    Array(eventConfig.targetTopic, transformedMessage).mkString(",")
  }

  private def transformMessage(message: String, transformFunctionName: String): String = {
    val transformFunction = Transformations.getByName(transformFunctionName)
    transformFunction(message)
  }

  private def spitTypeMessage(message: String): (String, String) = {
    message.split(",") match {
      case Array(messageType, message) => (messageType, message)
      case _ =>
        logger.info(s"Failed to find event type in message=$message. Use default type.")
        ("default", message)
    }
  }

  def getTopic(message: String): String = {
    message.split(",") match {
      case Array(topic, message) => topic
      case _ => throw new RuntimeException(s"Failed to find target topic in string type message!")
    }
  }

  def getMessage(message: String): String = {
    message.split(",") match {
      case Array(topic, message) => message
      case _ => throw new RuntimeException(s"Failed to find message in string type message!")
    }
  }
}
