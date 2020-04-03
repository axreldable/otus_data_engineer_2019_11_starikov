package ru.star

import com.typesafe.scalalogging.LazyLogging

object MessageWorker extends LazyLogging {
  def internalEventFrom(inMessage: String, eventConfig: EventConfig): InternalEvent = {
    val (messageType, message) = spitTypeMessage(inMessage)
    val transformFunction = Transformations.getByName(eventConfig.transformFunction(messageType))
    val transformMessage = transformFunction(message)

    InternalEvent(transformMessage, eventConfig.targetTopic(messageType))
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
