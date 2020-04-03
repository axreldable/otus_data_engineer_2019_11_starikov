package ru.star

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import com.typesafe.scalalogging.LazyLogging

case class InternalEvent(messageType: String, message: String, targetTopic: String) extends Serializable {
  def serialize(): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(this)
    oos.close()
    stream.toByteArray
  }
}

object InternalEvent extends Serializable with LazyLogging {
  def from(message: String, topic: String): InternalEvent = {
    val messageType = getType(message)
    InternalEvent(messageType, message, topic)
  }

  def getType(message: String): String = {
    message.split(",") match {
      case Array(messageType, message) => messageType
      case _ =>
        logger.info(s"Failed to find event type in message=$message. Use default type.")
        "default"
    }
  }
}
