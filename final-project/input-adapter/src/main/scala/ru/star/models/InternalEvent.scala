package ru.star.models

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

case class InternalEvent(message: String, targetTopic: String) extends Serializable {
  def serialize(): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(this)
    oos.close()
    stream.toByteArray
  }
}
