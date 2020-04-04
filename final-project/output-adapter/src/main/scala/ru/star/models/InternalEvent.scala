package ru.star.models

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.createTypeInformation

case class InternalEvent(message: String, targetTopic: String) extends Serializable {
  def serialize(): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(this)
    oos.close()
    stream.toByteArray
  }
}

class InternalEventDeserializer extends DeserializationSchema[InternalEvent] {
  override def deserialize(message: Array[Byte]): InternalEvent = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(message))
    val value = ois.readObject
    ois.close()
    value.asInstanceOf[InternalEvent]
  }

  override def isEndOfStream(nextElement: InternalEvent): Boolean = false

  override def getProducedType: TypeInformation[InternalEvent] = {
    createTypeInformation[InternalEvent]
  }
}


class InternalEventSerializer extends SerializationSchema[InternalEvent] {
  override def serialize(element: InternalEvent): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(element)
    oos.close()
    stream.toByteArray
  }
}
