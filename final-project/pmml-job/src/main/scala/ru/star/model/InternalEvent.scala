package ru.star.model

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.radicalbit.flink.pmml.scala.models.control.ServingMessage
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.ml.math.Vector
import org.apache.flink.streaming.api.scala.createTypeInformation

object ServingMessage {
  def deserialize[T](event: Array[Byte]): T = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(event))
    val value = ois.readObject
    ois.close()
    value.asInstanceOf[T]
  }

  def serialize[T](event: T): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(event)
    oos.close()
    stream.toByteArray
  }
}

class ServingMessageDeserializer extends DeserializationSchema[ServingMessage] {
  override def deserialize(event: Array[Byte]): ServingMessage = {
    ServingMessage.deserialize(event)
  }

  override def isEndOfStream(nextEvent: ServingMessage): Boolean = false

  override def getProducedType: TypeInformation[ServingMessage] = {
    createTypeInformation[ServingMessage]
  }
}


class ServingMessageSerializer extends SerializationSchema[ServingMessage] {
  override def serialize(event: ServingMessage): Array[Byte] = ServingMessage.serialize(event)
}
