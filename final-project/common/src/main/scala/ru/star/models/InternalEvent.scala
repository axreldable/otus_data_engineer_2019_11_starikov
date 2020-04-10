package ru.star.models

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.radicalbit.flink.pmml.scala.models.input.BaseEvent
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.ml.math.Vector
import org.apache.flink.streaming.api.scala.createTypeInformation

case class InternalEvent(messageType: String, modelId: String, message: String, vector: Vector, prediction: Option[Double], occurredOn: Long) extends BaseEvent with Serializable

object InternalEvent {
  def deserialize(event: Array[Byte]): InternalEvent = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(event))
    val value = ois.readObject
    ois.close()
    value.asInstanceOf[InternalEvent]
  }

  def serialize(event: InternalEvent): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(event)
    oos.close()
    stream.toByteArray
  }
}

class InternalEventDeserializer extends DeserializationSchema[InternalEvent] {
  override def deserialize(event: Array[Byte]): InternalEvent = {
    InternalEvent.deserialize(event)
  }

  override def isEndOfStream(nextEvent: InternalEvent): Boolean = false

  override def getProducedType: TypeInformation[InternalEvent] = {
    createTypeInformation[InternalEvent]
  }
}


class InternalEventSerializer extends SerializationSchema[InternalEvent] {
  override def serialize(event: InternalEvent): Array[Byte] = InternalEvent.serialize(event)
}
