package ru.star.model

import io.radicalbit.flink.pmml.scala.models.control.ServingMessage
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.createTypeInformation
import ru.star.models.Serializer

class ServingMessageDeserializer extends DeserializationSchema[ServingMessage] {
  override def deserialize(event: Array[Byte]): ServingMessage = {
    Serializer.deserialize(event)
  }

  override def isEndOfStream(nextEvent: ServingMessage): Boolean = false

  override def getProducedType: TypeInformation[ServingMessage] = {
    createTypeInformation[ServingMessage]
  }
}
