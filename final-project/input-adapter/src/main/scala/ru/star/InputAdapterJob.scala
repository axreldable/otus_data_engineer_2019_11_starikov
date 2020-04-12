package ru.star

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import ru.star.models.{InternalEvent, Serializer}

object InputAdapterJob extends App with LazyLogging {
  println("input-adapter started.")

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val params = InputAdapterParams(args)
  println("params", params)

  val messageConsumer = new FlinkKafkaConsumer[String](
    "ml-stream-input-adapter-message-in", new SimpleStringSchema(), params.kafkaConsumerProperties
  )

  val configConsumer = new FlinkKafkaConsumer[String](
    "ml-stream-input-adapter-config-in", new SimpleStringSchema(), params.kafkaProducerProperties
  )

  val stringProducer = new FlinkKafkaProducer[(String, String)](
    "ml-stream-input-adapter-error",
    new KeyedSerializationSchema[(String, String)]() {
      override def serializeKey(messageTuple: (String, String)): Array[Byte] = null

      override def serializeValue(messageTuple: (String, String)): Array[Byte] = messageTuple match {
        case (targetTopic, message) => message.getBytes()
      }

      override def getTargetTopic(messageTuple: (String, String)): String = messageTuple match {
        case (targetTopic, message) => targetTopic
      }
    },
    params.kafkaProducerProperties
  )

  val eventProducer = new FlinkKafkaProducer[(String, InternalEvent)](
    "ml-stream-input-adapter-error",
    new KeyedSerializationSchema[(String, InternalEvent)]() {
      override def serializeKey(eventTuple: (String, InternalEvent)): Array[Byte] = null

      override def serializeValue(eventTuple: (String, InternalEvent)): Array[Byte] = eventTuple match {
        case (targetTopic, event) => Serializer.serialize(event)
      }

      override def getTargetTopic(eventTuple: (String, InternalEvent)): String = eventTuple match {
        case (targetTopic, event) => targetTopic
      }
    },
    params.kafkaProducerProperties
  )

  InputAdapterBuilder(
    env = env,
    eventConfigPath = params.eventConfigPath,
    messageSource = messageConsumer,
    configSource = configConsumer,
    eventSink = eventProducer,
    stringSink = stringProducer
  ).build()

  env.execute("input-adapter")
}
