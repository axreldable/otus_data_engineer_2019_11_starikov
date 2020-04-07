package ru.star

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import ru.star.models.InternalEvent
import ru.star.utils.MessageWorker

object InputAdapterJob extends App with LazyLogging {
  println("input-adapter started.")

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val params = InputAdapterParams(args)
  println("params", params)

  val messageConsumer = new FlinkKafkaConsumer[String](
    "ml-stream-input-adapter-message-in", new SimpleStringSchema(), params.kafkaConsumerProperties
  )

  val configConsumer = new FlinkKafkaConsumer[String](
    "ml-stream-input-adapter-config-in", new SimpleStringSchema(), params.kafkaConsumerProperties
  )

  val stringProducer = new FlinkKafkaProducer[String](
    "ml-stream-input-adapter-error",
    new KeyedSerializationSchema[String]() {
      override def serializeKey(event: String): Array[Byte] = null

      override def serializeValue(event: String): Array[Byte] = MessageWorker.getMessage(event).getBytes()

      override def getTargetTopic(event: String): String = MessageWorker.getTopic(event)
    },
    params.kafkaProducerProperties
  )

  val eventProducer = new FlinkKafkaProducer[InternalEvent](
    "ml-stream-input-adapter-error",
    new KeyedSerializationSchema[InternalEvent]() {
      override def serializeKey(event: InternalEvent): Array[Byte] = null

      override def serializeValue(event: InternalEvent): Array[Byte] = event.serialize()

      override def getTargetTopic(event: InternalEvent): String = event.targetTopic
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
