package ru.star

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

object InputAdapterJob extends App with LazyLogging {
  println("input-adapter started.")

  val env = StreamExecutionEnvironment.createLocalEnvironment()

  val params = InputAdapterParams(args)
  println("params", params)

  val messageConsumer = new FlinkKafkaConsumer[String](
    "input-adapter-in", new SimpleStringSchema(), params.kafkaConsumerProperties
  )

  val stringProducer = new FlinkKafkaProducer[String](
    "input-adapter-error",
    new KeyedSerializationSchema[String]() {
      override def serializeKey(event: String): Array[Byte] = null

      override def serializeValue(event: String): Array[Byte] = getMessage(event).getBytes()

      override def getTargetTopic(event: String): String = getTopic(event)
    },
    params.kafkaProducerProperties
  )

  val eventProducer = new FlinkKafkaProducer[InternalEvent](
    "input-adapter-error",
    new KeyedSerializationSchema[InternalEvent]() {
      override def serializeKey(event: InternalEvent): Array[Byte] = null

      override def serializeValue(event: InternalEvent): Array[Byte] = event.serialize()

      override def getTargetTopic(event: InternalEvent): String = event.targetTopic
    },
    params.kafkaProducerProperties
  )

  InputAdapterBuilder(
    env = env,
    eventConfig = params.eventConfig,
    messageSource = messageConsumer,
    eventSink = eventProducer,
    stringSink = stringProducer
  ).build()

  //  env.execute("input-adapter")

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
