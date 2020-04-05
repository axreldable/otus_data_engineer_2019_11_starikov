package ru.star

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import ru.star.models.{InternalEvent, InternalEventDeserializer}
import ru.star.utils.MessageWorker

object OutputAdapterJob extends App {
  println("output-adapter started.")

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val params = OutputAdapterParams(args)
  println("params", params)

  val messageConsumer = new FlinkKafkaConsumer[String](
    "output-adapter-message-in", new SimpleStringSchema(), params.kafkaConsumerProperties
  )

  val eventConsumer = new FlinkKafkaConsumer[InternalEvent](
    "output-adapter-event-in", new InternalEventDeserializer(), params.kafkaConsumerProperties
  )

  val configConsumer = new FlinkKafkaConsumer[String](
    "output-adapter-config-in", new SimpleStringSchema(), params.kafkaConsumerProperties
  )

  val stringProducer = new FlinkKafkaProducer[String](
    "output-adapter-error",
    new KeyedSerializationSchema[String]() {
      override def serializeKey(event: String): Array[Byte] = null

      override def serializeValue(event: String): Array[Byte] = {
        println(s"Try to get message from $event")
        MessageWorker.getMessage(event).getBytes()
      }

      override def getTargetTopic(event: String): String = MessageWorker.getTopic(event)
    },
    params.kafkaProducerProperties
  )

  OutputAdapterBuilder(
    env = env,
    eventConfigPath = params.eventConfigPath,
    messageSource = messageConsumer,
    eventSource = eventConsumer,
    configSource = configConsumer,
    stringSink = stringProducer
  ).build()

  env.execute("output-adapter")
}
