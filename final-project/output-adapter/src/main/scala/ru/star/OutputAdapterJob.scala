package ru.star

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import ru.star.models.{InternalEvent, InternalEventDeserializer}

object OutputAdapterJob extends App {
  println("output-adapter started.")

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val params = OutputAdapterParams(args)
  println("params", params)

  val messageConsumer = new FlinkKafkaConsumer[String](
    "ml-stream-output-adapter-message-in", new SimpleStringSchema(), params.kafkaConsumerProperties
  )

  val eventConsumer = new FlinkKafkaConsumer[InternalEvent](
    "ml-stream-output-adapter-event-in", new InternalEventDeserializer(), params.kafkaConsumerProperties
  )

  val configConsumer = new FlinkKafkaConsumer[String](
    "ml-stream-output-adapter-config-in", new SimpleStringSchema(), params.kafkaConsumerProperties
  )

  val stringProducer = new FlinkKafkaProducer[(String, String)](
    "ml-stream-output-adapter-error",
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
