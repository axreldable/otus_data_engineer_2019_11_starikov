package ru.star

import java.util.Properties

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.LoggerFactory
import ru.star.models.{InternalEvent, InternalEventSerializer}

object IrisProducer extends App with EnsureParameters {
  private val logger = LoggerFactory.getLogger(IrisProducer.getClass)
  logger.info("IrisProducer started.")

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val kafkaProperties = new Properties()
  kafkaProperties.put("bootstrap.servers", "localhost:9092")

  val eventProducer = new FlinkKafkaProducer[InternalEvent](
    "ml-stream-pmml-event-in", new InternalEventSerializer(), kafkaProperties
  )

  val irisDataStream: DataStream[InternalEvent] = IrisSource.irisSource(env, None).map(iris => {
    val r = InternalEvent("iris", "name_1", iris.toString, iris.toVector, None, 1L)
    println(r)
    r
  })

  irisDataStream.addSink(eventProducer)

  env.execute("IrisProducer")
}
