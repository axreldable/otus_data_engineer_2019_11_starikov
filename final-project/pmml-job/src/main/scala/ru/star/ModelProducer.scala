package ru.star

import java.util.Properties

import io.radicalbit.flink.pmml.scala.models.control.{AddMessage, ServingMessage}
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.slf4j.LoggerFactory
import ru.star.IrisSource.{NumberOfParameters, RandomGenerator, RandomMax, RandomMin, truncateDouble}
import ru.star.model.ServingMessageSerializer
import ru.star.models.{InternalEvent, InternalEventSerializer}

object ModelProducer extends App with EnsureParameters {
  private val logger = LoggerFactory.getLogger(IrisProducer.getClass)
  logger.info("IrisProducer started.")

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val kafkaProperties = new Properties()
  kafkaProperties.put("bootstrap.servers", "localhost:9092")

  val modelProducer = new FlinkKafkaProducer[ServingMessage](
    "ml-stream-pmml-model-in", new ServingMessageSerializer(), kafkaProperties
  )

  val irisDataStream = irisSource(env)

  irisDataStream.addSink(modelProducer)

  env.execute("IrisProducer")

  @throws(classOf[Exception])
  def irisSource(env: StreamExecutionEnvironment): DataStream[ServingMessage] = {
    env.addSource((sc: SourceContext[ServingMessage]) => {
      val path = "/Users/axreldable/Desktop/projects/otus/data-engineer/otus_data_engineer_2019_11_starikov/final-project/pmml-job/src/main/resources/kmeans.xml"
      val message = AddMessage("123e4567-e89b-12d3-a456-426655440000", 1, path, Utils.now())

      sc.collect(message)
    })

  }
}
