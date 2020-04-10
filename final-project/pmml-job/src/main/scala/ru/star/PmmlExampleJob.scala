package ru.star

import io.radicalbit.flink.pmml.scala.api.reader.ModelReader
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import org.apache.flink.streaming.api.scala._
import io.radicalbit.flink.pmml.scala._
import org.apache.flink.ml.math.DenseVector

object PmmlExampleJob extends App with EnsureParameters {
  private val logger = LoggerFactory.getLogger(PmmlExampleJob.getClass)
  logger.info("hello!")

  val params: ParameterTool = ParameterTool.fromArgs(args)
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.getConfig.setGlobalJobParameters(params)
  val (inputModel, output) = ensureParams(params)

  //Read data from custom iris source
  val irisDataStream = IrisSource.irisSource(env, None)

  //Convert iris to DenseVector
  val irisToVector: DataStream[DenseVector] = irisDataStream.map(iris => iris.toVector)

  //Load PMML model
  val model = ModelReader(inputModel)

  irisToVector
    .quickEvaluate(model)
    .print()

  env.execute("Quick evaluator Clustering")
}
