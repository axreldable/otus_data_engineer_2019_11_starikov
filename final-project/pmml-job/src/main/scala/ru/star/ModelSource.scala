package ru.star

import io.radicalbit.flink.pmml.scala.models.control.{AddMessage, ServingMessage}
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

object ModelSource {
  private final val NumberOfParameters = 4
  private final lazy val RandomGenerator = scala.util.Random
  private final val RandomMin = 0.2
  private final val RandomMax = 6.0

  private final def truncateDouble(n: Double) = (math floor n * 10) / 10

  @throws(classOf[Exception])
  def modelSource(env: StreamExecutionEnvironment, availableModelIdOp: Option[Seq[String]]): DataStream[ServingMessage] = {
    val availableModelId = availableModelIdOp.getOrElse(Seq.empty[String])
    env.addSource((sc: SourceContext[ServingMessage]) => {
      val path = "/Users/axreldable/Desktop/projects/otus/data-engineer/otus_data_engineer_2019_11_starikov/final-project/pmml-job/src/main/resources/kmeans.xml"
      val message =  AddMessage("123e4567-e89b-12d3-a456-426655440000", 1L, path, 1L)
      println(message.modelId)
      sc.collect(message)
      Thread.sleep(1000)
    })

  }

}
