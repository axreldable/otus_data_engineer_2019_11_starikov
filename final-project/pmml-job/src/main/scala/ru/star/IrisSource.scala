package ru.star

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object IrisSource {
  private final val NumberOfParameters = 4
  private final lazy val RandomGenerator = scala.util.Random
  private final val RandomMin = 0.2
  private final val RandomMax = 6.0

  private final def truncateDouble(n: Double) = (math floor n * 10) / 10

  @throws(classOf[Exception])
  def irisSource(env: StreamExecutionEnvironment, availableModelIdOp: Option[Seq[String]]): DataStream[Iris] = {
    val availableModelId = availableModelIdOp.getOrElse(Seq.empty[String])
    env.addSource((sc: SourceContext[Iris]) => {
      while (true) {
        def randomVal = RandomMin + (RandomMax - RandomMin) * RandomGenerator.nextDouble()
        val dataForIris = Seq.fill(NumberOfParameters)(truncateDouble(randomVal))
        val iris =
          Iris("123e4567-e89b-12d3-a456-426655440000_1",
               dataForIris(0),
               dataForIris(1),
               dataForIris(2),
               dataForIris(3),
               Utils.now())
        sc.collect(iris)
        Thread.sleep(1000)
      }
    })

  }

}
