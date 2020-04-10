/*
 * Copyright (C) 2017  Radicalbit
 *
 * This file is part of flink-JPMML
 *
 * flink-JPMML is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * flink-JPMML is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with flink-JPMML.  If not, see <http://www.gnu.org/licenses/>.
 */

package ru.star

import io.radicalbit.flink.pmml.scala._
import io.radicalbit.flink.pmml.scala.api.PmmlModel
import io.radicalbit.flink.pmml.scala.logging.LazyLogging
import io.radicalbit.flink.pmml.scala.models.control.ServingMessage
import io.radicalbit.flink.pmml.scala.models.core.ModelId
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import ru.star.sources.ControlSource

/**
  * Toy Job about Stateful Dynamic Model Serving:
  * The job owns two input streams:
  * - event stream: The main input events stream
  * - control stream: Control messages about model repository server current state
  *
  */
object DynamicEvaluateKmeans {

  def main(args: Array[String]): Unit = {

    val parameterTool = ParameterTool.fromArgs(args)

    val parameters = DynamicParams.fromParameterTool(parameterTool)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(parameters.ckpInterval, CheckpointingMode.EXACTLY_ONCE)

    val currentModelId: ModelId = ModelId.fromIdentifier("123e4567-e89b-12d3-a456-426655440000_1")
    println(currentModelId)

    val eventStream = IrisSource.irisSource(env, Option(parameters.availableIds))
    val controlStream = ModelSource.modelSource(env, Option(parameters.availableIds)).map(iris => {
      println(iris)
      iris
    })

    val predictions = eventStream.map(iris => {
      println(iris)
      iris
    })
      .withSupportStream(controlStream)
      .evaluate { (event: Iris, model: PmmlModel) =>
        println("!!!")
        println(event)
        println(model)
        val vectorized = event.toVector
        val prediction = model.predict(vectorized, Some(0.0))
        (event, prediction.value)
      }

    predictions.print()

    env.execute("Dynamic Clustering Example")
  }

}
