package ru.star

import org.apache.flink.api.java.utils.ParameterTool
import ru.star.sources.ControlSource

object DynamicParams {

  def fromParameterTool(params: ParameterTool): DynamicParams = {

    val outputPath = params.getRequired("output")

    val pathsAndIds = retrievePathsAndIds(params.getRequired("models"))

    val policy = computeGenPolicy(params.get("gen-policy", "loop"))

    val availableIdModels = computeAvailableIds(pathsAndIds)

    val intervalCheckpoint = params.get("intervalCheckpoint", 1000.toString).toLong

    val maxIntervalControlStream = params.get("maxIntervalControlStream", 5000L.toString).toLong

    DynamicParams(outputPath, policy, pathsAndIds, availableIdModels, intervalCheckpoint, maxIntervalControlStream)
  }

  private def retrievePathsAndIds(paths: String) = {
    val rawModelsPaths = paths.split(",")
    Utils.retrieveMappingIdPath(rawModelsPaths)
  }

  private def computeGenPolicy(rawPolicy: String) =
    rawPolicy match {
      case "random" => ControlSource.Random
      case "loop" => ControlSource.Loop
      case "finite" => ControlSource.Finite
      case _ => throw new IllegalArgumentException(s"$rawPolicy is not recognized generation policy.")
    }

  private def computeAvailableIds(pathsAndIds: Map[String, String]) =
    Utils.retrieveAvailableId(pathsAndIds)

}

case class DynamicParams(outputPath: String,
                         genPolicy: ControlSource.Mode,
                         pathAndIds: Map[String, String],
                         availableIds: Seq[String],
                         ckpInterval: Long,
                         ctrlGenInterval: Long)
