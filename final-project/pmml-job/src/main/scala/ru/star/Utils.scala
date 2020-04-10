package ru.star

import java.util.UUID

import io.radicalbit.flink.pmml.scala.models.core.ModelId

object Utils {

  final val modelVersion = 1.toString

  def retrieveMappingIdPath(modelPaths: Seq[String]): Map[String, String] =
    modelPaths.map(path => (UUID.randomUUID().toString, path)).toMap

  def retrieveAvailableId(mappingIdPath: Map[String, String]): Seq[String] =
    mappingIdPath.keys.map(name => name + ModelId.separatorSymbol + modelVersion).toSeq

  def now(): Long = System.currentTimeMillis()
}
