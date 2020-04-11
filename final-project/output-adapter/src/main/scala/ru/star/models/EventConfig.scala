package ru.star.models

import com.typesafe.scalalogging.LazyLogging

final case class EventConfig(targetTopic: String, transformFunction: String, separator: String) extends Serializable

final case class OutputAdapterConfig(version: String, separator: String, transformConfig: Map[String, EventConfig])
  extends Serializable with LazyLogging {
  def getEventConfig(messageType: String): EventConfig = {
    this.transformConfig.get(messageType) match {
      case Some(typeTransformation) => typeTransformation
      case _ =>
        logger.error(s"Failed to find type configuration for messageType=$messageType in config! Will use default.")
        this.transformConfig("default")
    }
  }
}
