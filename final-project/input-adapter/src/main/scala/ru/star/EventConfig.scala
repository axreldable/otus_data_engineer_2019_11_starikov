package ru.star

import com.typesafe.scalalogging.LazyLogging

final case class TypeConfig(targetTopic: String, transformFunction: String, form: String) extends Serializable

final case class EventConfig(version: String, transformConfig: Map[String, TypeConfig])
  extends Serializable with LazyLogging {
  private def getTypeConfig(messageType: String): TypeConfig = {
    this.transformConfig.get(messageType) match {
      case Some(typeTransformation) => typeTransformation
      case _ =>
        logger.error(s"Failed to find type configuration for messageType=$messageType in config! Will use default.")
        this.transformConfig("default")
    }
  }

  def targetTopic(messageType: String): String = {
    getTypeConfig(messageType).targetTopic
  }

  def transformFunction(messageType: String): String = {
    getTypeConfig(messageType).transformFunction
  }

  def form(messageType: String): String = {
    getTypeConfig(messageType).form
  }
}
