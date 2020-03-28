package ru.star

import com.typesafe.config.ConfigFactory

object Config {
  def getValue[T](value: Option[T], propertyName: String): T = {
    value.getOrElse(ConfigFactory.load.getAnyRef(propertyName).asInstanceOf[T])
  }
}
