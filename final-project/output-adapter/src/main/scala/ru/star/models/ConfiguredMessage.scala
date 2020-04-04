package ru.star.models

case class ConfiguredMessage(message: String, prediction: String, config: EventConfig) extends Serializable
case class ConfiguredEvent(event: InternalEvent, config: EventConfig) extends Serializable
