package ru.star.models

case class ConfiguredMessage(message: String, config: EventConfig) extends Serializable
