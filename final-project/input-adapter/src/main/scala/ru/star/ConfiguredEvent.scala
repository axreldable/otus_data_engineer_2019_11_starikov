package ru.star

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

case class ConfiguredEvent(event: InternalEvent, config: EventConfig) extends Serializable
