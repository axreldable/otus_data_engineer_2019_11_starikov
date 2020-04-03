package ru.star

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import pureconfig.ConfigSource
//import pureconfig.generic.auto._
import pureconfig.generic.auto._

class InternalEventMapper extends RichMapFunction[String, InternalEvent] with LazyLogging {

  private var eventConf: EventConfig = _

  override def open(config: Configuration): Unit = {
    super.open(config)

    val confFile = getRuntimeContext.getDistributedCache.getFile("event.conf")
    eventConf = ConfigSource.file(confFile).loadOrThrow[EventConfig]
  }

  override def map(message: String): InternalEvent = {
    MessageWorker.internalEventFrom(message, eventConf)
  }
}
