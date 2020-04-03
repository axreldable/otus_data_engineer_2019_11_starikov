package ru.star.map

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import pureconfig.ConfigSource
import ru.star.models.{ConfiguredMessage, InputAdapterConfig}
import ru.star.process.MessageWorker
//import pureconfig.generic.auto._
import pureconfig.generic.auto._

class MessageConfigMapper extends RichMapFunction[String, ConfiguredMessage] with LazyLogging {

  private var inputAdapterConfig: InputAdapterConfig = _

  override def open(config: Configuration): Unit = {
    super.open(config)

    val confFile = getRuntimeContext.getDistributedCache.getFile("event.conf")
    inputAdapterConfig = ConfigSource.file(confFile).loadOrThrow[InputAdapterConfig]
  }

  override def map(message: String): ConfiguredMessage = {
    MessageWorker.mapWithConfig(message, inputAdapterConfig)
  }
}
