package ru.star.map

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import pureconfig.ConfigSource
import ru.star.models.{ConfiguredMessage, OutputAdapterConfig}
import ru.star.utils.MessageWorker
//import pureconfig.generic.auto._
import pureconfig.generic.auto._

class MessageConfigMapper extends RichCoFlatMapFunction[(Int, String), (Int, String), ConfiguredMessage] with LazyLogging {

  private var currentConfig: ValueState[OutputAdapterConfig] = _

  override def open(config: Configuration): Unit = {
    super.open(config)

    currentConfig = getRuntimeContext.getState(
      new ValueStateDescriptor("config-state", createTypeInformation[OutputAdapterConfig])
    )
  }

  override def flatMap1(keyedMessage: (Int, String), out: Collector[ConfiguredMessage]): Unit = {
    val message = keyedMessage._2

    if (currentConfig.value() == null) {
      val confFile = getRuntimeContext.getDistributedCache.getFile("event.conf")
      currentConfig.update(ConfigSource.file(confFile).loadOrThrow[OutputAdapterConfig])
      println("Update config from file")
    }

    out.collect(MessageWorker.mapWithConfig(message, currentConfig.value()))
  }

  override def flatMap2(keyedConfig: (Int, String), out: Collector[ConfiguredMessage]): Unit = {
    val config = keyedConfig._2

    currentConfig.update(ConfigSource.string(config).loadOrThrow[OutputAdapterConfig])
    println("Update config from stream")
  }
}
