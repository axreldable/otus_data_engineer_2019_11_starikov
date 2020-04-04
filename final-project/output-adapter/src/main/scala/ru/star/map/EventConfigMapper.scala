package ru.star.map

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import pureconfig.ConfigSource
import ru.star.models.{ConfiguredEvent, ConfiguredMessage, InternalEvent, OutputAdapterConfig}
import ru.star.utils.MessageWorker
//import pureconfig.generic.auto._
import pureconfig.generic.auto._

class EventConfigMapper extends RichCoFlatMapFunction[(Int, InternalEvent), (Int, String), ConfiguredEvent] with LazyLogging {

  private var currentConfig: ValueState[OutputAdapterConfig] = _

  override def open(config: Configuration): Unit = {
    super.open(config)

    currentConfig = getRuntimeContext.getState(
      new ValueStateDescriptor("config-state", createTypeInformation[OutputAdapterConfig])
    )
  }

  override def flatMap1(keyedEvent: (Int, InternalEvent), out: Collector[ConfiguredEvent]): Unit = {
    val event = keyedEvent._2

    if (currentConfig.value() == null) {
      val confFile = getRuntimeContext.getDistributedCache.getFile("event.conf")
      currentConfig.update(ConfigSource.file(confFile).loadOrThrow[OutputAdapterConfig])
      println("Update config from file")
    }

    out.collect(MessageWorker.mapWithConfig(event, currentConfig.value()))
  }

  override def flatMap2(keyedConfig: (Int, String), out: Collector[ConfiguredEvent]): Unit = {
    val config = keyedConfig._2

    currentConfig.update(ConfigSource.string(config).loadOrThrow[OutputAdapterConfig])
    println("Update config from stream")
  }
}
