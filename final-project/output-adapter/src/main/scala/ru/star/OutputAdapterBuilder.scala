package ru.star

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import ru.star.map.{EventConfigMapper, MessageConfigMapper}
import ru.star.models.{ConfiguredEvent, ConfiguredMessage, InternalEvent}
import ru.star.utils.MessageWorker

final case class OutputAdapterBuilder(env: StreamExecutionEnvironment,
                                      eventConfigPath: String,
                                      messageSource: SourceFunction[String],
                                      eventSource: SourceFunction[InternalEvent],
                                      configSource: SourceFunction[String],
                                      stringSink: SinkFunction[(String, String)]
                                     ) {
  def build(): Unit = {
    env.registerCachedFile(eventConfigPath, "event.conf", false)

    val messages = env
      .addSource(messageSource)
      .name("message-stream")
      .map(element => (1, element))
      .keyBy(0)
    val events = env
      .addSource(eventSource)
      .name("event-stream")
      .map(element => (1, element))
      .keyBy(0)
    val configs = env
      .addSource(configSource)
      .name("config-stream")
      .map(element => (1, element))
      .keyBy(0)

    val configuredMessages: DataStream[ConfiguredMessage] = messages.connect(configs)
      .flatMap(new MessageConfigMapper())
      .name("configured-message-stream")

    val configuredEvents: DataStream[ConfiguredEvent] = events.connect(configs)
      .flatMap(new EventConfigMapper())
      .name("configured-event-stream")

    configuredMessages
      .map(MessageWorker.stringMessageFrom(_))
      .name("result-message-stream")
      .addSink(stringSink)
      .name("result-sink")

    configuredEvents
      .map(MessageWorker.stringMessageFrom(_))
      .name("result-event-stream")
      .addSink(stringSink)
      .name("result-sink")
  }
}
