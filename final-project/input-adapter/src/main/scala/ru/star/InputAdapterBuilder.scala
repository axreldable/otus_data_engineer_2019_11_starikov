package ru.star

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import ru.star.map.MessageConfigMapper
import ru.star.models.{ConfiguredMessage, InternalEvent}
import ru.star.utils.MessageWorker

final case class InputAdapterBuilder(env: StreamExecutionEnvironment,
                                     eventConfigPath: String,
                                     messageSource: SourceFunction[String],
                                     configSource: SourceFunction[String],
                                     eventSink: SinkFunction[InternalEvent],
                                     stringSink: SinkFunction[String]
                                    ) {
  def build(): Unit = {
    env.registerCachedFile(eventConfigPath, "event.conf", false)

    val messages = env
      .addSource(messageSource)
      .name("message-stream")
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

    configuredMessages
      .filter(_.config.form.equals("internal-event"))
      .map(MessageWorker.internalEventFrom(_))
      .name("internal-event-stream")
      .addSink(eventSink)
      .name("internal-event-kafka-sink")

    configuredMessages
      .filter(_.config.form.equals("string"))
      .map(MessageWorker.stringMessageFrom(_))
      .name("string-message-stream")
      .addSink(stringSink)
      .name("string-message-kafka-sink")
  }
}
