package ru.star

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import ru.star.map.MessageConfigMapper
import ru.star.models.{ConfiguredMessage, InternalEvent}
import ru.star.utils.{Helpers, MessageWorker}

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
      .map(element => (1, element))
      .keyBy(0)
    val configs = env
      .addSource(configSource)
      .map(element => (1, element))
      .keyBy(0)

    //    val events = env.addSource(messageSource)
    val configuredMessages: DataStream[ConfiguredMessage] = messages.connect(configs)
      .flatMap(new MessageConfigMapper())

    val internalEvens: DataStream[InternalEvent] = configuredMessages
      .filter(_.config.form.equals("internal-event"))
      .map(MessageWorker.internalEventFrom(_))

    val stringMessages: DataStream[String] = configuredMessages
      .filter(_.config.form.equals("string"))
      .map(MessageWorker.stringMessageFrom(_))

    internalEvens.map(println(_))
    stringMessages.map(println(_))
  }
}
