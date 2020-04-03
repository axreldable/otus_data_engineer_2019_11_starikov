package ru.star

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import ru.star.map.MessageConfigMapper
import ru.star.models.{ConfiguredMessage, InternalEvent}
import ru.star.utils.{Helpers, MessageWorker}

final case class InputAdapterBuilder(env: StreamExecutionEnvironment,
                                     eventConfigName: String,
                                     messageSource: SourceFunction[String],
                                     eventSink: SinkFunction[InternalEvent],
                                     stringSink: SinkFunction[String]
                                    ) {
  def build(): Unit = {
    env.registerCachedFile(Helpers.resourceAbsolutePath(eventConfigName), "event.conf", false)

    val source = env.fromCollection(List("type-1,1", "type-2,2", "type-3,3", "type-4,four", "default,5", "6"))

    //    val events = env.addSource(messageSource)
    val configuredMessages: DataStream[ConfiguredMessage] = source
      .map(new MessageConfigMapper())

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
