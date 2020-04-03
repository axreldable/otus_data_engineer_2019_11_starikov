package ru.star

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import ru.star.map.EventConfigMapper

final case class InputAdapterBuilder(env: StreamExecutionEnvironment,
                                     eventConfigPath: String,
                                     messageSource: SourceFunction[String],
                                     eventSink: SinkFunction[InternalEvent],
                                     stringSink: SinkFunction[String]
                                    ) {
  def build(): Unit = {
    env.registerCachedFile(eventConfigPath, "event.conf", false)

    val source = env.fromCollection(List("type-1,1", "type-2,2", "type-3,3", "type-4,four", "default,5", "6"))

    //    val events = env.addSource(messageSource)
    val configuredMessages: DataStream[(String, EventConfig)] = source
      .map(new EventConfigMapper())

    val internalEvens: DataStream[InternalEvent] = configuredMessages
      .filter(configuredMessage => configuredMessage match {
        case (_, config: EventConfig) => config.form.equals("internal-event")
      })
      .map(configuredMessage => configuredMessage match {
        case (message: String, config: EventConfig) => MessageWorker.internalEventFrom(message, config)
      })

    val stringMessages: DataStream[String] = configuredMessages
      .filter(configuredMessage => configuredMessage match {
        case (_, config: EventConfig) => config.form.equals("string")
      })
      .map(configuredMessage => configuredMessage match {
        case (message: String, config: EventConfig) => MessageWorker.stringMessageFrom(message, config)
      })

    internalEvens.map(println(_))
    stringMessages.map(println(_))
  }
}
