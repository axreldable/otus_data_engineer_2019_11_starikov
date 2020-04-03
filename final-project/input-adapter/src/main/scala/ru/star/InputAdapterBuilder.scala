package ru.star

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import ru.star.InputAdapterJob.env

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
    val events = source
      .map(new EventConfigMapper())
      .map(println(_))

//      events
//        .filter(event => event.fo)
//        .addSink(eventSink)
//      events
//        .addSink(stringSink)
//
//        events
//          .filter(event => eventConfig.stringTypes contains event.messageType)
//          .map(event => Array(event.targetTopic, event.message).mkString(","))
//          .addSink(stringSink)
  }
}
