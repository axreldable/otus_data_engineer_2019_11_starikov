package ru.star

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import ru.star.InputAdapterJob.env
import ru.star.InternalEvent.getType

final case class InputAdapterBuilder(env: StreamExecutionEnvironment,
                                     eventConfig: EventConfig,
                                     messageSource: SourceFunction[String],
                                     eventSink: SinkFunction[InternalEvent],
                                     stringSink: SinkFunction[String]
                                    ) {
  def build(): Unit = {
    env.registerCachedFile("file:///path/to/exec/file", "localExecFile", true)

    val source = env.fromCollection(List("type-1,1", "type-2,2", "type-3,3", "default,4", "5"))

//    val events = env.addSource(messageSource)
    val events = source
      .map(message => {
        val messageType = getType(message)
        val topic = eventConfig.targetTopicFromType(messageType)
        InternalEvent.from(message, topic)
      })
      .map(message => message)

//    events
//      .filter(event => eventConfig.internalEventTypes contains event.messageType)
//      .addSink(eventSink)
//    events
//      .addSink(stringSink)

//    events
//      .filter(event => eventConfig.stringTypes contains event.messageType)
//      .map(event => Array(event.targetTopic, event.message).mkString(","))
//      .addSink(stringSink)
  }
}
