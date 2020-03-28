package ru.star

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

final case class InputAdapterBuilder(env: StreamExecutionEnvironment,
                                     eventSource: SourceFunction[String],
                                     eventSink: SinkFunction[String]
                                    ) {
  def build(): Unit = {
    processMessage(eventSource, eventSink, processString)
  }

  def processString(in: String): String = {
    in
  }

  def processMessage[IN: TypeInformation, OUT: TypeInformation](source: SourceFunction[IN],
                                                                sink: SinkFunction[OUT],
                                                                f: IN => OUT): Unit = {
    env
      .addSource(source)
      .map(message => {
        println(s"Precessing '$message' in input-adapter.")
        f(message)
      })
      .addSink(sink)
  }
}
