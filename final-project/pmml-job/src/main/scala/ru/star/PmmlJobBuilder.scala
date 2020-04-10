package ru.star

import io.radicalbit.flink.pmml.scala._
import io.radicalbit.flink.pmml.scala.api.PmmlModel
import io.radicalbit.flink.pmml.scala.api.reader.ModelReader
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import ru.star.models.{InternalEvent}
import ru.star.sources.ControlSource

final case class PmmlJobBuilder(env: StreamExecutionEnvironment,
                                modelConfigPath: String,
                                eventSource: SourceFunction[InternalEvent],
                                eventSink: SinkFunction[InternalEvent]
                               ) {
  def build(): Unit = {
    env.registerCachedFile(modelConfigPath, "model.conf", false)

//    val messages = env
//      .addSource(eventSource)
//      .name("event-stream")
//      .map(element => (1, element))
//      .keyBy(0)
//    val models = env
//      .addSource(modelSource)
//      .name("model-stream")
//      .map(element => (1, element))
//      .keyBy(0)
//
//    val predictionEvents: DataStream[InternalEvent] = messages.connect(models)
//      .flatMap(new MessageConfigMapper())
//      .name("configured-message-stream")
//
//    val internalEvents = env.addSource(eventSource)
//
//    val modelReader = ModelReader(irisModelPath)
//
//    val r = internalEvents
//      .keyBy(event => event.messageType)
//      .evaluate(modelReader) {
//        case (event, model) =>
//          val vectorized = event.vector
//          val prediction = model.predict(vectorized)
//          (event, prediction.value.getOrElse(-1.0))
//      }
//
//    val prediction: DataStream[(InternalEvent, Double)] = internalEvents.evaluate(modelReader) {
//      case (event, model) =>
//        val vectorized = event.vector
//        val prediction = model.predict(vectorized)
//        (event, prediction.value.getOrElse(-1.0))
//    }
//
//    prediction.print()
  }
}
