package ru.star

import io.radicalbit.flink.pmml.scala._
import io.radicalbit.flink.pmml.scala.api.PmmlModel
import io.radicalbit.flink.pmml.scala.models.control.ServingMessage
import io.radicalbit.flink.pmml.scala.models.input.BaseEvent
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import ru.star.models.InternalEvent

case class PmmlEvent(modelId: String, occurredOn: Long, internalEvent: InternalEvent) extends BaseEvent

final case class PmmlJobBuilder(env: StreamExecutionEnvironment,
                                eventSource: SourceFunction[InternalEvent],
                                modelSource: SourceFunction[ServingMessage],
                                eventSink: SinkFunction[InternalEvent]
                               ) {
  def build(): Unit = {
    val events = env.addSource(eventSource)
      .map(internalEvent => PmmlEvent(internalEvent.modelId, System.currentTimeMillis(), internalEvent))
    val models = env.addSource(modelSource)

    val predictionEvents: DataStream[InternalEvent] = events
      .withSupportStream(models)
      .evaluate { (event: PmmlEvent, model: PmmlModel) =>
        println(event)
        println(model)
        val vectorized = event.internalEvent.vector
        val prediction = model.predict(vectorized)
        InternalEvent(
          messageType = event.internalEvent.messageType,
          modelId = event.modelId,
          message = event.internalEvent.message,
          vector = event.internalEvent.vector,
          prediction = prediction.value.getOrElse(-1.0)
        )
      }

    predictionEvents.addSink(eventSink)
  }
}
