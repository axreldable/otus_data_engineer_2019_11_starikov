package ru.star

import io.radicalbit.flink.pmml.scala._
import io.radicalbit.flink.pmml.scala.api.reader.ModelReader
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import ru.star.models.InternalEvent

final case class PmmlJobBuilder(env: StreamExecutionEnvironment,
                                irisModelPath: String,
                                eventSource: SourceFunction[InternalEvent],
                                eventSink: SinkFunction[InternalEvent]
                               ) {
  def build(): Unit = {
    val internalEvents = env.addSource(eventSource)

    val modelReader = ModelReader(irisModelPath)

    val prediction: DataStream[(InternalEvent, Double)] = internalEvents.evaluate(modelReader) {
      case (event, model) =>
        val vectorized = event.vector
        val prediction = model.predict(vectorized)
        (event, prediction.value.getOrElse(-1.0))
    }

    prediction.print()
  }
}
