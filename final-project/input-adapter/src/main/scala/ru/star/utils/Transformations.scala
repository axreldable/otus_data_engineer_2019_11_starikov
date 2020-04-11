package ru.star.utils

import org.apache.flink.ml.math.DenseVector
import ru.star.models.InternalEvent

object Transformations {
  def getStringTransformation(name: String): String => String = {
    name match {
      case "as-is" => asIs
      case "first-letter" => firstLetter
      case _ => throw new RuntimeException(s"Unknown transformation name = $name")
    }
  }

  def getEventTransformation(name: String): (String, String) => InternalEvent = {
    name match {
      case "iris-event" => irisEvent
      case _ => throw new RuntimeException(s"Unknown transformation name = $name")
    }
  }

  private def asIs(message: String): String = {
    message
  }

  private def firstLetter(message: String): String = {
    message.headOption.getOrElse("").toString
  }

  /**
   * Iris event looks like: 4.3,5.6,3.3,3.8
   */
  private def irisEvent(modelId: String, message: String): InternalEvent = {
    val Array(sepalLength, sepalWidth, petalLength, petalWidth) = message.split(",").map(_.toDouble)
    InternalEvent(
      messageType = "iris-event",
      modelId = modelId,
      message = message,
      vector = DenseVector(sepalLength, sepalWidth, petalLength, petalWidth),
      prediction = Double.NaN
    )
  }
}
