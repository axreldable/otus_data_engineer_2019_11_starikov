package ru.star.utils

object Transformations {
  def getByName(name: String): String => String = {
    name match {
      case "as-is" => asIs
      case "first-letter" => firstLetter
      case "tweet" => tweetPrediction
      case "iris" => irisPrediction
      case _ => throw new RuntimeException(s"Unknown transformation name = $name")
    }
  }

  private def asIs(message: String): String = {
    message
  }

  private def firstLetter(message: String): String = {
    message.headOption.getOrElse("").toString
  }

  private def tweetPrediction(predicting: String): String = {
    predicting match {
      case "1" => "positive"
      case "0" => "negative"
      case _ => "unknown"
    }
  }

  private def irisPrediction(predicting: String): String = {
    predicting match {
      case "1.0" => "iris_type_1"
      case "2.0" => "iris_type_2"
      case "3.0" => "iris_type_3"
      case "4.0" => "iris_type_4"
      case _ => "unknown"
    }
  }
}
