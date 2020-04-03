package ru.star

object Transformations {
  def getByName(name: String): String => String = {
    name match {
      case "as-is" => asIs
      case "first-letter" => firstLetter
      case _ => throw new RuntimeException(s"Unknown transformation name = $name")
    }
  }

  def asIs(message: String): String = {
    message
  }

  def firstLetter(message: String): String = {
    message.headOption.getOrElse("").toString
  }
}
