package ru.star

import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}

object JsonReader extends App {
  private val jsonPath = args(0)

  private implicit val spark: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()
  implicit lazy val formats = DefaultFormats

  spark.sparkContext.textFile(jsonPath)
    .map(parse(_).extract[Wine])
    .foreach(println)

  spark.close()
}
