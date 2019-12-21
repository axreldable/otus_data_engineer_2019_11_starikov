package ru.star

import org.apache.spark.rdd.RDD
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

  readJsons(jsonPath)
    .map(toWine)
    .foreach(println)

  spark.close()

  private[star] def readJsons(path: String)(implicit spark: SparkSession): RDD[String] = {
    spark.sparkContext.textFile(path)
  }

  private[star] def toWine(json: String): Wine = {
    parse(json).extract[Wine]
  }
}
