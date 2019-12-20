package ru.star

import org.apache.spark.sql.SparkSession

object JsonReader extends App {
  private val jsonPath = args(0)

  private implicit val spark: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()

  private val r = spark.sparkContext.textFile(jsonPath)

  r.take(10).foreach(println(_))

  spark.close()
}
