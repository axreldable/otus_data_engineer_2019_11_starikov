package ru.star

import org.apache.spark.sql.SparkSession

object BostonCrimesMap extends App {
  private implicit val spark: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()

  spark.close()
}
