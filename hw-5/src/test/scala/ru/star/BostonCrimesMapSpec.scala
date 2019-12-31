package ru.star

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class BostonCrimesMapSpec extends FlatSpec with DataFrameSuiteBase with Matchers {
  behavior of "JsonReader"

  it should "read rdds from json file" in {
    implicit val iSpark: SparkSession = spark

    val rddJsons = BostonCrimesMap.readJsons(Helper.resourceAbsolutePath("wines.json"))

    val expectedRddJsons = Helper.expectedStringJsons()

    rddJsons.collect() sameElements expectedRddJsons.collect()
  }

  it should "transform json to Wine case class" in {
    val winesSource = Source.fromFile(Helper.resourceAbsolutePath("wines.json"))
    val wines = winesSource.getLines.toList.take(2)
      .map(BostonCrimesMap.toWine)

    val expectedWines = Helper.expectedWines()

    wines equals expectedWines
  }
}
