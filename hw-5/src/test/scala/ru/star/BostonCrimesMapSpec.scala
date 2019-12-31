package ru.star

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class BostonCrimesMapSpec extends FlatSpec with DataFrameSuiteBase with Matchers {
  behavior of "BostonCrimesMap"

}
