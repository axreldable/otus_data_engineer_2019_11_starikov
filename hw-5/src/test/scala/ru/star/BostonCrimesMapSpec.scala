package ru.star

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.{FlatSpec, Matchers}
import ru.star.BostonCrimesMap.{calculateCrimeStatistic, readCrimes, readOffenseCodes}

class BostonCrimesMapSpec extends FlatSpec with DataFrameSuiteBase with Matchers {
  behavior of "BostonCrimesMap"

  it should "calculate crime statistics" in {
    implicit val iSpark: SparkSession = spark

    val crimeFacts = readCrimes(Helper.resourceAbsolutePath("crime.csv"))
    val offenseCodes = readOffenseCodes(Helper.resourceAbsolutePath("offense_codes.csv"))

    val crimeStatistic = calculateCrimeStatistic(crimeFacts, offenseCodes)
    val expectedCrimeStatistic = Helper.expectedCrimeStatistic()

    assertDataFrameEquals(crimeStatistic.orderBy(crimeStatistic.columns.map(col): _*),
      expectedCrimeStatistic.orderBy(expectedCrimeStatistic.columns.map(col): _*))
  }
}
