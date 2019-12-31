package ru.star

import org.scalatest.{FlatSpec, Matchers}

class BostonCrimesMapParamsSpec extends FlatSpec with Matchers {
  behavior of "BostonCrimesMapParams"

  it should "parse string parameters to case class" in {
    val args = Array(
      "--crime-path", "/src/main/resources/crime.csv",
      "--offense-codes-path", "/src/main/resources/offense_codes.csv",
      "--result-output-folder", "/src/main/resources/crime_result_calc"
    )

    val rez = BostonCrimesMapParams(args)

    rez.crimePath shouldEqual "/src/main/resources/crime.csv"
    rez.offenseCodesPath shouldEqual "/src/main/resources/offense_codes.csv"
    rez.resultOutputFolder shouldEqual "/src/main/resources/crime_result_calc"
  }
}
