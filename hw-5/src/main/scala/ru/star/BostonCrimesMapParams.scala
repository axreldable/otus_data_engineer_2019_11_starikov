package ru.star

final case class BostonCrimesMapParams(
                                        crimePath: String = null,
                                        offenseCodesPath: String = null,
                                        resultOutputFolder: String = null
                                      )

object BostonCrimesMapParams {
  private lazy val parser = new scopt.OptionParser[BostonCrimesMapParams]("boston-crimes-params") {
    opt[String]("crime-path") action {
      (v, c) => c copy (crimePath = v)
    } text "A path to the crime.csv"

    opt[String]("offense-codes-path") action {
      (v, c) => c copy (offenseCodesPath = v)
    } text "A path to the offense_codes.csv"

    opt[String]("result-output-folder") action {
      (v, c) => c copy (resultOutputFolder = v)
    } text "A path to the crimes statistic result directory"
  }

  def apply(inputArgs: Array[String]): BostonCrimesMapParams = parser
    .parse(inputArgs, BostonCrimesMapParams())
    .getOrElse {
      throw new Exception(s"Failed to parse application arguments: ${inputArgs.mkString(",")}.")
    }
}
