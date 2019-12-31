package ru.star

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{count, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BostonCrimesMap extends App {
  private implicit val spark: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()

  private val appParams = BostonCrimesMapParams(args)

  private val crimeFacts = readCrimes(appParams.crimePath)
  private val offenseCodes = readOffenseCodes(appParams.offenseCodesPath)

  private val crimesWithTypes = calculateCrimeStatistic(crimeFacts, offenseCodes)
  crimesWithTypes.show()

  spark.close()

  private[star] def calculateCrimeStatistic(crimeFacts: DataFrame, offenseCodes: DataFrame)
                                           (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    crimeFacts.join(broadcast(offenseCodes), $"OFFENSE_CODE" === $"CODE")
      .groupBy("DISTRICT")
      .agg(
        count("*").as("crimes_total"),
        medianCrimesMonthly(collect_list($"MONTH")).as("crimes_monthly"),
        topTypes(collect_list($"crime_type"), lit(3)).as("frequent_crime_types"),
        averageValue(collect_list($"Lat")) as "lat",
        averageValue(collect_list($"Long")) as "lng"
      )
  }

  private[star] def readCrimes(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .select("DISTRICT", "MONTH", "Lat", "Long", "OFFENSE_CODE")
  }

  private[star] def readOffenseCodes(path: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .withColumn("crime_type", trim(split($"NAME", "-")(0)))
      .select("CODE", "crime_type")
  }

  private[star] def median(seq: Seq[Int]): Int = {
    val sortedSeq = seq.sortWith(_ < _)

    if (seq.size % 2 != 0) {
      sortedSeq(sortedSeq.size / 2)
    }
    else {
      val (up, down) = sortedSeq.splitAt(seq.size / 2)
      (up.last + down.head) / 2
    }
  }

  private[star] def medianCrimesMonthly: UserDefinedFunction = udf((monthNumbers: Seq[Int]) => {
    val crimesPerMonth = monthNumbers
      .groupBy(identity)
      .mapValues(_.size).values
      .toList

    median(crimesPerMonth)
  })

  private[star] def topTypes: UserDefinedFunction = udf((types: Seq[String], amount: Int) => {
    types.groupBy(identity)
      .mapValues(_.size)
      .toList.sortBy({
      case (typeName, count) => count
    }).reverse.take(amount).map({
      case (typeName, count) => typeName
    }).mkString(", ")
  })

  private[star] def averageValue: UserDefinedFunction = udf((coords: Seq[Double]) => {
    coords.sum / coords.length
  })
}
