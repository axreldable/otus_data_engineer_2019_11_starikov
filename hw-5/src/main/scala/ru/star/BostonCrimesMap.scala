package ru.star

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
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

  private val crimeStatistic = calculateCrimeStatistic(crimeFacts, offenseCodes)

  crimeStatistic
    .coalesce(1)
    .write
    .parquet(appParams.resultOutputFolder)

  spark.close()

  private[star] def calculateCrimeStatistic(crimeFacts: DataFrame, offenseCodes: DataFrame)
                                           (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val crimes = crimeFacts.join(broadcast(offenseCodes), $"OFFENSE_CODE" === $"CODE")

    val window = Window.partitionBy("DISTRICT").orderBy($"crime_type_count".desc)
    val crimeTypesFrequent = crimes
      .groupBy("DISTRICT", "crime_type")
      .agg(count("*").as("crime_type_count"))
      .withColumn("row_number", row_number.over(window))
      .filter($"row_number" <= 3)
      .drop("crime_type_count", "row_number")
      .groupBy("DISTRICT")
      .agg(array_join(collect_list($"crime_type"), ", ") as "frequent_crime_types")
      .withColumnRenamed("DISTRICT", "DISTRICT_1")

    crimes.join(crimeTypesFrequent, crimes("DISTRICT") <=> crimeTypesFrequent("DISTRICT_1"))
      .groupBy("DISTRICT")
      .agg(
        count("*").as("crimes_total"),
        medianCrimesMonthly(collect_list($"MONTH"), collect_list($"YEAR")).as("crimes_monthly"),
        first("frequent_crime_types").as("frequent_crime_types"),
        avg($"Lat") as "lat",
        avg($"Long") as "lng"
      )
  }

  private[star] def readCrimes(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .select("DISTRICT", "MONTH", "YEAR", "Lat", "Long", "OFFENSE_CODE")
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

  private[star] def medianCrimesMonthly: UserDefinedFunction = udf((monthNumbers: Seq[Int], yearNumbers: Seq[Int]) => {
    val crimesPerMonth = monthNumbers.zip(yearNumbers)
      .groupBy(identity)
      .mapValues(_.size).values
      .toList

    median(crimesPerMonth)
  })
}
