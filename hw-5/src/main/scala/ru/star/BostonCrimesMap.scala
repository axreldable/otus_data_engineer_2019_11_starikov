package ru.star

import org.apache.spark.sql.expressions.Window
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

    val windowCrime = Window.partitionBy("DISTRICT").orderBy($"crime_type_count".desc)
    val crimeTypesFrequent = crimes
      .groupBy("DISTRICT", "crime_type")
      .agg(count("*").as("crime_type_count"))
      .withColumn("row_number", row_number.over(windowCrime))
      .filter($"row_number" <= 3)
      .drop("crime_type_count", "row_number")
      .groupBy("DISTRICT")
      .agg(array_join(collect_list($"crime_type"), ", ") as "frequent_crime_types")
      .withColumnRenamed("DISTRICT", "DISTRICT_1")

    val crimesCountPerMonth = crimes
      .groupBy("DISTRICT", "MONTH", "YEAR")
      .agg(count("*").as("crime_per_month"))
      .withColumnRenamed("DISTRICT", "DISTRICT_2")
    crimesCountPerMonth.createOrReplaceTempView("crimesCountPerMonth")
    val crimesMonthly = spark.sql("select DISTRICT_2, percentile_approx(crime_per_month, 0.5) as crime_per_month " +
      "from crimesCountPerMonth " +
      "group by DISTRICT_2")


    crimes.join(crimeTypesFrequent, crimes("DISTRICT") <=> crimeTypesFrequent("DISTRICT_1"))
      .join(crimesMonthly, crimes("DISTRICT") <=> crimesMonthly("DISTRICT_2"))
      .groupBy("DISTRICT")
      .agg(
        count("*").as("crimes_total"),
        first($"crime_per_month").as("crimes_monthly"),
        first($"frequent_crime_types").as("frequent_crime_types"),
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
}
