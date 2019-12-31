package ru.star

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{count, _}

object BostonCrimesMap extends App {
  private implicit val spark: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  private val appParams = BostonCrimesMapParams(args)

  private val crimeFacts = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(appParams.crimePath)
    .select("DISTRICT", "MONTH", "Lat", "Long", "OFFENSE_CODE")
  crimeFacts.show()

  private val offenseCodes = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(appParams.offenseCodesPath)
    .withColumn("crime_type", trim(split('NAME, "-")(0)))
    .select("CODE", "crime_type")
  offenseCodes.show()

  private val crimesWithTypes = crimeFacts.join(broadcast(offenseCodes), $"OFFENSE_CODE" === $"CODE")
    .groupBy("DISTRICT")
    .agg(
      count("*").as("crimes_total"),
      medianCrimesMonthly(collect_list($"MONTH")).as("crimes_monthly"),
      topTypes(collect_list($"crime_type"), lit(3)).as("frequent_crime_types"),
      averageValue(collect_list($"Lat")) as "lat",
      averageValue(collect_list($"Long")) as "lng"
    )

  crimesWithTypes.show()

  spark.close()

  private[star] def median(inputList: List[Double]): Double = {
    val count = inputList.size
    if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      (inputList(l) + inputList(r)) / 2
    } else
      inputList(count / 2)
  }

  private[star] def medianCrimesMonthly: UserDefinedFunction = udf((monthNumbers: Seq[Int]) => {
    val crimesPerMonth = monthNumbers
      .groupBy(identity)
      .mapValues(_.size).values
      .toList.map(_.toDouble)

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
