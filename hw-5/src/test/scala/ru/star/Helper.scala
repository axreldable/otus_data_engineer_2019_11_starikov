package ru.star

import java.nio.file.Paths

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object Helper {
  def resourceAbsolutePath(resource: String): String = {
    try {
      Paths.get(getClass.getClassLoader.getResource(resource).toURI).toFile.getAbsolutePath
    } catch {
      case _: NullPointerException => throw new NullPointerException(s"Perhaps resource $resource not found.")
    }
  }

  def expectedCrimeStatistic()(implicit spark: SparkSession): DataFrame = {
    val schema = StructType(
      Seq(
        StructField("DISTRICT", StringType),
        StructField("crimes_total", LongType, nullable = false),
        StructField("crimes_monthly", LongType),
        StructField("frequent_crime_types", StringType),
        StructField("lat", DoubleType),
        StructField("lng", DoubleType)
      )
    )

    val data = Seq(
      Row("C6", 25385L, 646L, "SICK/INJURED/MEDICAL, DRUGS, M/V", 42.20569978728891, -70.84528692402687),
      Row(null, 1816L, 42L, "M/V ACCIDENT, M/V, DRUGS", 26.991882721422016, -46.28239534085709),
      Row("B2", 54467L, 1422L, "M/V, VERBAL DISPUTE, SICK/INJURED/MEDICAL", 42.314921927596174, -71.07410758350842),
      Row("C11", 46992L, 1233L, "M/V, SICK/INJURED/MEDICAL, INVESTIGATE PERSON", 42.29230583869601, -71.05094277377799),
      Row("E13", 20076L, 513L, "M/V, SICK/INJURED/MEDICAL, M/V ACCIDENT", 42.308173687998014,-71.09539988374799),
      Row("B3", 38312L, 965L, "VERBAL DISPUTE, INVESTIGATE PERSON, M/V", 42.282145383317, -71.07763247207065),
      Row("E5", 14683L, 376L, "SICK/INJURED/MEDICAL, M/V, INVESTIGATE PERSON", 42.178764298448776,-70.97563721423036),
      Row("A15", 7388L, 187L, "M/V ACCIDENT, INVESTIGATE PERSON, M/V", 42.14367908932849, -70.68739590327756),
      Row("A7", 15372L, 392L, "SICK/INJURED/MEDICAL, DRUGS, M/V ACCIDENT", 42.35707785832368, -70.99771133642915),
      Row("D14", 22913L, 581L, "M/V, TOWED MOTOR VEHICLE, SICK/INJURED/MEDICAL", 42.340672304494795, -71.12646463782836),
      Row("D4", 44887L, 1171L, "PROPERTY, LARCENY THEFT FROM BUILDING, LARCENY IN A BUILDING $200 & OVER", 42.34048321456934, -71.0760364279138),
      Row("E18", 18855L, 476L, "SICK/INJURED/MEDICAL, M/V, INVESTIGATE PERSON", 42.2629874575512,-71.11881548330078),
      Row("A1", 38615L, 977L, "PROPERTY, DRUGS, ASSAULT & BATTERY", 42.32714635519895, -71.01336442252963)
    )

    spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )
  }
}
