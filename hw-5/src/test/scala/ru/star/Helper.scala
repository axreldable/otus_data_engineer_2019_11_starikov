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
        StructField("crimes_monthly", IntegerType, nullable = false),
        StructField("frequent_crime_types", StringType),
        StructField("lat", DoubleType, nullable = false),
        StructField("lng", DoubleType, nullable = false)
      )
    )

    val data = Seq(
      Row("C6", 42358L, 1075, "SICK/INJURED/MEDICAL, DRUGS, INVESTIGATE PERSON", 42.212798383637164, -70.85659541531778),
      Row(null, 2924L, 70, "DRUGS, M/V ACCIDENT, M/V", 23.23595516905933, -40.20597381162553),
      Row("B2", 89529L, 2338, "VERBAL DISPUTE, SICK/INJURED/MEDICAL, INVESTIGATE PERSON", 42.315938200795536, -71.07556580552921),
      Row("C11", 76824L, 2004, "SICK/INJURED/MEDICAL, INVESTIGATE PERSON, VERBAL DISPUTE", 42.293077489311, -71.05195656619692),
      Row("E13", 31594L, 799, "SICK/INJURED/MEDICAL, INVESTIGATE PERSON, DRUGS", 42.31084139071743, -71.09944246979124),
      Row("B3", 64743L, 1666, "VERBAL DISPUTE, INVESTIGATE PERSON, MISSING PERSON", 42.282710490616815, -71.07839078769328),
      Row("E5", 23490L, 601, "SICK/INJURED/MEDICAL, INVESTIGATE PERSON, PROPERTY", 42.1933382520268, -70.99669276050352),
      Row("A15", 11588L, 288, "INVESTIGATE PERSON, VANDALISM, SICK/INJURED/MEDICAL", 42.18447221798593, -70.75327686373632),
      Row("A7", 24488L, 627, "SICK/INJURED/MEDICAL, INVESTIGATE PERSON, VANDALISM", 42.3589078456626, -71.0014343504576),
      Row("D14", 36032L, 916, "TOWED MOTOR VEHICLE, SICK/INJURED/MEDICAL, INVESTIGATE PERSON", 42.34275757457698, -71.12993328083058),
      Row("D4", 77130L, 2003, "PROPERTY, INVESTIGATE PERSON, SICK/INJURED/MEDICAL", 42.34113387698492, -71.07698400447009),
      Row("E18", 30878L, 779, "SICK/INJURED/MEDICAL, INVESTIGATE PERSON, VERBAL DISPUTE", 42.26264507324335, -71.11885136777379),
      Row("A1", 66302L, 1674, "PROPERTY, SICK/INJURED/MEDICAL, WARRANT ARREST", 42.32937112644855, -71.01693411399596)
    )

    spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )
  }
}
