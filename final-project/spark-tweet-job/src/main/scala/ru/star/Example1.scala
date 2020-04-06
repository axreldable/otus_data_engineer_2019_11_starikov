package ru.star

import com.typesafe.scalalogging._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, _}
import ru.star.SparkTweetJob.spark


object Example1 extends App with StrictLogging {
  val appName: String = "spark-stream-job"
  println("!" + appName)

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val inputStreamPath = "/Users/axreldable/Desktop/projects/otus/data-engineer/otus_data_engineer_2019_11_starikov/final-project/spark-tweet-job/data/events-stream"

  val dataSchema = new StructType()
    .add("tweet", StringType)

  val lines = spark
    .readStream
    .schema(dataSchema)
    .option("maxFilesPerTrigger", 1)
    .json(inputStreamPath)

  println(lines.isStreaming)    // Returns True for DataFrames that have streaming sources

  lines.printSchema

  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))

  // Generate running word count
  val wordCounts: DataFrame = words.groupBy("value").count().sort($"count".desc)

  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()
}
