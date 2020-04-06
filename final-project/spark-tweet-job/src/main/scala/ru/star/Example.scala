package ru.star

import com.typesafe.scalalogging._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.{StringType, StructType}


object Example extends App with StrictLogging {
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

  // Create DataFrame representing the stream of input lines from connection to localhost:9999
//  val lines = spark.readStream
//    .schema(dataSchema)
//    .option("maxFilesPerTrigger", 1)
//    .json(inputStreamPath)

//  val lines = spark.readStream
//    .textFile(inputStreamPath)

  // Create DataFrame representing the stream of input lines from connection to localhost:9999
  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 1234)
    .load()

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
