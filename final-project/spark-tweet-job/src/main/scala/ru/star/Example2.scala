package ru.star

import com.typesafe.scalalogging._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._


object Example2 extends App with StrictLogging {
  val appName: String = "spark-stream-job"
  println("!" + appName)

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

  val lines = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "flink-ml-in")
    .load()

  println(lines.isStreaming)    // Returns True for DataFrames that have streaming sources

  lines.printSchema

  // Split the lines into words
  val words = lines.select("value").as[String].flatMap(_.split(","))

  // Generate running word count
  val wordCounts: DataFrame = words.groupBy("value").count().sort($"count".desc)

  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()
}
