package ru.star

import com.typesafe.scalalogging._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery}


object SparkTweetJob extends App with StrictLogging {
  val appName: String = "spark-stream-job"
  println("!" + appName)
  println(System.getenv("MONGO_SERVER"))

  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
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

  println(lines.isStreaming)

  lines.printSchema

  val words = lines.select("value").as[String].flatMap(_.split(","))
    .withColumnRenamed("value", "tweet")

  val trainedModel = PipelineModel.load("/Users/axreldable/Desktop/projects/otus/data-engineer/otus_data_engineer_2019_11_starikov/final-project/spark-tweet-job/models/spark-ml-model-rf")
  val getProbabilityNegativeTweet = udf((prediction: org.apache.spark.ml.linalg.Vector) => prediction(1))
  val getProbabilityPositiveTweet = udf((prediction: org.apache.spark.ml.linalg.Vector) => prediction(0))

  val predictionDf = trainedModel.transform(words)
    .withColumn("probability_negative", getProbabilityNegativeTweet($"probability"))
    .withColumn("probability_positive", getProbabilityPositiveTweet($"probability"))
    .withColumn("is_positive", when($"probability_positive" > 0.5, 1).otherwise(0))
    .select("tweet", "prediction")
    .withColumn("rez", concat(col("tweet"), lit(","), col("prediction")))
    .withColumnRenamed("rez", "value")
    .select("value")

//  val rez: DataStreamWriter[Row] = predictionDf.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
//    batchDF
//      .withColumn("probability_negative", getProbabilityNegativeTweet($"probability"))
//      .withColumn("probability_positive", getProbabilityPositiveTweet($"probability"))
//      .withColumn("is_positive", when($"probability_positive" > 0.5, 1).otherwise(0))
//  }

//  predictionDf
//    .writeStream
//    .outputMode("append")
//    .format("console")
//    .start()

  predictionDf
    .writeStream
    .outputMode("append")
    .format("kafka")
    .option("checkpointLocation","checkpoints/spark-tweet-job")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "target-topic-1")
    .start()


  spark.streams.awaitAnyTermination()
}
