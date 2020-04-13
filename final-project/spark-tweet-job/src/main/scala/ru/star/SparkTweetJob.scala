package ru.star

import com.typesafe.scalalogging._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.streaming._


object SparkTweetJob extends App with StrictLogging {
  val appName: String = "spark-stream-job"

  val params = SparkTweetJobParams(args)
  println("params", params)

  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

  import spark.implicits._

  val lines = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", params.kafkaConfig.bootstrapServers)
    .option("subscribe", params.kafkaConfig.inputTopic)
    .option("failOnDataLoss", "false") // todo: read about it
    .load()

  println(lines.isStreaming)
  lines.printSchema
  println(s"modelPath:${params.modelPath}")

  val words: DataFrame = lines.select("value").as[String].flatMap(_.split(","))
    .withColumnRenamed("value", "tweet")

  val trainedModel: PipelineModel = PipelineModel.load(params.modelPath)
  val getProbabilityNegativeTweet = udf((prediction: org.apache.spark.ml.linalg.Vector) => prediction(1))
  val getProbabilityPositiveTweet = udf((prediction: org.apache.spark.ml.linalg.Vector) => prediction(0))

  val predictionDf = trainedModel.transform(words)
    .withColumn("probability_negative", getProbabilityNegativeTweet($"probability"))
    .withColumn("probability_positive", getProbabilityPositiveTweet($"probability"))
    .withColumn("is_positive", when($"probability_positive" > 0.5, 1).otherwise(0))

  predictionDf
    .select("tweet", "is_positive")
    .withColumn("value",
      concat(lit("tweet-type"), lit(params.separator), col("tweet"), lit(params.separator),
        col("is_positive").cast(StringType)))
    .select("value")
    .writeStream
    .outputMode("append")
    .format("kafka")
    .option("checkpointLocation", params.checkpointLocation)
    .option("kafka.bootstrap.servers", params.kafkaConfig.bootstrapServers)
    .option("topic", params.kafkaConfig.outputTopic)
    .start()

  spark.streams.awaitAnyTermination()
}
