package ru.star

import com.typesafe.scalalogging._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._


object StructuredConsumer extends App with StrictLogging {
  val appName: String = "structured-consumer-example"

  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .master("local[2]")
    .getOrCreate()
  import spark.implicits._
  val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "kafka:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("tweet-topic-1")
  val stream = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

//  stream.map(record => {
//    logger.info(s"Key: ${record.key().toString}, Value: ${record.value().toString}")
//  })
//  stream.print()
  stream.foreachRDD { rdd =>
    rdd.foreach { record =>
      val value = record.value()
      println(s"Message: $value")
    }
  }

  streamingContext.start()
  streamingContext.awaitTermination()
}
