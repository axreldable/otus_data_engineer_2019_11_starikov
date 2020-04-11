package ru.star

import com.typesafe.scalalogging._

object GeneratorApp extends App with StrictLogging {

  val stringProducer = Helpers.initStringProducer("localhost:9092")
  val modelProducer = Helpers.initModelProducer("localhost:9092")

  Helpers.sendModel("ml-stream-pmml-model-in", modelProducer)

  Helpers.sendMessages(
    tweetSource = "/tmp/data/training.1600000.processed.noemoticon.csv",
    tweetTopic = "ml-stream-input-adapter-message-in",
    irisTopic = "ml-stream-input-adapter-message-in",
    producer = stringProducer
  )
}
