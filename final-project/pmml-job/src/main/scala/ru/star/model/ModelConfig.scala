package ru.star.model

case class InnerModelConfig(modelId: String, version: Long, path: String)

case class ModelConfig(config: Map[String, InnerModelConfig])
