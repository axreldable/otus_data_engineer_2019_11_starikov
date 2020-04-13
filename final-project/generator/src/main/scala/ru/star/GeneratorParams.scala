package ru.star

import scopt.OptionParser


case class GeneratorParams(
                            bootstrapServices: String = null
                          )

object GeneratorParams {
  private lazy val parser: OptionParser[GeneratorParams] = new scopt.OptionParser[GeneratorParams](
    "generator"
  ) {

    opt[String]("bootstrap-servers") required() action {
      (v, c) => c copy (bootstrapServices = v)
    } text "bootstrap.servers string, ex: localhost:9092,kafka:9093"

  }

  def apply(inputArgs: Array[String]): GeneratorParams = parser
    .parse(inputArgs, GeneratorParams())
    .get
}
