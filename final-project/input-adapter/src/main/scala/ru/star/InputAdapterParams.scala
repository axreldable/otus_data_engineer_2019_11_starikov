package ru.star

import java.util.Properties

final case class Parameters(kafkaConsumerProperties: Properties)

object Parameters {
  def apply(params: InputAdapterParams): Parameters = {
    val kafkaConsumerProperties = new Properties()
    kafkaConsumerProperties.put("bootstrap.servers", Config.getValue(params.inputBootstrapServers, "InputKafka"))


    new Parameters(kafkaConsumerProperties)
  }

  def apply(inputArgs: Array[String]): Parameters = apply(InputAdapterParams(inputArgs))
}

final case class InputAdapterParams(inputBootstrapServers: Option[String] = None)

object InputAdapterParams {
  private lazy val parser = new scopt.OptionParser[InputAdapterParams]("input-adapter-params") {

    opt[String]("input-bootstrap-servers") action {
      (v, c) => c copy (inputBootstrapServers = Some(v))
    } text "input-bootstrap-servers"
  }

  def apply(inputArgs: Array[String]): InputAdapterParams = parser
    .parse(inputArgs, InputAdapterParams())
    .getOrElse {
      throw new Exception(s"Failed to parse application arguments: ${inputArgs.mkString(",")}.")
    }
}

