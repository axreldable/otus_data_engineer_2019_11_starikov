package ru.star

import org.apache.flink.api.java.utils.ParameterTool

trait EnsureParameters {
  def ensureParams(params: ParameterTool): (String, String) = {
    (params.getRequired("model"), params.getRequired("output"))
  }
}
