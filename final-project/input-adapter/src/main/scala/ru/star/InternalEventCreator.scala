package ru.star

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

class InternalEventCreator extends RichMapFunction[String, InternalEvent] {

  override def open(config: Configuration): Unit = {

    // access cached file via RuntimeContext and DistributedCache
    val myFile: File = getRuntimeContext.getDistributedCache.getFile("hdfsFile")
    // read the file (or navigate the directory)
    ...
  }

  override def map(value: String): Int = {
    // use content of cached file
    ...
  }
}