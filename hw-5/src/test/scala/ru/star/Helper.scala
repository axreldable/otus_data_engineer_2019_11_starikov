package ru.star

import java.nio.file.Paths


object Helper {
  def resourceAbsolutePath(resource: String): String = {
    try {
      Paths.get(getClass.getClassLoader.getResource(resource).toURI).toFile.getAbsolutePath
    } catch {
      case _: NullPointerException => throw new NullPointerException(s"Perhaps resource $resource not found.")
    }
  }
}
