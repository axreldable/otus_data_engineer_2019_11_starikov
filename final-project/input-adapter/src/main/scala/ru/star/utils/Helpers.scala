package ru.star.utils

object Helpers {
  /**
    * Gets an absolute path to the file in the resources directory
    *
    * @param resource - a path to the file in the resources directory
    * @return an absolute path to the file in the resources directory
    */
  def resourceAbsolutePath(resource: String): String = {
    try {
      getClass.getClassLoader.getResource(resource).getPath
    } catch {
      case _: NullPointerException => throw new NullPointerException(s"Perhaps resource $resource not found.")
    }
  }
}
