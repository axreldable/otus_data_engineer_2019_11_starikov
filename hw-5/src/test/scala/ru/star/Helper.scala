package ru.star

import java.nio.file.Paths

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object Helper {
  def resourceAbsolutePath(resource: String): String = {
    try {
      Paths.get(getClass.getClassLoader.getResource(resource).toURI).toFile.getAbsolutePath
    } catch {
      case _: NullPointerException => throw new NullPointerException(s"Perhaps resource $resource not found.")
    }
  }

  def expectedStringJsons()(implicit spark: SparkSession): RDD[String] = {
    val data = Seq(
      "{\"country\":\"France\",\"points\":92,\"title\":\"Château d'Arche 2011  Sauternes\",\"variety\":\"Bordeaux-style White Blend\",\"winery\":\"Château d'Arche\"}",
      "{\"id\":1747,\"points\":92,\"price\":49.0,\"title\":\"Château Gaudin 2011  Pauillac\",\"variety\":\"Bordeaux-style Red Blend\",\"winery\":\"Château Gaudin\"}",
      "{\"id\":1748,\"country\":\"France\",\"title\":\"Château Phélan-Ségur 2012  Saint-Estèphe\",\"variety\":\"Bordeaux-style Red Blend\",\"winery\":\"Château Phélan-Ségur\"}",
      "{\"id\":1749,\"country\":\"US\",\"points\":92,\"title\":\"Darcie Kent Vineyards 2012 De Mayo Block Chardonnay (Livermore Valley)\",\"variety\":\"Chardonnay\",\"winery\":\"Darcie Kent Vineyards\"}",
      "{\"id\":1750,\"country\":\"France\",\"points\":84,\"price\":13.0,\"variety\":\"Bordeaux-style Red Blend\",\"winery\":\"Univitis\"}",
      "{\"id\":1751,\"country\":\"Spain\",\"points\":84,\"price\":12.0,\"title\":\"Volver 2014 Tarima Sparkling (Alicante)\",\"winery\":\"Volver\"}",
      "{\"id\":1752,\"country\":\"US\",\"points\":84,\"price\":10.0,\"title\":\"Washington Hills 2014 Late Harvest Riesling (Washington)\",\"variety\":\"Riesling\"}"
    )

    spark.sparkContext.parallelize(data)
  }

  def expectedWines(): Seq[Wine] = {
    Seq(
      Wine(None, Some("France"), Some(92), None, Some("Château d'Arche 2011  Sauternes"), Some("Bordeaux-style White Blend"), Some("Château d'Arche")),
      Wine(Some(1747), None, Some(92), Some(49.0), Some("Château Gaudin 2011  Pauillac"), Some("Bordeaux-style Red Blend"), Some("Château Gaudin"))
    )
  }
}
