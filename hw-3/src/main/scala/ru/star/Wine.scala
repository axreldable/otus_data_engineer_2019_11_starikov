package ru.star

case class Wine(
                 id: Option[Int],
                 country: Option[String],
                 points: Option[Int],
                 price: Option[Double],
                 title: Option[String],
                 variety: Option[String],
                 winery: Option[String]
               )
