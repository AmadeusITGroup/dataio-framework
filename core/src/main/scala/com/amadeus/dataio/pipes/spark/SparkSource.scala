package com.amadeus.dataio.pipes.spark

sealed trait SparkSource

case class SparkTableSource(table: String) extends SparkSource

case class SparkPathSource(path: String, format: Option[String]) extends SparkSource
