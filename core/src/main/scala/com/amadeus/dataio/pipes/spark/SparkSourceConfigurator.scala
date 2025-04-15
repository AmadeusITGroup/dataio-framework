package com.amadeus.dataio.pipes.spark

import com.amadeus.dataio.config.fields.getPath
import com.typesafe.config.Config

import scala.util.Try

trait SparkSourceConfigurator {
  def getSparkSource(implicit config: Config): Option[SparkSource] = {
    def getOptionalString(key: String) = Try(config.getString(key)).toOption

    val tableConfig = getOptionalString("table").map(SparkTableSource)

    val pathConfig = getPath.flatMap { path =>
      val format = getOptionalString("format")
      Some(SparkPathSource(path, format))
    }

    if (tableConfig.isDefined && pathConfig.isDefined) {
      throw new Exception("Both `table` and `path` are defined. Please specify only one.")
    }

    tableConfig.orElse(pathConfig)
  }
}
