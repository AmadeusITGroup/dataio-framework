package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

import scala.util.Try

import scala.collection.JavaConverters._

trait DropDuplicatesConfigurator {

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return The list of column names to use with `dropDuplicates(...)`.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getDropDuplicatesColumns(implicit config: Config): Seq[String] = {
    Try {
      config.getString("DropDuplicates.Columns").split(",").map(_.trim).toList
    } getOrElse {
      config.getStringList("DropDuplicates.Columns").asScala
    }
  }

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return `true` if `dropDuplicates(...)` should be applied.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getDropDuplicatesActive(implicit config: Config): Boolean = {
    config.getBoolean("DropDuplicates.Active")
  }
}
