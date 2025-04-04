package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

import scala.util.Try

import scala.collection.JavaConverters._

trait DropDuplicatesConfigurator {

  /** @param config The typesafe Config object holding the configuration.
    * @return The list of column names to use with `dropDuplicates(...)`.
    */
  def getDropDuplicatesColumns(implicit config: Config): Seq[String] = {
    Try {
      config.getString("drop_duplicates").split(",").map(_.trim).toList
    } orElse Try {
      config.getStringList("drop_duplicates").asScala
    } getOrElse {
      Seq[String]()
    }
  }

  /** @param config The typesafe Config object holding the configuration.
    * @return `true` if `dropDuplicates(...)` should be applied.
    */
  def getDropDuplicatesActive(implicit config: Config): Boolean = {
    config.hasPath("drop_duplicates")
  }
}
