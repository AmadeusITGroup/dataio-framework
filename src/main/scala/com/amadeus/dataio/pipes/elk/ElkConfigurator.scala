package com.amadeus.dataio.pipes.elk

import com.typesafe.config.Config

import scala.util.Try

/**
 * Elasticsearch parameterization
 */
object ElkConfigurator {

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return A String of the index to use.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getIndex(implicit config: Config): String = {
    config.getString("Index")
  }

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return A String of the date field to be used for sub index partitioning.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getDateField(implicit config: Config): String = {
    config.getString("DateField")
  }

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return A Option[String] of the date pattern to use for sub index partitioning.
   */
  def getSubIndexDatePattern(implicit config: Config): Option[String] = {
    Try(config.getString("SubIndexDatePattern")).toOption
  }
}
