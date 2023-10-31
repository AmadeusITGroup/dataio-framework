package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

trait RepartitionByRangeConfigurator {

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return The number of partitions to use with `repartitionByRange(...)`.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getRepartitionByRangeNumber(implicit config: Config): Int = {
    config.getInt("RepartitionByRange.Number")
  }

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return The column name to use with `repartitionByRange(...)`.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getRepartitionByRangeColumn(implicit config: Config): String =
    config.getString("RepartitionByRange.Column")
}
