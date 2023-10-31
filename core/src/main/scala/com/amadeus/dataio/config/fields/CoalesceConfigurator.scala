package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

import scala.util.Try

trait CoalesceConfigurator {

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return The number of partitions.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getCoalesceNumber(implicit config: Config): Int = {
    Try {
      config.getInt("Coalesce")
    } getOrElse {
      config.getInt("Coalesce.Number")
    }
  }
}
