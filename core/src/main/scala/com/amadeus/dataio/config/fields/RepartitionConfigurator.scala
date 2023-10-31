package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

import scala.util.Try

trait RepartitionConfigurator {

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return The number of partitions to use with `repartition(...)`.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getRepartitionNumber(implicit config: Config): Int = {
    Try {
      config.getInt("RepartitioningNumber")
    } getOrElse {
      config.getInt("Repartition.Number")
    }
  }

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return The column expression to use with `repartition(...)`.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getRepartitionColumn(implicit config: Config): String = {
    Try {
      config.getString("RepartitioningColumn")
    }.getOrElse {
      config.getString("Repartition.Column")
    }
  }
}
