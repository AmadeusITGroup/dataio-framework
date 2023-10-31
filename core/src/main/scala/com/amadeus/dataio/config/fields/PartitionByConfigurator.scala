package com.amadeus.dataio.config.fields

import com.amadeus.dataio.core.Logging
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.util.Try

trait PartitionByConfigurator extends Logging {

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return A sequence of the column names to partition by.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getPartitionByColumns(implicit config: Config): Seq[String] =
    Try {
      val attempt = config.getString("PartitioningColumn").split(",").map(_.trim).toList // retro compatibility
      logger.warn("Configuration field PartitioningColumn is deprecated. Please use PartitionBy (String[]) instead.")
      attempt
    } orElse {
      Try { config.getString("PartitionBy").split(",").map(_.trim).toList }
    } orElse {
      Try { config.getStringList("PartitionBy").asScala }
    } getOrElse {
      Seq[String]()
    }
}
