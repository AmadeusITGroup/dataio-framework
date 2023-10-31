package com.amadeus.dataio.config.fields

import com.amadeus.dataio.core.Logging
import com.typesafe.config.Config

import scala.util.Try
import scala.collection.JavaConverters._

trait SortWithinPartitionsConfigurator extends Logging {

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return The sequence of column names to use with `sortWithinPartitions(...)`.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getSortWithinPartitionsColumns(implicit config: Config): Seq[String] = {
    Try {
      val attempt = config.getString("SortWithinPartitionColumn").split(",").map(_.trim).toSeq // retro compatibility
      logger.warn("Configuration field SortWithinPartitionColumn is deprecated. Please use SortWithinPartitions.Columns (String[]) instead.")
      attempt

    } orElse {
      Try {
        config.getString("SortWithinPartitions.Columns").split(",").map(_.trim).toSeq
      }
    } getOrElse {
      config.getStringList("SortWithinPartitions.Columns").asScala
    }
  }
}
