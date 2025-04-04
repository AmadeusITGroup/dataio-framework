package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

import scala.util.Try
import scala.collection.JavaConverters._

trait SortWithinPartitionsConfigurator {

  /** @param config The typesafe Config object holding the configuration.
    * @return The sequence of expressions to use with `sortWithinPartitions(...)`.
    */
  def getSortWithinPartitionsExprs(implicit config: Config): Seq[String] = {
    Try {
      config.getString("sort_within_partitions.exprs").split(",").map(_.trim).toSeq
    } orElse Try {
      config.getStringList("sort_within_partitions.exprs").asScala
    } getOrElse {
      Seq[String]()
    }
  }
}
