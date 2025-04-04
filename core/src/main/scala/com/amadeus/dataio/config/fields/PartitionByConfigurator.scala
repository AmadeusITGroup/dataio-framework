package com.amadeus.dataio.config.fields

import com.amadeus.dataio.core.Logging
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.util.Try

trait PartitionByConfigurator extends Logging {

  /** @param config The typesafe Config object holding the configuration.
    * @return A sequence of the column names to partition by.
    */
  def getPartitionByColumns(implicit config: Config): Seq[String] =
    Try {
      config.getString("partition_by").split(",").map(_.trim).toList
    } orElse Try {
      config.getStringList("partition_by").asScala
    } getOrElse {
      Seq[String]()
    }
}
