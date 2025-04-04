package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

import scala.util.Try

trait CoalesceConfigurator {

  /** @param config The typesafe Config object holding the configuration.
    * @return The number of partitions, or None.
    */
  def getCoalesceNumber(implicit config: Config): Option[Int] = {
    Try {
      config.getInt("coalesce")
    }.toOption
  }
}
