package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

import scala.util.Try

trait RepartitionConfigurator {

  /** @param config The typesafe Config object holding the configuration.
    * @return The number of partitions to use with `repartition(...)`, or None.
    */
  def getRepartitionNum(implicit config: Config): Option[Int] = {
    Try {
      config.getInt("repartition.num")
    }.toOption
  }

  /** @param config The typesafe Config object holding the configuration.
    * @return The column expression to use with `repartition(...)`, or None.
    */
  def getRepartitionExprs(implicit config: Config): Option[String] = {
    Try {
      config.getString("repartition.exprs")
    }.toOption
  }
}
