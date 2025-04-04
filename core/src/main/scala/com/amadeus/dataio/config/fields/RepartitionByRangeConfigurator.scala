package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

import scala.util.Try

trait RepartitionByRangeConfigurator {

  /** @param config The typesafe Config object holding the configuration.
    * @return The number of partitions to use with `repartitionByRange(...)`, or None.
    */
  def getRepartitionByRangeNum(implicit config: Config): Option[Int] = {
    Try {
      config.getInt("repartition_by_range.num")
    }.toOption
  }

  /** @param config The typesafe Config object holding the configuration.
    * @return The expressions to use with `repartitionByRange(...)`, or None.
    */
  def getRepartitionByRangeExprs(implicit config: Config): Option[String] =
    Try {
      config.getString("repartition_by_range.exprs")
    }.toOption
}
