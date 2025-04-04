package com.amadeus.dataio.config.fields

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SortWithinPartitionsConfiguratorTest extends AnyFlatSpec with Matchers {
  behavior of "getSortWithinPartitionsExprs"

  it should "parse comma-separated string values" in {
    val configStr =
      """
        sort_within_partitions.exprs = "id, timestamp, value"
      """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getSortWithinPartitionsExprs
    result should be(Seq("id", "timestamp", "value"))
  }

  it should "parse string list values" in {
    val configStr =
      """
        sort_within_partitions.exprs = ["id", "timestamp", "value"]
      """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getSortWithinPartitionsExprs
    result should be(Seq("id", "timestamp", "value"))
  }

  it should "return empty sequence when expressions don't exist" in {
    val configStr =
      """
        some_other_config = "value"
      """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getSortWithinPartitionsExprs
    result should be(Seq())
  }
}
