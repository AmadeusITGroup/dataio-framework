package com.amadeus.dataio.config.fields

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PartitionByConfiguratorTest extends AnyFlatSpec with Matchers {
  behavior of "getPartitionByColumns"
  "getPartitionByColumns" should "parse comma-separated string values" in {
    val configStr =
      """
       partition_by = "year, month, day"
     """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getPartitionByColumns
    result should be(List("year", "month", "day"))
  }

  it should "parse string list values" in {
    val configStr =
      """
       partition_by = ["year", "month", "day"]
     """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getPartitionByColumns
    result should be(List("year", "month", "day"))
  }

  it should "return empty sequence when partition_by doesn't exist" in {
    val configStr =
      """
       some_other_config = "value"
     """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getPartitionByColumns
    result should be(Seq())
  }
}
