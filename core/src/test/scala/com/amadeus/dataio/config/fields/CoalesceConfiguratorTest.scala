package com.amadeus.dataio.config.fields

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CoalesceConfiguratorTest extends AnyFlatSpec with Matchers {

  behavior of "getCoalesceNumber"
  it should "return the coalesce number when it exists in config" in {
    val configStr =
      """
    coalesce = 10
  """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    getCoalesceNumber should be(Some(10))
  }

  it should "return None when coalesce number doesn't exist in config" in {
    val configStr =
      """
    some_other_config = 10
  """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    getCoalesceNumber should be(None)
  }
}
