package com.amadeus.dataio.config.fields

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TimeoutConfiguratorTest extends AnyFlatSpec with Matchers {

  behavior of "getTimeout"
  it should "return timeout in milliseconds when configured" in {
    val configStr =
      """
        timeout = "5 seconds"
      """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getTimeout
    result should be(Some(5000))
  }

  it should "return None when timeout not configured" in {
    val configStr =
      """
        some_other_config = "value"
      """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getTimeout
    result should be(None)
  }

  it should "return None when timeout format is invalid" in {
    val configStr =
      """
        timeout = "invalid format"
      """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getTimeout
    result should be(None)
  }
}
