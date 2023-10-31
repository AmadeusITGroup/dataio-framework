package com.amadeus.dataio.config.fields

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TimeoutConfiguratorTest extends AnyWordSpec with Matchers {

  "getTimeout" should {
    "return timeout in millisecond" when {

      "given implicit duration in hour (as string)" in {
        val config = ConfigFactory.parseMap(
          Map("Timeout" -> "2")
        )

        getTimeout(config) shouldEqual 7200000
      }

      "given implicit duration in hour (as int)" in {
        val config = ConfigFactory.parseMap(
          Map("Timeout" -> 2)
        )

        getTimeout(config) shouldEqual 7200000
      }

      "given explicit duration in hours" in {
        val config = ConfigFactory.parseMap(
          Map("Timeout" -> "4 hours")
        )

        getTimeout(config) shouldEqual 14400000
      }

      "given explicit duration in minutes" in {
        val config = ConfigFactory.parseMap(
          Map("Timeout" -> "3 minutes")
        )

        getTimeout(config) shouldEqual 180000
      }

      "given explicit duration in seconds" in {
        val config = ConfigFactory.parseMap(
          Map("Timeout" -> "1 seconds")
        )

        getTimeout(config) shouldEqual 1000
      }

      "given explicit duration in milliseconds" in {
        val config = ConfigFactory.parseMap(
          Map("Timeout" -> "5 milliseconds")
        )

        getTimeout(config) shouldEqual 5
      }
    }
  }
}
