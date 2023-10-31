package com.amadeus.dataio.config.fields

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CoalesceConfiguratorTest extends AnyWordSpec with Matchers {

  "getCoalesceNumber" should {
    "return 5" when {
      "given Coalesce = 5" in {
        val config = ConfigFactory.parseMap(
          Map("Coalesce" -> 5)
        )

        getCoalesceNumber(config) shouldBe 5
      }

      "given Coalesce{ Number = 5 }" in {
        val config = ConfigFactory.parseMap(
          Map("Coalesce" -> Map("Number" -> 5))
        )

        getCoalesceNumber(config) shouldBe 5
      }
    }

    "throw a ConfigException" when {
      "given a Config without Coalesce or Coalesce.Number" in {
        intercept[ConfigException] {
          val config = ConfigFactory.empty()

          getCoalesceNumber(config)
        }
      }
    }
  }
}
