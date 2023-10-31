package com.amadeus.dataio.config.fields

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RepartitionByRangeConfiguratorTest extends AnyWordSpec with Matchers {
  "getRepartitionByRangeNumber" should {
    "return 5" when {
      "given RepartitionByRange{ Number = 5 }" in {
        val config = ConfigFactory.parseMap(
          Map("RepartitionByRange" -> Map("Number" -> 5))
        )

        getRepartitionByRangeNumber(config) shouldBe 5
      }
    }

    "throw a ConfigException" when {
      "given a Config without RepartitionByRange.Number" in {
        intercept[ConfigException] {
          val config = ConfigFactory.empty()

          getRepartitionByRangeNumber(config)
        }
      }
    }
  }

  "getRepartitionByRangeColumn" should {
    "return colName" when {
      "given RepartitionByRange{ Column = colName }" in {
        val config = ConfigFactory.parseMap(
          Map("RepartitionByRange" -> Map("Column" -> "colName"))
        )

        getRepartitionByRangeColumn(config) shouldBe "colName"
      }
    }

    "throw a ConfigException" when {
      "given a Config without RepartitionByRange.Column" in {
        intercept[ConfigException] {
          val config = ConfigFactory.empty()

          getRepartitionByRangeColumn(config)
        }
      }
    }
  }
}
