package com.amadeus.dataio.config.fields

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RepartitionConfiguratorTest extends AnyWordSpec with Matchers {
  "getRepartitionNumber" should {
    "return 5" when {
      "given RepartitioningNumber = 5" in {
        val config = ConfigFactory.parseMap(
          Map("RepartitioningNumber" -> 5)
        )

        getRepartitionNumber(config) shouldBe 5
      }

      "given Repartition{ Number = 5 }" in {
        val config = ConfigFactory.parseMap(
          Map("Repartition" -> Map("Number" -> 5))
        )

        getRepartitionNumber(config) shouldBe 5
      }
    }

    "throw a ConfigException" when {
      "given a Config without RepartitioningNumber or Repartition.Number" in {
        intercept[ConfigException] {
          val config = ConfigFactory.empty()

          getRepartitionNumber(config)
        }
      }
    }
  }

  "getRepartitionColumn" should {
    "return colName" when {
      "given RepartitioningColumn = colName" in {
        val config = ConfigFactory.parseMap(
          Map("RepartitioningColumn" -> "colName")
        )

        getRepartitionColumn(config) shouldBe "colName"
      }

      "given Repartition{ Column = colName }" in {
        val config = ConfigFactory.parseMap(
          Map("Repartition" -> Map("Column" -> "colName"))
        )

        getRepartitionColumn(config) shouldBe "colName"
      }
    }

    "throw a ConfigException" when {
      "given a Config without RepartitioningColumn or Repartition.Column" in {
        intercept[ConfigException] {
          val config = ConfigFactory.empty()

          getRepartitionColumn(config)
        }
      }
    }
  }
}
