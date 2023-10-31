package com.amadeus.dataio.config.fields

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DropDuplicatesConfiguratorTest extends AnyWordSpec with Matchers {

  "getDropDuplicatesColumns" should {
    "return Seq(col1, col2)" when {
      "given DropDuplicates.Columns = col1, col2" in {
        val config = ConfigFactory.parseMap(
          Map("DropDuplicates.Columns" -> "col1, col2")
        )
        getDropDuplicatesColumns(config) should contain theSameElementsAs Seq("col1", "col2")
      }

      "given DropDuplicates.Columns = [col1, col2]" in {
        val config = ConfigFactory.parseMap(
          Map("DropDuplicates.Columns" -> Seq("col1", "col2"))
        )

        getDropDuplicatesColumns(config) should contain theSameElementsAs Seq("col1", "col2")
      }
    }

    "throw a ConfigException" when {
      "given a Config without DropDuplicates.Columns" in {
        intercept[ConfigException] {
          val config = ConfigFactory.empty()

          getDropDuplicatesColumns(config)
        }
      }
    }
  }

  "getDropDuplicatesActive" should {
    "return true" when {
      "given DropDuplicates.Active = true" in {
        val config = ConfigFactory.parseMap(
          Map("DropDuplicates.Active" -> true)
        )

        getDropDuplicatesActive(config) shouldBe true
      }
    }

    "throw a ConfigException" when {
      "given a Config without DropDuplicates.Active" in {
        intercept[ConfigException] {
          val config = ConfigFactory.empty()

          getDropDuplicatesActive(config)
        }
      }
    }
  }
}
