package com.amadeus.dataio.config.fields

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PartitionByConfiguratorTest extends AnyWordSpec with Matchers {
  "getPartitionByColumns" should {
    "return Seq(col, col2, col3)" when {
      "given PartitionBy = col1, col2, col3" in {
        val config = ConfigFactory.parseMap(
          Map("PartitionBy" -> "col1, col2, col3")
        )

        getPartitionByColumns(config) should contain theSameElementsAs Seq("col1", "col2", "col3")
      }

      "given PartitionBy = [col1, col2, col3]" in {
        val config = ConfigFactory.parseMap(
          Map("PartitionBy" -> Seq("col1", "col2", "col3"))
        )

        getPartitionByColumns(config) should contain theSameElementsAs Seq("col1", "col2", "col3")
      }

      "given PartitioningColumn = col1, col2, col3" in {
        val config = ConfigFactory.parseMap(
          Map("PartitioningColumn" -> "col1, col2, col3")
        )

        getPartitionByColumns(config) should contain theSameElementsAs Seq("col1", "col2", "col3")
      }


    }

    "return an empty Sequence" when {
      "given a missing input" in {
        val config = ConfigFactory.parseMap(Map[String, String]())

        getPartitionByColumns(config) should contain theSameElementsAs Seq[String]()
      }
    }
  }
}
