package com.amadeus.dataio.config.fields

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SortWithinPartitionsConfiguratorTest extends AnyWordSpec with Matchers {
  "getSortWithinPartitionsColumns" should {
    "return Seq(col, col2, col3)" when {
      val expectedColumnNames = Seq("col1", "col2", "col3")

      "given SortWithinPartitions{ Columns = col1, col2, col3 }" in {
        val config = ConfigFactory.parseMap(
          Map("SortWithinPartitions" -> Map("Columns" -> "col1, col2, col3"))
        )
        getSortWithinPartitionsColumns(config) should contain theSameElementsAs expectedColumnNames
      }

      "given SortWithinPartitions{ Columns = [col1, col2, col3] }" in {
        val config = ConfigFactory.parseMap(
          Map("SortWithinPartitions" -> Map("Columns" -> Seq("col1", "col2", "col3")))
        )
        getSortWithinPartitionsColumns(config) should contain theSameElementsAs expectedColumnNames
      }

      "given SortWithinPartitionColumn = col1, col2, col3" in {
        val config = ConfigFactory.parseMap(
          Map("SortWithinPartitionColumn" -> "col1, col2, col3")
        )
        getSortWithinPartitionsColumns(config) should contain theSameElementsAs expectedColumnNames
      }
    }

    "throw a ConfigException" when {
      "given a Config without SortWithinPartitions.Columns or SortWithinPartitionColumn" in {
        intercept[ConfigException] {
          val config = ConfigFactory.empty()

          getSortWithinPartitionsColumns(config)
        }
      }
    }
  }
}
