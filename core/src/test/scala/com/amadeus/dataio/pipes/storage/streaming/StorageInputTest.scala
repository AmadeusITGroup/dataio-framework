package com.amadeus.dataio.pipes.storage.streaming

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.col
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate

class StorageInputTest extends AnyWordSpec with Matchers {

  "StorageInput" should {
    "be initialized according to configuration" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Input" -> Map(
            "Type"   -> "com.amadeus.dataio.pipes.storage.streaming.StorageInput",
            "Name"   -> "my-test-stream",
            "Path"   -> "input/fileStreamInputConfigTest.csv",
            "Format" -> "csv",
            "Options" -> Map(
              "delimiter" -> ";"
            )
          )
        )
      )

      val fileStreamInput = StorageInput.apply(config.getConfig("Input"))

      fileStreamInput.path shouldEqual "input/fileStreamInputConfigTest.csv"
      fileStreamInput.format.get shouldEqual "csv"
      fileStreamInput.options shouldEqual Map("delimiter" -> ";")
      fileStreamInput.coalesce should be(None)
      fileStreamInput.dateRange should be(None)
      fileStreamInput.dateColumn should be(None)
      fileStreamInput.repartitionColumn should be(None)
      fileStreamInput.repartitionNumber should be(None)
    }

    "be initialized with all optional properties" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Input" -> Map(
            "Type"                 -> "com.amadeus.dataio.pipes.storage.streaming.StorageInput",
            "Name"                 -> "my-test-stream",
            "Path"                 -> "input/fileStreamInputConfigTest.csv",
            "Format"               -> "csv",
            "DateReference"        -> "2022-01-01",
            "DateOffset"           -> "+7D",
            "DateColumn"           -> "dateCol",
            "RepartitioningColumn" -> "repCol",
            "RepartitioningNumber" -> "100",
            "Coalesce"             -> "10",
            "Options" -> Map(
              "delimiter" -> ";"
            )
          )
        )
      )

      val fileStreamInput = StorageInput.apply(config.getConfig("Input"))

      val startDateRange = LocalDate.of(2022, 1, 1).atStartOfDay()
      val endDateRange   = LocalDate.of(2022, 1, 8).atStartOfDay()

      fileStreamInput.path shouldEqual "input/fileStreamInputConfigTest.csv"
      fileStreamInput.format.get shouldEqual "csv"
      fileStreamInput.options shouldEqual Map("delimiter" -> ";")
      fileStreamInput.coalesce.get shouldEqual 10
      fileStreamInput.dateRange.get.from shouldEqual startDateRange
      fileStreamInput.dateRange.get.until shouldEqual endDateRange
      fileStreamInput.dateColumn.get shouldEqual col("dateCol")
      fileStreamInput.repartitionColumn.get shouldEqual "repCol"
      fileStreamInput.repartitionNumber.get shouldEqual 100
    }


    "catch a missing Offset" in {

      intercept[IllegalArgumentException] {
        val config = ConfigFactory.parseMap(
          Map(
            "Input" -> Map(
              "Type" -> "com.amadeus.dataio.pipes.storage.streaming.StorageInput",
              "Name" -> "my-test-stream",
              "Path" -> "input/fileStreamInputConfigTest.csv",
              "Format" -> "csv",
              "DateReference" -> "2022-01-01",
              "DateColumn" -> "dateCol",
              "RepartitioningColumn" -> "repCol",
              "RepartitioningNumber" -> "100",
              "Coalesce" -> "10",
              "Options" -> Map(
                "delimiter" -> ";"
              )
            )
          )
        )

        StorageInput.apply(config.getConfig("Input"))
      }
    }
  }
}
