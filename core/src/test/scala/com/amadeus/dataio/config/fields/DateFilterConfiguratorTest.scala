package com.amadeus.dataio.config.fields

import com.amadeus.dataio.core.time.DateRange
import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.apache.spark.sql.functions.col
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.immutable.Map

class DateFilterConfiguratorTest extends AnyWordSpec with Matchers {

  val expectedFromDate = LocalDate.parse("2022-01-01", DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay()
  val expectedUntilDate = LocalDate.parse("2022-01-08", DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay()

  "getDateFilterRange" should {
    "return DateRange(2022-01-01, +7D)" when {


      "given DateReference = 2022-01-01, DateOffset = +7D" in {
        val config = ConfigFactory.parseMap(
          Map("DateReference" -> "2022-01-01", "DateOffset" -> "+7D")
        )

        val result = getDateFilterRange(config)

        result.from shouldBe expectedFromDate
        result.until shouldBe expectedUntilDate
      }

      "given DateFilter{Reference = 2022-01-01, Offset = +7D}" in {
        val config = ConfigFactory.parseMap(
          Map("DateFilter" -> Map("Reference" -> "2022-01-01", "Offset" -> "+7D"))
        )

        val result = getDateFilterRange(config)

        result.from shouldBe expectedFromDate
        result.until shouldBe expectedUntilDate
      }
    }

    "throw a ConfigException" when {
      "given a Config without DateReference and DateOffset and DateFilter.Reference and DateFilter.Offset " in {
        intercept[ConfigException] {
          val config = ConfigFactory.empty()

          getDateFilterRange(config)
        }
      }

      "given DateReference without DateOffset" in {
        intercept[IllegalArgumentException] {
          val config = ConfigFactory.parseMap(
            Map("DateReference" -> "2022-01-01")
          )

          getDateFilterRange(config)
        }
      }

      "given DateOffset without DateReference" in {
        intercept[IllegalArgumentException] {
          val config = ConfigFactory.parseMap(
            Map("DateOffset" -> "+7D")
          )

          getDateFilterRange(config)
        }
      }

      "given DateFilter.Reference without DateFilter.Offset" in {
        intercept[IllegalArgumentException] {
          val config = ConfigFactory.parseMap(
            Map("DateFilter.Reference" -> "2022-01-01")
          )

          getDateFilterRange(config)
        }
      }

      "given DateFilter.Offset without DateFilter.Reference" in {
        intercept[IllegalArgumentException] {
          val config = ConfigFactory.parseMap(
            Map("DateFilter.Offset" -> "+7D")
          )

          getDateFilterRange(config)
        }
      }
    }
  }


  "testArguments" should {
    "be true if both reference and offset are in the configuration" in {
      val config = ConfigFactory.parseMap(
        Map("DateReference" -> "2022-01-01",
          "DateOffset" -> "+7D")
      )

      testArguments("DateReference", "DateOffset")(config) shouldBe true
    }

    "be true if neither reference nor offset are in the configuration" in {
      val config = ConfigFactory.parseMap(
        Map("random" -> "2022-01-01")
      )

      testArguments("DateReference", "DateOffset")(config) shouldBe true
    }

    "be false if only reference is in the configuration" in {
      val config = ConfigFactory.parseMap(
        Map("DateReference" -> "2022-01-01")
      )

      testArguments("DateReference", "DateOffset")(config) shouldBe false
    }

    "be false if only offset is in the configuration" in {
      val config = ConfigFactory.parseMap(
        Map("DateOffset" -> "+7D")
      )

      testArguments("DateReference", "DateOffset")(config) shouldBe false
    }
  }

  "getArguments" should {

    val expectedDateRange = DateRange(expectedFromDate, expectedUntilDate)

    "return a DateRange if both reference and offset are in the configuration" in {
      val config = ConfigFactory.parseMap(
        Map("DateReference" -> "2022-01-01",
          "DateOffset" -> "+7D")
      )

      getArguments("DateReference", "DateOffset")(config) shouldEqual expectedDateRange
    }

    "throw if both reference and offset are missing from configuration" in {
      intercept[ConfigException] {
        val config = ConfigFactory.parseMap(
          Map("random" -> "2022-01-01")
        )

        getArguments("DateReference", "DateOffset")(config) shouldEqual expectedDateRange
      }
    }

    "throw if reference is missing from configuration" in {
      intercept[ConfigException] {
        val config = ConfigFactory.parseMap(
          Map("DateOffset" -> "+7D")
        )

        getArguments("DateReference", "DateOffset")(config) shouldEqual expectedDateRange
      }
    }

    "throw if offset is missing from configuration" in {
      intercept[ConfigException] {
        val config = ConfigFactory.parseMap(
          Map("DateReference" -> "2022-01-01")
        )

        getArguments("DateReference", "DateOffset")(config) shouldEqual expectedDateRange
      }
    }
  }

  "getDateFilterColumn" should {
    "return col(dateCol)" when {
      val expectedCol = col("dateCol")

      "given DateColumn = dateCol" in {
        val config = ConfigFactory.parseMap(
          Map("DateColumn" -> "dateCol")
        )

        getDateFilterColumn(config) shouldBe expectedCol
      }

      "given DateFilter.Column = dateCol" in {
        val config = ConfigFactory.parseMap(
          Map("DateFilter" -> Map("Column" -> "dateCol"))
        )

        getDateFilterColumn(config) shouldBe expectedCol
      }
    }

    "throw a ConfigException" when {
      "given a Config without DateFilterColumn and DateFilter.Column" in {
        intercept[ConfigException] {
          val config = ConfigFactory.empty()

          getDateFilterColumn(config)
        }
      }
    }
  }
}
