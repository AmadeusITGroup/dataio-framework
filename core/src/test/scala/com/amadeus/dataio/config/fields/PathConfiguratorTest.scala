package com.amadeus.dataio.config.fields

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.format.DateTimeFormatter
import scala.util.Try

class PathConfiguratorTest extends AnyWordSpec with Matchers {
  "getPath" should {
    "return file.csv" when {
      "given Path = file.csv" in {
        val config = ConfigFactory.parseMap(
          Map("Path" -> "file.csv")
        )
        getPath(config) shouldBe "file.csv"
      }

      "given Path{ Template = file.csv }" in {
        val config = ConfigFactory.parseMap(
          Map("Path" -> Map("Template" -> "file.csv"))
        )
        getPath(config) shouldBe "file.csv"
      }
    }

    "return file_20220101.csv" when {
      "given Path{ Date = 2022-01-01 , DateOffset = +5D, Template = file_%{from}.csv }" in {
        val config = ConfigFactory.parseMap(
          Map("Path" -> Map("Date" -> "2022-01-01", "DateOffset" -> "+5D", "Template" -> "file_%{from}.csv", "DatePattern" -> "yyyyMMdd"))
        )
        getPath(config) shouldBe "file_20220101.csv"
      }
    }

    "return file_20220106.csv" when {
      "given Path{ Date = 2022-01-01 , DateOffset = +5D, Template = file_%{to}.csv }" in {
        val config = ConfigFactory.parseMap(
          Map("Path" -> Map("Date" -> "2022-01-01", "DateOffset" -> "+5D", "Template" -> "file_%{to}.csv", "DatePattern" -> "yyyyMMdd"))
        )
        getPath(config) shouldBe "file_20220106.csv"
      }
    }

    "return file_<datetime>.csv with format yyyyMMdd-hhmmss" when {
      val expectedFileNamePattern = "^file_\\d{8}-\\d{6}\\.csv$".r
      val dateTimePattern         = "\\d{8}-\\d{6}".r
      val dateFormatter           = DateTimeFormatter.ofPattern("yyyyMMdd-hhmmss")

      "given Path = file_%{datetime}.csv" in {
        val config = ConfigFactory.parseMap(
          Map("Path" -> Map("Template" -> "file_%{datetime}.csv", "DatePattern" -> "yyyyMMdd-hhmmss"))
        )
        val resultFileName = getPath(config)

        (expectedFileNamePattern findAllIn resultFileName).size shouldBe 1

        val foundDateTime = dateTimePattern findFirstIn resultFileName
        Try(dateFormatter.parse(foundDateTime.get)).isSuccess shouldBe true
      }

      "given Path { Template = file_%{datetime}.csv }" in {
        val config = ConfigFactory.parseMap(
          Map("Path" -> Map("Template" -> "file_%{datetime}.csv", "DatePattern" -> "yyyyMMdd-hhmmss"))
        )
        val resultFileName = getPath(config)

        val foundDateTime = dateTimePattern findFirstIn resultFileName
        Try(dateFormatter.parse(foundDateTime.get)).isSuccess shouldBe true
      }

      "given Path { Template = file_%{datetime}.csv and a fixed date }" in {
        val config = ConfigFactory.parseMap(
          Map("Path" -> Map("Template" -> "file_%{datetime}.csv", "Date" -> "2022-10-21 +1D",  "DatePattern" -> "yyyyMMdd-hhmmss"))
        )
        val resultFileName = getPath(config)

        val foundDateTime = dateTimePattern findFirstIn resultFileName
        Try(dateFormatter.parse(foundDateTime.get)).isSuccess shouldBe true
      }
    }

    "return file_<uuid>.csv" when {
      val expectedFileNamePattern = "^file_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.csv$".r

      "given Path = file_%{uuid}.csv" in {
        val config = ConfigFactory.parseMap(
          Map("Path" -> Map("Template" -> "file_%{uuid}.csv"))
        )
        val resultFileName = getPath(config)
        (expectedFileNamePattern findAllIn resultFileName).size shouldBe 1
      }

      "given Path{ Template = file_%{uuid}.csv }" in {
        val config = ConfigFactory.parseMap(
          Map("Path" -> Map("Template" -> "file_%{uuid}.csv"))
        )
        val resultFileName = getPath(config)
        (expectedFileNamePattern findAllIn resultFileName).size shouldBe 1
      }
    }

    "return detemplatized file name" when {
      "given a date with month and day < 10" in {
        val config = ConfigFactory.parseMap(
          Map("Path" -> Map("Template" -> "file_%{year}_%{month}_%{day}.csv", "Date" -> "2022-09-09", "DatePattern" -> "yyyy-MM-dd"))
        )
        getPath(config) shouldBe "file_2022_09_09.csv"
      }

      "given a date with month and day > 10" in {
        val config = ConfigFactory.parseMap(
          Map("Path" -> Map("Template" -> "file_%{year}_%{month}_%{day}.csv", "Date" -> "2022-10-23", "DatePattern" -> "yyyy-MM-dd"))
        )
        getPath(config) shouldBe "file_2022_10_23.csv"
      }

      "given a date with a non-standard pattern" in {
        val config = ConfigFactory.parseMap(
          Map("Path" -> Map("Template" -> "file_%{year}_%{month}_%{day}.csv", "Date" -> "20220909", "DatePattern" -> "yyyyMMdd"))
        )
        getPath(config) shouldBe "file_2022_09_09.csv"
      }

      "given a date that only needs to match the year" in {
        val config = ConfigFactory.parseMap(
          Map("Path" -> Map("Template" -> "file_%{year}.csv", "Date" -> "2022-10-23", "DatePattern" -> "yyyy-MM-dd"))
        )
        getPath(config) shouldBe "file_2022.csv"
      }
      /*
      "given a date without an explicit DatePattern" in {
        val config = ConfigFactory.parseMap(
          Map("Path" -> Map("Template" -> "file_%{year}_%{month}_%{day}.csv", "Date" -> "2022-10-23"))
        )
        getPath(config) shouldBe "file_2022_10_23.csv"
      }
       */
    }

    "throw a ConfigException" when {
      "given a Config without Path" in {
        intercept[ConfigException] {
          val config = ConfigFactory.empty()

          getOptions(config)
        }
      }

      "given a Config without Template if Path is an object" in {
        intercept[ConfigException] {
          val config = ConfigFactory.parseMap(
            Map("Path" -> Map.empty)
          )

          getOptions(config)
        }
      }

      "given a Config without Date if trying to use %{from}" in {
        intercept[ConfigException] {
          val config = ConfigFactory.parseMap(
            Map("Path" -> Map("Template" -> "file_%{from}.csv", "DateOffset" -> "+5D"))
          )

          getOptions(config)
        }
      }

      "given a Config without DateOffset if trying to use %{from}" in {
        intercept[ConfigException] {
          val config = ConfigFactory.parseMap(
            Map("Path" -> Map("Template" -> "file_%{from}.csv", "Date" -> "2022-01-01"))
          )

          getOptions(config)
        }
      }

      "given a Config without Date if trying to use %{to}" in {
        intercept[ConfigException] {
          val config = ConfigFactory.parseMap(
            Map("Path" -> Map("Template" -> "file_%{to}.csv", "DateOffset" -> "+5D"))
          )

          getOptions(config)
        }
      }

      "given a Config without DateOffset if trying to use %{to}" in {
        intercept[ConfigException] {
          val config = ConfigFactory.parseMap(
            Map("Path" -> Map("Template" -> "file_%{to}.csv", "Date" -> "2022-01-01"))
          )

          getOptions(config)
        }
      }
    }
  }
}
