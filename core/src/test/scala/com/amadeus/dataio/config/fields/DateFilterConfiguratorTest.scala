package com.amadeus.dataio.config.fields

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class DateFilterConfiguratorTest extends AnyFlatSpec with Matchers {
  behavior of "getDateFilterRange"

  it should "return date range when proper config exists" in {
    val configStr               = """
      date_filter {
        reference= "2023-01-01"
        offset = "+5D"
      }
    """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getDateFilterRange
    result.isDefined should be(true)

    val dateRange = result.get
    dateRange.from.toLocalDate should be(LocalDate.parse("2023-01-01"))
    dateRange.until.toLocalDate should be(LocalDate.parse("2023-01-06"))
  }

  it should "throw IllegalArgumentException when only reference exists" in {
    val configStr =
      """
      date_filter.reference = "2023-01-01"
    """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    an[IllegalArgumentException] should be thrownBy {
      getDateFilterRange
    }
  }

  it should "throw IllegalArgumentException when only offset exists" in {
    val configStr =
      """
      date_filter.offset = "+5D"
    """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    an[IllegalArgumentException] should be thrownBy {
      getDateFilterRange
    }
  }

  it should "return None when neither reference nor offset exists" in {
    val configStr =
      """
      some_other_config = "value"
    """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getDateFilterRange
    result should be(None)
  }

  "getDateFilterColumn" should "return a Column when column exists in config" in {
    val configStr =
      """
      date_filter.column = "created_at"
    """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getDateFilterColumn
    result.isDefined should be(true)
    // Cannot directly compare Column objects, so checking if defined is sufficient
  }

  it should "return None when column doesn't exist in config" in {
    val configStr =
      """
      some_other_config = "value"
    """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getDateFilterColumn
    result should be(None)
  }
}
