package com.amadeus.dataio.config.fields

import com.amadeus.dataio.testutils.ConfigCreator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class DateFilterConfiguratorTest extends AnyFlatSpec with Matchers with ConfigCreator {

  // Test fixture to create a configurator instance
  trait TestConfigurator extends DateFilterConfigurator

  behavior of "DateFilterConfigurator.getDateFilterConfig"

  // ===== No Configuration Tests =====

  it should "return None when date_filter config is not present" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |some_other_config = "value"
                                         |""".stripMargin)

    getDateFilterConfig shouldBe None
  }

  // ===== reference+offset Syntax Tests =====

  it should "parse reference+offset syntax" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |date_filter {
                                         |  reference = "2026-01-25"
                                         |  offset = "-7D"
                                         |}
                                         |""".stripMargin)

    val result = getDateFilterConfig
    result shouldBe defined
    result.get shouldBe a[DateFilterConfig.Range]
  }

  it should "throw exception when only reference is provided (missing offset)" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |date_filter {
                                         |  reference = "2026-01-25"
                                         |}
                                         |""".stripMargin)

    an[IllegalArgumentException] should be thrownBy {
      getDateFilterConfig
    }
  }

  it should "throw exception when only offset is provided (missing reference)" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |date_filter {
                                         |  offset = "-7D"
                                         |}
                                         |""".stripMargin)

    an[IllegalArgumentException] should be thrownBy {
      getDateFilterConfig
    }
  }

  // ===== from/until Syntax Tests =====

  it should "parse new syntax with both from and until" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |date_filter {
                                         |  from = "2026-01-18"
                                         |  until = "2026-01-25"
                                         |}
                                         |""".stripMargin)

    val result = getDateFilterConfig
    result shouldBe defined
    result.get shouldBe a[DateFilterConfig.Range]
  }

  it should "parse new syntax with only from" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |date_filter {
                                         |  from = "2026-01-20"
                                         |}
                                         |""".stripMargin)

    val result = getDateFilterConfig
    result shouldBe defined
    result.get match {
      case DateFilterConfig.FromOnly(date) =>
        date shouldBe LocalDate.parse("2026-01-20")
      case _ => fail("Expected FromOnly")
    }
  }

  it should "parse new syntax with only until" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |date_filter {
                                         |  until = "2026-01-25"
                                         |}
                                         |""".stripMargin)

    val result = getDateFilterConfig
    result shouldBe defined
    result.get match {
      case DateFilterConfig.UntilOnly(date) =>
        date shouldBe LocalDate.parse("2026-01-25")
      case _ => fail("Expected UntilOnly")
    }
  }

  // ===== Error Tests =====

  it should "throw exception when from == until" in new TestConfigurator {
    implicit val configStr = """
                      |date_filter {
                      |  column = "bookingDate"
                      |  from = "2026-01-15"
                      |  until = "2026-01-15"
                      |}
                      |""".stripMargin

    implicit val config = createConfig(configStr)

    an[IllegalArgumentException] should be thrownBy {
      getDateFilterConfig
    }
  }

  it should "throw exception when reference, offset and from are present" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |date_filter {
                                         |  reference = "2026-01-25"
                                         |  offset = "-7D"
                                         |  from = "2026-01-18"
                                         |}
                                         |""".stripMargin)

    an[IllegalArgumentException] should be thrownBy {
      getDateFilterConfig
    }
  }

  it should "throw exception when reference, offset, and until are present" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |date_filter {
                                         |  reference = "2026-01-25"
                                         |  offset = "-7D"
                                         |  until = "2026-01-30"
                                         |}
                                         |""".stripMargin)

    an[IllegalArgumentException] should be thrownBy {
      getDateFilterConfig
    }
  }

  // ===== Invalid Configuration Tests =====

  it should "return None and log warning when date_filter exists but has no valid syntax" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |date_filter {
                                         |  column = "booking_date"
                                         |}
                                         |""".stripMargin)

    // Should return None and log a warning
    getDateFilterConfig shouldBe None
  }

  it should "throw exception for invalid date format in from" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |date_filter {
                                         |  from = "invalid-date"
                                         |}
                                         |""".stripMargin)

    an[Exception] should be thrownBy {
      getDateFilterConfig
    }
  }

  it should "throw exception for invalid date format in until" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |date_filter {
                                         |  until = "not-a-date"
                                         |}
                                         |""".stripMargin)

    an[Exception] should be thrownBy {
      getDateFilterConfig
    }
  }

  // ===== Column Configuration Tests =====

  behavior of "DateFilterConfigurator.getDateFilterColumn"

  it should "return column when specified" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |date_filter {
                                         |  column = "booking_date"
                                         |}
                                         |""".stripMargin)

    val result = getDateFilterColumn
    result shouldBe defined
  }

  it should "return None when column is not specified" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |date_filter {
                                         |  from = "2026-01-20"
                                         |}
                                         |""".stripMargin)

    getDateFilterColumn shouldBe None
  }

  it should "return None when date_filter is not present" in new TestConfigurator {
    implicit val config = createConfig("""
                                         |other_config = "value"
                                         |""".stripMargin)

    getDateFilterColumn shouldBe None
  }
}
