package com.amadeus.dataio.integration

import com.amadeus.dataio.config.fields.DateFilterConfigurator
import com.amadeus.dataio.core.transformers.DateFilterer
import com.amadeus.dataio.testutils.SparkSpec
import org.apache.spark.sql.Dataset

import java.sql.Date

// Case class must be outside the test class for Spark encoder generation
case class Booking(id: Int, bookingDate: Date, customerName: String)

class DateFilterIntegrationTest extends SparkSpec {

  def createBookings()(implicit spark: org.apache.spark.sql.SparkSession): Dataset[Booking] = {
    import spark.implicits._
    Seq(
      Booking(1, Date.valueOf("2026-01-10"), "Alice"),
      Booking(2, Date.valueOf("2026-01-15"), "Bob"),
      Booking(3, Date.valueOf("2026-01-20"), "Charlie"),
      Booking(4, Date.valueOf("2026-01-25"), "David"),
      Booking(5, Date.valueOf("2026-01-30"), "Eve")
    ).toDS()
  }

  object TestConfigurator extends DateFilterConfigurator

  behavior of "DateFilter Integration (Configurator + Filterer)"

  // ===== Reference+Offset Syntax Integration Tests =====

  it should "work end-to-end with reference+offset syntax" in sparkTest { implicit spark =>
    val configStr = """
                      |date_filter {
                      |  column = "bookingDate"
                      |  reference = "2026-01-25"
                      |  offset = "-7D"
                      |}
                      |""".stripMargin

    implicit val config = createConfig(configStr)

    val filterConfig = TestConfigurator.getDateFilterConfig
    val column = TestConfigurator.getDateFilterColumn

    filterConfig shouldBe defined
    column shouldBe defined

    val ds = createBookings()
    val filterer = DateFilterer[Booking](filterConfig, column)
    val result = filterer(ds)

    // Reference - offset = 2026-01-25 - 7D = 2026-01-18
    // Filter: >= 2026-01-18 AND < 2026-01-25 (from inclusive, until exclusive)
    // Should include: 2026-01-20 (id 3)
    result should haveCountOf(1)
    result.collect().map(_.id) should contain theSameElementsAs Seq(3)
  }

  // ===== From/Until Syntax Integration Tests =====

  it should "work end-to-end with from/until syntax" in sparkTest { implicit spark =>
    val configStr = """
                      |date_filter {
                      |  column = "bookingDate"
                      |  from = "2026-01-15"
                      |  until = "2026-01-25"
                      |}
                      |""".stripMargin

    implicit val config = createConfig(configStr)

    val filterConfig = TestConfigurator.getDateFilterConfig
    val column = TestConfigurator.getDateFilterColumn

    val ds = createBookings()
    val filterer = DateFilterer[Booking](filterConfig, column)
    val result = filterer(ds)

    // Filter: >= 2026-01-15 AND < 2026-01-25
    // Should include: 2026-01-15, 2026-01-20 (ids 2, 3)
    result should haveCountOf(2)
    result.collect().map(_.id) should contain theSameElementsAs Seq(2, 3)
  }

  it should "work end-to-end with only from" in sparkTest { implicit spark =>
    val configStr = """
                      |date_filter {
                      |  column = "bookingDate"
                      |  from = "2026-01-20"
                      |}
                      |""".stripMargin

    implicit val config = createConfig(configStr)

    val filterConfig = TestConfigurator.getDateFilterConfig
    val column = TestConfigurator.getDateFilterColumn

    val ds = createBookings()
    val filterer = DateFilterer[Booking](filterConfig, column)
    val result = filterer(ds)

    // Should include bookings >= 2026-01-20
    result should haveCountOf(3)
    result.collect().map(_.id) should contain theSameElementsAs Seq(3, 4, 5)
  }

  it should "work end-to-end with only until" in sparkTest { implicit spark =>
    val configStr = """
                      |date_filter {
                      |  column = "bookingDate"
                      |  until = "2026-01-25"
                      |}
                      |""".stripMargin

    implicit val config = createConfig(configStr)

    val filterConfig = TestConfigurator.getDateFilterConfig
    val column = TestConfigurator.getDateFilterColumn

    val ds = createBookings()
    val filterer = DateFilterer[Booking](filterConfig, column)
    val result = filterer(ds)

    // Should include bookings < 2026-01-25
    result should haveCountOf(3)
    result.collect().map(_.id) should contain theSameElementsAs Seq(1, 2, 3)
  }

  // ===== Edge Cases Integration Tests =====

  it should "handle single day range correctly (until = from + 1)" in sparkTest { implicit spark =>
    val configStr = """
                      |date_filter {
                      |  column = "bookingDate"
                      |  from = "2026-01-20"
                      |  until = "2026-01-21"
                      |}
                      |""".stripMargin

    implicit val config = createConfig(configStr)

    val filterConfig = TestConfigurator.getDateFilterConfig
    val column = TestConfigurator.getDateFilterColumn

    val ds = createBookings()
    val result = DateFilterer[Booking](filterConfig, column)(ds)

    // Should include only 2026-01-20
    result should haveCountOf(1)
    result.collect().head.id shouldBe 3
  }

  // ===== No Filter Integration Test =====

  it should "return all data when no date_filter configuration is present" in sparkTest { implicit spark =>
    val configStr = """
                      |some_other_config = "value"
                      |""".stripMargin

    implicit val config = createConfig(configStr)

    val filterConfig = TestConfigurator.getDateFilterConfig
    val column = TestConfigurator.getDateFilterColumn

    filterConfig shouldBe None
    column shouldBe None

    val ds = createBookings()
    val filterer = DateFilterer[Booking](filterConfig, column)
    val result = filterer(ds)

    result should haveCountOf(5)
  }

  // ===== Error Handling Integration Tests =====

  it should "fail when column is missing but config is present" in sparkTest { implicit spark =>
    val configStr = """
                      |date_filter {
                      |  from = "2026-01-20"
                      |}
                      |""".stripMargin

    implicit val config = createConfig(configStr)

    val filterConfig = TestConfigurator.getDateFilterConfig
    val column = TestConfigurator.getDateFilterColumn

    filterConfig shouldBe defined
    column shouldBe None

    val ds = createBookings()
    val filterer = DateFilterer[Booking](filterConfig, column)

    an[Exception] should be thrownBy {
      filterer(ds)
    }
  }

  // ===== Boundary Condition Tests =====

  it should "correctly handle inclusive from boundary" in sparkTest { implicit spark =>
    val configStr = """
                      |date_filter {
                      |  column = "bookingDate"
                      |  from = "2026-01-20"
                      |}
                      |""".stripMargin

    implicit val config = createConfig(configStr)

    val filterConfig = TestConfigurator.getDateFilterConfig
    val column = TestConfigurator.getDateFilterColumn

    val ds = createBookings()
    val result = DateFilterer[Booking](filterConfig, column)(ds).collect()

    // Should include the boundary date 2026-01-20
    result.exists(_.bookingDate == Date.valueOf("2026-01-20")) shouldBe true
  }

  it should "correctly handle exclusive until boundary" in sparkTest { implicit spark =>
    val configStr = """
                      |date_filter {
                      |  column = "bookingDate"
                      |  until = "2026-01-20"
                      |}
                      |""".stripMargin

    implicit val config = createConfig(configStr)

    val filterConfig = TestConfigurator.getDateFilterConfig
    val column = TestConfigurator.getDateFilterColumn

    val ds = createBookings()
    val result = DateFilterer[Booking](filterConfig, column)(ds).collect()

    // Should NOT include the boundary date 2026-01-20
    result.exists(_.bookingDate == Date.valueOf("2026-01-20")) shouldBe false
  }
}