package com.amadeus.dataio.core.transformers

import com.amadeus.dataio.config.fields.DateFilterConfig
import com.amadeus.dataio.core.time.DateRange
import com.amadeus.dataio.testutils.SparkSpec
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

import java.sql.Date
import java.time.{LocalDate, LocalDateTime}

// Case classes must be outside the test class for Spark encoder generation
case class EventData(id: Int, eventDate: Date, name: String)
case class CustomEventData(id: Int, timestamp: Date, name: String)

class DateFiltererTest extends SparkSpec {

  // Helper to create test datasets
  def createTestDataset()(implicit spark: org.apache.spark.sql.SparkSession): Dataset[EventData] = {
    import spark.implicits._
    Seq(
      EventData(1, Date.valueOf("2026-01-15"), "Event 1"),
      EventData(2, Date.valueOf("2026-01-18"), "Event 2"),
      EventData(3, Date.valueOf("2026-01-20"), "Event 3"),
      EventData(4, Date.valueOf("2026-01-22"), "Event 4"),
      EventData(5, Date.valueOf("2026-01-25"), "Event 5"),
      EventData(6, Date.valueOf("2026-01-28"), "Event 6"),
      EventData(7, Date.valueOf("2026-01-30"), "Event 7")
    ).toDS()
  }

  behavior of "DateFilterer.applyDateFilter"

  // ===== No Filter Tests =====

  it should "return original dataset when both config and column are None" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    val filterer = DateFilterer[EventData](None, None)

    val result = filterer(ds)
    result should haveCountOf(ds.count())
  }

  it should "throw exception when config is provided but column is None" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    val config = DateFilterConfig.FromOnly(LocalDate.parse("2026-01-20"))
    val filterer = DateFilterer[EventData](Some(config), None)

    an[Exception] should be thrownBy {
      filterer(ds)
    }
  }

  it should "throw exception when column is provided but config is None" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    val filterer = DateFilterer[EventData](None, Some(col("eventDate")))

    an[Exception] should be thrownBy {
      filterer(ds)
    }
  }

  // ===== FromOnly Tests (>= date) =====

  it should "filter correctly with FromOnly (>= date)" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    val config = DateFilterConfig.FromOnly(LocalDate.parse("2026-01-20"))
    val filterer = DateFilterer[EventData](Some(config), Some(col("eventDate")))

    val result = filterer(ds)

    result should haveCountOf(5)
    result.collect().map(_.id) should contain theSameElementsAs Seq(3, 4, 5, 6, 7)
  }

  it should "include the boundary date with FromOnly" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    val config = DateFilterConfig.FromOnly(LocalDate.parse("2026-01-20"))
    val filterer = DateFilterer[EventData](Some(config), Some(col("eventDate")))

    val result = filterer(ds).collect()

    result.exists(_.eventDate == Date.valueOf("2026-01-20")) shouldBe true
  }

  it should "return empty dataset when FromOnly date is after all events" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    val config = DateFilterConfig.FromOnly(LocalDate.parse("2026-02-01"))
    val filterer = DateFilterer[EventData](Some(config), Some(col("eventDate")))

    val result = filterer(ds)
    result should beEmpty
  }

  it should "return all events when FromOnly date is before all events" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    val config = DateFilterConfig.FromOnly(LocalDate.parse("2026-01-01"))
    val filterer = DateFilterer[EventData](Some(config), Some(col("eventDate")))

    val result = filterer(ds)
    result should haveCountOf(7)
  }

  // ===== UntilOnly Tests (< date) =====

  it should "filter correctly with UntilOnly (< date)" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    val config = DateFilterConfig.UntilOnly(LocalDate.parse("2026-01-25"))
    val filterer = DateFilterer[EventData](Some(config), Some(col("eventDate")))

    val result = filterer(ds)

    result should haveCountOf(4)
    result.collect().map(_.id) should contain theSameElementsAs Seq(1, 2, 3, 4)
  }

  it should "exclude the boundary date with UntilOnly" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    val config = DateFilterConfig.UntilOnly(LocalDate.parse("2026-01-25"))
    val filterer = DateFilterer[EventData](Some(config), Some(col("eventDate")))

    val result = filterer(ds).collect()

    result.exists(_.eventDate == Date.valueOf("2026-01-25")) shouldBe false
  }

  it should "return empty dataset when UntilOnly date is before all events" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    val config = DateFilterConfig.UntilOnly(LocalDate.parse("2026-01-01"))
    val filterer = DateFilterer[EventData](Some(config), Some(col("eventDate")))

    val result = filterer(ds)
    result should beEmpty
  }

  it should "return all events when UntilOnly date is after all events" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    val config = DateFilterConfig.UntilOnly(LocalDate.parse("2026-02-01"))
    val filterer = DateFilterer[EventData](Some(config), Some(col("eventDate")))

    val result = filterer(ds)
    result should haveCountOf(7)
  }

  // ===== Range Tests =====

  it should "filter correctly with DateRange using LocalDate" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    // DateRange.apply(LocalDate, LocalDate) converts to midnight LocalDateTime
    val dateRange = DateRange(LocalDate.parse("2026-01-20"), LocalDate.parse("2026-01-25"))
    val config = DateFilterConfig.Range(dateRange)
    val filterer = DateFilterer[EventData](Some(config), Some(col("eventDate")))

    val result = filterer(ds)

    // Filter: >= 2026-01-20 AND < 2026-01-25 (from inclusive, until exclusive)
    // Should include: 2026-01-20, 2026-01-22 (ids 3, 4)
    result should haveCountOf(2)
    result.collect().map(_.id) should contain theSameElementsAs Seq(3, 4)
  }

  it should "filter correctly with DateRange using reference and offset" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    // DateRange with reference and offset (legacy syntax)
    val dateRange = DateRange("2026-01-20", "+5D")
    val config = DateFilterConfig.Range(dateRange)
    val filterer = DateFilterer[EventData](Some(config), Some(col("eventDate")))

    val result = filterer(ds)

    // from = 2026-01-20, until = 2026-01-20 + 5D = 2026-01-25
    // Filter: >= 2026-01-20 AND < 2026-01-25 (from inclusive, until exclusive)
    // Should include: 2026-01-20, 2026-01-22 (ids 3, 4)
    result should haveCountOf(2)
    result.collect().map(_.id) should contain theSameElementsAs Seq(3, 4)
  }

  it should "include from boundary and exclude until boundary in Range" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    val dateRange = DateRange(LocalDate.parse("2026-01-20"), LocalDate.parse("2026-01-25"))
    val config = DateFilterConfig.Range(dateRange)
    val filterer = DateFilterer[EventData](Some(config), Some(col("eventDate")))

    val result = filterer(ds).collect()

    // from is inclusive
    result.exists(_.eventDate == Date.valueOf("2026-01-20")) shouldBe true
    // until is exclusive
    result.exists(_.eventDate == Date.valueOf("2026-01-25")) shouldBe false
  }

  it should "return empty dataset when Range is outside all events" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    val dateRange = DateRange(LocalDate.parse("2026-02-01"), LocalDate.parse("2026-02-10"))
    val config = DateFilterConfig.Range(dateRange)
    val filterer = DateFilterer[EventData](Some(config), Some(col("eventDate")))

    val result = filterer(ds)
    result should beEmpty
  }

  it should "handle one day range correctly" in sparkTest { implicit spark =>
    val ds = createTestDataset()
    // To get exactly one day, until should be the next day
    val dateRange = DateRange(LocalDate.parse("2026-01-20"), LocalDate.parse("2026-01-21"))
    val config = DateFilterConfig.Range(dateRange)
    val filterer = DateFilterer[EventData](Some(config), Some(col("eventDate")))

    val result = filterer(ds)

    result should haveCountOf(1)
    result.collect().head.id shouldBe 3
  }

  it should "return an empty result when from == until" in sparkTest { implicit spark =>
      val ds = createTestDataset()
      // To get exactly one day, until should be the next day
      val dateRange = DateRange(LocalDate.parse("2026-01-20"), LocalDate.parse("2026-01-20"))
      val config = DateFilterConfig.Range(dateRange)
      val filterer = DateFilterer[EventData](Some(config), Some(col("eventDate")))

      val result = filterer(ds)

      result should haveCountOf(0)
  }

  // ===== Combined Logic Tests =====

  it should "work with different column names" in sparkTest { implicit spark =>
    import spark.implicits._

    val ds = Seq(
      CustomEventData(1, Date.valueOf("2026-01-15"), "Event 1"),
      CustomEventData(2, Date.valueOf("2026-01-25"), "Event 2")
    ).toDS()

    val config = DateFilterConfig.FromOnly(LocalDate.parse("2026-01-20"))
    val filterer = DateFilterer[CustomEventData](Some(config), Some(col("timestamp")))

    val result = filterer(ds)
    result should haveCountOf(1)
    result.collect().head.id shouldBe 2
  }

  it should "work with Dataset[Row] not just typed datasets" in sparkTest { implicit spark =>
    val ds = createTestDataset().toDF()
    val config = DateFilterConfig.FromOnly(LocalDate.parse("2026-01-20"))

    // Note: For untyped datasets, we need to use the untyped apply method
    val filterer = new DateFilterer {
      override val dateFilterConfig = Some(config)
      override val dateColumn = Some(col("eventDate"))
    }

    val result = filterer.applyDateFilter(ds)
    result should haveCountOf(5)
  }
}