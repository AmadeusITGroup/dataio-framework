package com.amadeus.dataio.core.transformers

import com.amadeus.dataio.core.time.DateRange
import com.amadeus.dataio.testutils.SparkSpec
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.col
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.LocalDateTime

case class TestRecord(id: Int, eventDate: Date, value: Double)
class DateFiltererTest extends SparkSpec with Matchers {

  val testData = Seq(
    TestRecord(1, Date.valueOf("2023-01-01"), 10.0),
    TestRecord(2, Date.valueOf("2023-01-15"), 20.0),
    TestRecord(3, Date.valueOf("2023-01-31"), 30.0),
    TestRecord(4, Date.valueOf("2023-02-01"), 40.0),
    TestRecord(5, Date.valueOf("2023-02-15"), 50.0),
    TestRecord(6, Date.valueOf("2023-02-28"), 60.0),
    TestRecord(7, Date.valueOf("2023-03-01"), 70.0)
  )

  "DateFilterer" should "filter dataset with proper date range" in sparkTest { spark =>
    import spark.implicits._

    val testDs: Dataset[TestRecord] = spark.createDataset(testData)
    // Create date range for all of January 2023
    val rangeStart = LocalDateTime.of(2023, 1, 1, 0, 0)
    val rangeEnd   = LocalDateTime.of(2023, 1, 31, 23, 59, 59)
    val dateRange  = DateRange(rangeStart, rangeEnd)

    // Create a date filterer with our range and column
    val filterer = DateFilterer[TestRecord](Some(dateRange), Some(col("eventDate")))

    // Apply filter
    val filtered = filterer(testDs)

    // Check results
    filtered.count() shouldBe 3
    filtered.collect().map(_.id) should contain theSameElementsAs Seq(1, 2, 3)
  }

  it should "filter with midnight time in until date correctly" in sparkTest { spark =>
    import spark.implicits._

    val testDs: Dataset[TestRecord] = spark.createDataset(testData)
    // Create date range ending at midnight
    val rangeStart = LocalDateTime.of(2023, 1, 1, 0, 0)
    val rangeEnd   = LocalDateTime.of(2023, 2, 1, 0, 0) // Midnight
    val dateRange  = DateRange(rangeStart, rangeEnd)

    // Create a date filterer with our range and column
    val filterer = DateFilterer[TestRecord](Some(dateRange), Some(col("eventDate")))

    // Apply filter
    val filtered = filterer(testDs)

    // Check results - should include January but not February 1st
    filtered.count() shouldBe 3
    filtered.collect().map(_.id) should contain theSameElementsAs Seq(1, 2, 3)
  }

  it should "filter with time after midnight in until date correctly" in sparkTest { spark =>
    import spark.implicits._

    val testDs: Dataset[TestRecord] = spark.createDataset(testData)
    // Create date range ending just after midnight
    val rangeStart = LocalDateTime.of(2023, 1, 1, 0, 0)
    val rangeEnd   = LocalDateTime.of(2023, 2, 1, 0, 1) // Just after midnight
    val dateRange  = DateRange(rangeStart, rangeEnd)

    // Create a date filterer with our range and column
    val filterer = DateFilterer[TestRecord](Some(dateRange), Some(col("eventDate")))

    // Apply filter
    val filtered = filterer(testDs)

    // Check results - should include January and February 1st
    filtered should haveCountOf(4)
    filtered.collect().map(_.id) should contain theSameElementsAs Seq(1, 2, 3, 4)
  }

  it should "throw exception when date range is specified but column is missing" in sparkTest { spark =>
    import spark.implicits._

    val testDs: Dataset[TestRecord] = spark.createDataset(testData)
    val rangeStart                  = LocalDateTime.of(2023, 1, 1, 0, 0)
    val rangeEnd                    = LocalDateTime.of(2023, 1, 31, 23, 59, 59)
    val range                       = DateRange(rangeStart, rangeEnd)

    val filterer = new DateFilterer {
      override val dateRange: Option[DateRange] = Some(range)
      override val dateColumn: Option[Column]   = None
    }

    val exception = intercept[Exception] {
      filterer.applyDateFilter(testDs)
    }

    exception.getMessage shouldBe "date_filter requires a date column"
  }

  it should "throw exception when column is specified but date range is missing" in sparkTest { spark =>
    import spark.implicits._

    val testDs: Dataset[TestRecord] = spark.createDataset(testData)
    val filterer = new DateFilterer {
      override val dateRange: Option[DateRange] = None
      override val dateColumn: Option[Column]   = Some(col("eventDate"))
    }

    val exception = intercept[Exception] {
      filterer.applyDateFilter(testDs)
    }

    exception.getMessage shouldBe "date_filter requires a date range"
  }

  it should "handle complex date ranges correctly" in sparkTest { spark =>
    import spark.implicits._

    val testDs: Dataset[TestRecord] = spark.createDataset(testData)
    // Create date range for a specific period
    val rangeStart = LocalDateTime.of(2023, 1, 15, 0, 0) // Mid-January
    val rangeEnd   = LocalDateTime.of(2023, 2, 15, 0, 0) // Mid-February
    val dateRange  = DateRange(rangeStart, rangeEnd)

    // Create a date filterer with our range and column
    val filterer = DateFilterer[TestRecord](Some(dateRange), Some(col("eventDate")))
    println(dateRange)
    // Apply filter
    val filtered = filterer(testDs)

    // Should include Jan 15 through Feb 15
    filtered should haveCountOf(3)
    filtered should containTheSameRowsAs(testDs.filter($"eventDate" >= "2023-01-15" and $"eventDate" < "2023-02-15"))
  }
}
