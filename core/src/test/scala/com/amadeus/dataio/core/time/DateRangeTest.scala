package com.amadeus.dataio.core.time

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

class DateRangeTest extends AnyWordSpec with Matchers {

  "DateRange" should {

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    "return DateRange(2022-01-01, 2022-01-06)" when {

      val expectedFromDate  = LocalDateTime.of(2022,1,1, 0, 0, 0)
      val expectedUntilDate  = LocalDateTime.of(2022,1,6, 0, 0, 0)

      "given 2022-01-01, 2022-01-06 as LocalDate" in {
        val firstDate  = LocalDate.parse("2022-01-01", formatter)
        val secondDate = LocalDate.parse("2022-01-06", formatter)
        val result     = DateRange(firstDate, secondDate)

        result.from shouldBe expectedFromDate
        result.until shouldBe expectedUntilDate
      }

      "given 2022-01-06, 2022-01-01 as LocalDate" in {
        val firstDate  = LocalDate.parse("2022-01-01", formatter)
        val secondDate = LocalDate.parse("2022-01-06", formatter)
        val result     = DateRange(secondDate, firstDate)

        result.from shouldBe expectedFromDate
        result.until shouldBe expectedUntilDate
      }

      "given 2022-01-01, 2022-01-06 as LocalDateTime" in {
        val firstDate  = LocalDateTime.of(2022, 1, 1, 0, 0, 0 )
        val secondDate = LocalDateTime.of(2022, 1, 6, 0, 0, 0)
        val result     = DateRange(firstDate, secondDate)

        result.from shouldBe expectedFromDate
        result.until shouldBe expectedUntilDate
      }

      "given 2022-01-01, +5D" in {
        val result = DateRange("2022-01-01", "+5D")

        result.from shouldBe expectedFromDate
        result.until shouldBe expectedUntilDate
      }

      "given 20220101, +5D" in {
        val result = DateRange("20220101", "+5D")

        result.from shouldBe expectedFromDate
        result.until shouldBe expectedUntilDate
      }
    }

    "return DateRange(2022-01-03 01:00, 2022-01-03 01:00)" when {

      val expectedFromDateWithTime  = LocalDateTime.of(2022,1,3, 1, 0, 0)
      val expectedFromDateWithoutTime  = LocalDateTime.of(2022,1,3, 0, 0, 0)

      "given 2022-01-01, +2D +1H" in {
        val result = DateRange("2022-01-01 +2D +1H")

        result.from shouldBe expectedFromDateWithTime
        result.until shouldBe expectedFromDateWithTime
      }

      "given 2022-01-03" in {
        val result = DateRange("2022-01-03")

        result.from shouldBe expectedFromDateWithoutTime
        result.until shouldBe expectedFromDateWithoutTime
      }
    }

    "run original BDP test" when {
      "given 1980-10-03" in {
        val range = DateRange("1980-10-03", "")
        assert(range.from == LocalDateTime.of(1980, 10, 3, 0, 0))
      }

      "given 1980-10-03 -1D" in {
        val range = DateRange("1980-10-03 -1D", "")
        assert(range.from == LocalDateTime.of(1980, 10, 2, 0, 0))
      }

      "given 1980-10-03 +1D" in {
        val range = DateRange("1980-10-03 +1D", "")
        assert(range.from == LocalDateTime.of(1980, 10, 4, 0, 0))
      }

      "given 1980-10-03 D=8" in {
        val range = DateRange("1980-10-03 D=8", "")
        assert(range.from == LocalDateTime.of(1980, 10, 8, 0, 0))
      }

      "given 1980-10-03 toYear" in {
        val range = DateRange("1980-10-03 toYear", "")
        assert(range.from == LocalDateTime.of(1980, 1, 1, 0, 0))
      }

      "given 1980-10-03 +1Y toYear M=2" in {
        val range = DateRange("1980-10-03 +1Y toYear M=2", "")
        assert(range.from == LocalDateTime.of(1981, 2, 1, 0, 0))
      }

      "given 1980-10-03 +1Y toYear M=2 +10M" in {
        val range = DateRange("1980-10-03 +1Y toYear M=2", "+10M")
        assert(range.from == LocalDateTime.of(1981, 2, 1, 0, 0))
        assert(range.until == LocalDateTime.of(1981, 12, 1, 0, 0))
      }

      "given 2017-02-14T00.00.00Z" in {
        val reference = "2017-02-14T00.00.00Z"
        val offset = "-7D"
        val range = DateRange(reference, offset)
        assert(range.from == LocalDateTime.of(2017, 2, 7, 0, 0))
        assert(range.until == LocalDateTime.of(2017, 2, 14, 0, 0))
      }

      "given LocalDateTime.of(2017, 2, 7, 0, 0)" in {
        val range = DateRange(LocalDateTime.of(2017, 2, 7, 0, 0))
        assert(range.from == LocalDateTime.of(2017, 2, 7, 0, 0))
        assert(range.until.isAfter(LocalDateTime.of(2017, 2, 7, 0, 0)))
      }
    }
  }
}

