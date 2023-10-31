package com.amadeus.dataio.core.time

import com.amadeus.dataio.core.time.Time.DateImplicits
import java.time.{LocalDateTime, ZoneOffset}
import com.amadeus.dataio.core.time.RangeType._
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec

class TimeTest extends AnyFunSpec with Matchers {

  describe("Validate time functions") {
    val instant1 = LocalDateTime.now()
    val instant2 = LocalDateTime.ofEpochSecond(946684800L, 0, ZoneOffset.UTC) // Long representation of 2000 January 01 00:00:00 UTC

    // Get MIN/MAX DateTime
    Time.minTime(instant1, instant2) shouldBe (instant2)
    Time.maxTime(instant1, instant2) shouldBe (instant1)

    // Get MIN/MAX DateTime with equal date
    Time.minTime(instant1, instant1) shouldBe (instant1)
    Time.maxTime(instant1, instant1) shouldBe (instant1)

    // Get time map of NOW
    Time.getTimeMap() should have size Formats.AllFormats.size

    // Get time map with prefix
    val prefix = "execTime"
    Time.getTimeMap(Some(prefix)).keySet.filter { x => x.startsWith(prefix) } should have size Formats.AllFormats.size
  }

  describe("isBetween") {
    val from = LocalDateTime.now()
    val until = from.plusHours(1L)

    from.minusHours(1L).isBetween(from, until) shouldBe false

    from.isBetween(from, until, ClosedClosed) shouldBe true
    from.isBetween(from, until, ClosedOpen) shouldBe true
    from.isBetween(from, until, OpenOpen) shouldBe false
    from.isBetween(from, until, OpenClosed) shouldBe false

    from.plusMinutes(10L).isBetween(from, until, ClosedClosed) shouldBe true
    from.plusMinutes(10L).isBetween(from, until, ClosedOpen) shouldBe true
    from.plusMinutes(10L).isBetween(from, until, OpenClosed) shouldBe true
    from.plusMinutes(10L).isBetween(from, until, OpenOpen) shouldBe true

    until.isBetween(from, until, ClosedClosed) shouldBe true
    until.isBetween(from, until, ClosedOpen) shouldBe false
    until.isBetween(from, until, OpenOpen) shouldBe false
    until.isBetween(from, until, OpenClosed) shouldBe true

    until.plusHours(1L).isBetween(from, until) shouldBe false

    // Range bounds are the same
    from.isBetween(from, from, ClosedClosed) shouldBe true
    from.isBetween(from, from, ClosedOpen) shouldBe false
    from.isBetween(from, from, OpenOpen) shouldBe false
    from.isBetween(from, from, OpenClosed) shouldBe false
  }

  describe("Date formatting functions") {
    val date = LocalDateTime.ofEpochSecond(1496275200L, 0, ZoneOffset.UTC) // Long representation of 2017 June 01 00:00:00 UTC

    // Use a labeled formatter to get time string
    date.formatted(Formats.IsoInstant) shouldBe ("2017-06-01T00.00.00Z")
  }

  describe("Date modification functions") {
    val date = LocalDateTime.ofEpochSecond(1496275200L, 0, ZoneOffset.UTC) // Long representation of 2017 June 01 00:00:00 UTC

    // Apply setter
    date.applySetter("Y", "2000").formatted(Formats.IsoInstant) shouldBe ("2000-06-01T00.00.00Z")
    date.applySetter("M", "12").formatted(Formats.IsoInstant) shouldBe ("2017-12-01T00.00.00Z")
    date.applySetter("D", "15").formatted(Formats.IsoInstant) shouldBe ("2017-06-15T00.00.00Z")
    date.applySetter("H", "12").formatted(Formats.IsoInstant) shouldBe ("2017-06-01T12.00.00Z")
    date.applySetter("m", "15").formatted(Formats.IsoInstant) shouldBe ("2017-06-01T00.15.00Z")
    date.applySetter("s", "30").formatted(Formats.IsoInstant) shouldBe ("2017-06-01T00.00.30Z")

    // Apply positive delta
    date.applyDelta("+1Y").formatted(Formats.IsoInstant) shouldBe ("2018-06-01T00.00.00Z")
    date.applyDelta("+1M").formatted(Formats.IsoInstant) shouldBe ("2017-07-01T00.00.00Z")
    date.applyDelta("+1W").formatted(Formats.IsoInstant) shouldBe ("2017-06-08T00.00.00Z")
    date.applyDelta("+1D").formatted(Formats.IsoInstant) shouldBe ("2017-06-02T00.00.00Z")
    date.applyDelta("+1H").formatted(Formats.IsoInstant) shouldBe ("2017-06-01T01.00.00Z")
    date.applyDelta("+1m").formatted(Formats.IsoInstant) shouldBe ("2017-06-01T00.01.00Z")
    date.applyDelta("+1s").formatted(Formats.IsoInstant) shouldBe ("2017-06-01T00.00.01Z")

    // Apply negative delta
    date.applyDelta("-1Y").formatted(Formats.IsoInstant) shouldBe ("2016-06-01T00.00.00Z")
    date.applyDelta("-1M").formatted(Formats.IsoInstant) shouldBe ("2017-05-01T00.00.00Z")
    date.applyDelta("-1W").formatted(Formats.IsoInstant) shouldBe ("2017-05-25T00.00.00Z")
    date.applyDelta("-1D").formatted(Formats.IsoInstant) shouldBe ("2017-05-31T00.00.00Z")
    date.applyDelta("-1H").formatted(Formats.IsoInstant) shouldBe ("2017-05-31T23.00.00Z")
    date.applyDelta("-1m").formatted(Formats.IsoInstant) shouldBe ("2017-05-31T23.59.00Z")
    date.applyDelta("-1s").formatted(Formats.IsoInstant) shouldBe ("2017-05-31T23.59.59Z")

    // Error cases
    an[IllegalArgumentException] should be thrownBy date.applySetter("toDay", "753") // wrong setter modifier
    an[IllegalArgumentException] should be thrownBy date.applySetter("n", "753") // unknown modifier label
    an[IllegalArgumentException] should be thrownBy date.applySetter("D", "one") // non numeric value
    an[IllegalArgumentException] should be thrownBy date.applyDelta("+753n") // unknown modifier label
    an[IllegalArgumentException] should be thrownBy date.applyDelta("-753n") // unknown modifier label
    an[IllegalArgumentException] should be thrownBy date.applyDelta("*7D") // incorrect delta sign
    an[IllegalArgumentException] should be thrownBy date.applyDelta("+oneD") // non numeric delta
  }

  describe("Date truncation functions") {
    val date = LocalDateTime.ofEpochSecond(1442496927L, 369, ZoneOffset.UTC) // Long representation of 2015 September 17 13:35:27 UTC

    date.getNano shouldBe (369)

    // Truncate via functions
    date.truncSecond.getNano shouldBe (0)
    date.truncSecond.formatted(Formats.IsoInstant) shouldBe ("2015-09-17T13.35.27Z")
    date.truncMinute.formatted(Formats.IsoInstant) shouldBe ("2015-09-17T13.35.00Z")
    date.truncHour.formatted(Formats.IsoInstant) shouldBe ("2015-09-17T13.00.00Z")
    date.truncDay.formatted(Formats.IsoInstant) shouldBe ("2015-09-17T00.00.00Z")
    date.truncMonth.formatted(Formats.IsoInstant) shouldBe ("2015-09-01T00.00.00Z")
    date.truncYear.formatted(Formats.IsoInstant) shouldBe ("2015-01-01T00.00.00Z")

    // Truncate via string modifier
    date.truncate("toSecond").getNano shouldBe (0)
    date.truncate("toSecond").formatted(Formats.IsoInstant) shouldBe ("2015-09-17T13.35.27Z")
    date.truncate("toMinute").formatted(Formats.IsoInstant) shouldBe ("2015-09-17T13.35.00Z")
    date.truncate("toHour").formatted(Formats.IsoInstant) shouldBe ("2015-09-17T13.00.00Z")
    date.truncate("toDay").formatted(Formats.IsoInstant) shouldBe ("2015-09-17T00.00.00Z")
    date.truncate("toMonth").formatted(Formats.IsoInstant) shouldBe ("2015-09-01T00.00.00Z")
    date.truncate("toYear").formatted(Formats.IsoInstant) shouldBe ("2015-01-01T00.00.00Z")

    // Error cases
    an[IllegalArgumentException] should be thrownBy date.truncate("toCentury") // unknown modifier label
    an[IllegalArgumentException] should be thrownBy date.truncate("D") // wrong truncation modifier
  }
}