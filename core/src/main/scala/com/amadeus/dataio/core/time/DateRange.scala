package com.amadeus.dataio.core.time

import com.amadeus.dataio.core.time.Time.{maxTime, minTime}
import java.time.{LocalDate, LocalDateTime}


/**
 * <p>A data structure holding a range of dates.</p>
 * <p>e.g.
 * <pre>
 *   val dateRange = DateRange("2022-01-01", "+5D")
 * </pre>
 * </p>
 * <p>e.g.
 * <pre>
 *   val dateRange = DateRange("2022-01-01", "2022-01-06")
 * </pre>
 * </p>
 * @param from The lower bound.
 * @param until The upper bound.
 */
case class DateRange(from: LocalDateTime, until: LocalDateTime)


object DateRange {

  /**
   * Creates a DateRange from two dates, given in any order, passed as java.time.LocalDateTime
   * @param firstDate One of the two bounds.
   * @param secondDate The other bound.
   * @return A new DateRange with the two bounds ordered.
   */
  def apply(limit1: LocalDateTime, limit2: LocalDateTime) : DateRange = {
    val (from, until) = (minTime(limit1, limit2), maxTime(limit1, limit2))
    new DateRange(from, until)
  }

  /**
   * Creates a DateRange from two dates, given in any order, passed as java.time.LocalDate,
   *  with the corresponding timing set to midnight
   * @param firstDate One of the two bounds.
   * @param secondDate The other bound.
   * @return A new DateRange with the two bounds ordered.
   */
  def apply(limit1: LocalDate, limit2: LocalDate) : DateRange = {
    apply(limit1.atStartOfDay(), limit2.atStartOfDay())
  }

  /**
   * Creates a DateRange from a reference ad an offset, e.g. 2020-10-09, +5D
   * @param firstDate One of the two bounds.
   * @param secondDate The other bound.
   * @return A new DateRange with the two bounds ordered.
   */
  def apply(reference: String, offset: String = "") : DateRange = {
    apply(Time.produceTime(reference), Time.produceTime(reference + " " + offset))
  }

  /**
   * Creates a DateRange from a date and time. The second bound will correspond to Now()
   * @param limit The fixed limit
   * @return A new DateRange with the two bounds ordered.
   */
  def apply(limit: LocalDateTime) : DateRange = {
    apply(limit, LocalDateTime.now)
  }
}
