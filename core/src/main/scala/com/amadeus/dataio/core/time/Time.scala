package com.amadeus.dataio.core.time

import com.amadeus.dataio.core.time.Formats.LabeledFormatter
import com.amadeus.dataio.core.time.RangeType._
import com.amadeus.dataio.core.time.Modifiers.ModifierString
import java.time.{LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import scala.util.Try


/**
 * This object exposes helper methods for working on Time objects.
 */
object Time {

  /**
   * Function that returns the first time stamp on chronological order
   * @param instant1 a time stamp
   * @param instant2 another time stamp
   * @return first time stamp on chronological order
   */
  def minTime(instant1: LocalDateTime, instant2: LocalDateTime): LocalDateTime = {
    if (instant1.isBefore(instant2)) {
      instant1
    } else {
      instant2
    }
  }

  /**
   * Function that returns the second time stamp on chronological order
   * @param instant1 a time stamp
   * @param instant2 another time stamp
   * @return second time stamp on chronological order
   */
  def maxTime(instant1: LocalDateTime, instant2: LocalDateTime): LocalDateTime = {
    if (instant1.isAfter(instant2)) {
      instant1
    } else {
      instant2
    }
  }

  /**
   * Function that returns a map of key-value pairs based on NOW and a prefix
   *
   * @param prefix a prefix string that is added to each key
   * @return map of key-value pairs based on a time stamp
   */
  def getTimeMap(prefix: Option[String]): Map[String, String] = {
    getTimeMap(LocalDateTime.now(), prefix)
  }

  /**
   * Function that returns a map of key-value pairs based on a time stamp and a prefix
   *
   * @param timestamp a time stamp (by default is NOW)
   * @param prefix an optional prefix string that is added to each key
   * @return map of key-value pairs based on a time stamp
   */
  def getTimeMap(timestamp: LocalDateTime = LocalDateTime.now(), prefix: Option[String] = None): Map[String, String] = {
    def addPrefix(label: String): String = prefix match {
      case Some(p) => p + "." + label
      case None    => label
    }
    Formats.AllFormats.map(f => (addPrefix(f.label), timestamp.formatted(f))).toMap
  }

  /**
   * Class used to extend LocalDateTime methods
   */
  implicit class DateImplicits(time: LocalDateTime) {

    /**
     * Use to get a time string based upon the chosen formatter
     * @param formatter a labeled formatter
     * @return time string based upon the formatter
     */
    def formatted(formatter: LabeledFormatter): String = {
      ZonedDateTime.of(time, ZoneId.of("Z")).format(formatter.formatter)
    }

    /**
     * Truncate a given time stamp to second (example: 2015-09-17T13:35:27.957Z -> 2015-09-17T13:35:27.000Z)
     * @return a time stamp
     */
    def truncSecond: LocalDateTime = {
      time.withNano(0)
    }

    /**
     * Truncate a given time stamp to minute (example: 2015-09-17T13:35:27.957Z -> 2015-09-17T13:35:00.000Z)
     * @return a time stamp
     */
    def truncMinute: LocalDateTime = {
      time.truncSecond.withSecond(0)
    }

    /**
     * Truncate a given time stamp to hour (example: 2015-09-17T13:35:27.957Z -> 2015-09-17T13:00:00.000Z)
     * @return a time stamp
     */
    def truncHour: LocalDateTime = {
      time.truncMinute.withMinute(0)
    }

    /**
     * Truncate a given time stamp to day (example: 2015-09-17T13:35:27.957Z -> 2015-09-17T00:00:00.000Z)
     * @return a time stamp
     */
    def truncDay: LocalDateTime = {
      time.truncHour.withHour(0)
    }

    /**
     * Truncate a given time stamp to month (example: 2015-09-17T13:35:27.957Z -> 2015-09-01T00:00:00.000Z)
     * @return a time stamp
     */
    def truncMonth: LocalDateTime = {
      time.truncDay.withDayOfMonth(1)
    }

    /**
     * Truncate a given time stamp to year (example: 2015-09-17T13:35:27.957Z -> 2015-01-01T00:00:00.000Z)
     * @return a time stamp
     */
    def truncYear: LocalDateTime = {
      time.truncMonth.withMonth(1)
    }

    /**
     * Truncate a given time stamp using a modifier
     *
     * @return a time stamp
     * @throws IllegalArgumentException when the modifier is incorrect
     */
    def truncate(modifier: String): LocalDateTime = {
      modifier.getModifier match {
        case Modifiers.TruncSecond => time.truncSecond
        case Modifiers.TruncMinute => time.truncMinute
        case Modifiers.TruncHour   => time.truncHour
        case Modifiers.TruncDay    => time.truncDay
        case Modifiers.TruncMonth  => time.truncMonth
        case Modifiers.TruncYear   => time.truncYear
        case _                     => throw new IllegalArgumentException(modifier)
      }
    }

    /**
     * Apply a delta to a time stamp using a modifier.
     * @param modifier a string with syntax: (+|-)number(time pattern symbol) -> examples: +5D -3W -1Y +6M -25m +1H -30s
     * @return a time stamp
     * @throws IllegalArgumentException when the modifier is incorrect
     */
    def applyDelta(modifier: String): LocalDateTime = {
      val sign   = modifier.substring(0, 1)
      val letter = modifier.substring(modifier.length - 1)
      val delta  = Try(Integer.parseInt(modifier.substring(1, modifier.length - 1)))
      if (delta.isSuccess) {
        if (sign.equals("-")) {
          letter.getModifier match {
            case Modifiers.LetterSecond => time.minusSeconds(delta.get)
            case Modifiers.LetterMinute => time.minusMinutes(delta.get)
            case Modifiers.LetterHour   => time.minusHours(delta.get)
            case Modifiers.LetterDay    => time.minusDays(delta.get)
            case Modifiers.LetterWeek   => time.minusWeeks(delta.get)
            case Modifiers.LetterMonth  => time.minusMonths(delta.get)
            case Modifiers.LetterYear   => time.minusYears(delta.get)
            case _                      => throw new IllegalArgumentException(modifier)
          }
        } else if (sign.equals("+")) {
          letter.getModifier match {
            case Modifiers.LetterSecond => time.plusSeconds(delta.get)
            case Modifiers.LetterMinute => time.plusMinutes(delta.get)
            case Modifiers.LetterHour   => time.plusHours(delta.get)
            case Modifiers.LetterDay    => time.plusDays(delta.get)
            case Modifiers.LetterWeek   => time.plusWeeks(delta.get)
            case Modifiers.LetterMonth  => time.plusMonths(delta.get)
            case Modifiers.LetterYear   => time.plusYears(delta.get)
            case _                      => throw new IllegalArgumentException(modifier)
          }
        } else {
          throw new IllegalArgumentException(modifier)
        }
      } else {
        throw new IllegalArgumentException(modifier)
      }
    }

    /**
     * Set a time property (second, minute, etc...) to a required value.
     * @param letter a time pattern symbol -> examples: D, W, Y, M, m, H, s
     * @param right a numeric value that you want to set
     * @return a time stamp
     * @throws IllegalArgumentException when the letter or numeric value are incorrect
     */
    def applySetter(letter: String, right: String): LocalDateTime = {
      val value = Try(Integer.parseInt(right))
      if (value.isSuccess) {
        letter.getModifier match {
          case Modifiers.LetterSecond => time.withSecond(value.get)
          case Modifiers.LetterMinute => time.withMinute(value.get)
          case Modifiers.LetterHour   => time.withHour(value.get)
          case Modifiers.LetterDay    => time.withDayOfMonth(value.get)
          case Modifiers.LetterMonth  => time.withMonth(value.get)
          case Modifiers.LetterYear   => time.withYear(value.get)
          case _                      => throw new IllegalArgumentException(letter)
        }
      } else {
        throw new IllegalArgumentException(right)
      }
    }

    /**
     * Determines if this time is between the two given time bounds. (ClosedClosed range type)
     * @param from the left bound
     * @param until the right bound
     * @return a Boolean
     */
    def isBetween(from: LocalDateTime, until: LocalDateTime): Boolean = {
      time.isBetween(from, until, ClosedClosed)
    }

    /**
     * Determines if this time is between the two given time bounds.
     * @param from the left bound
     * @param until the right bound
     * @param rangeType determines if the left bound and right bound are included in the range
     * @return a Boolean
     */
    def isBetween(from: LocalDateTime, until: LocalDateTime, rangeType: RangeType): Boolean = {
      require(!from.isAfter(until))
      time.isBetween(new DateRange(from, until), rangeType)
    }

    /**
     * Determines if this time is between the two given time bounds.
     * @param dateRange the date range containing the left and right time bounds
     * @param rangeType determines if the left bound and right bound are included in the range
     * @return a Boolean
     */
    def isBetween(dateRange: DateRange, rangeType: RangeType): Boolean = {
      (time.isBefore(dateRange.from), time.equals(dateRange.from), time.equals(dateRange.until), time.isAfter(dateRange.until)) match {
        case (true, _, _, _)              => false                                                // time, from before the left bound
        case (_, _, _, true)              => false                                                // after the right bound
        case (_, true, true, _)           => rangeType == ClosedClosed                            // left and right bound are the same time
        case (_, true, _, _)              => rangeType == ClosedOpen || rangeType == ClosedClosed // on the left bound
        case (_, _, true, _)              => rangeType == OpenClosed || rangeType == ClosedClosed // on the right bound
        case (false, false, false, false) => true                                                 // between bounds (both excluded)
      }
    }
  }

  /**
   * Function that take a time expression and produce a time stamp instance.
   * @param timeExpression a time expression formed by a reference followed by modifiers
   * @return a time stamp
   * @throws RuntimeException when the reference cannot be parsed as a time stamp
   */
  def produceTime(timeExpression: String): LocalDateTime = {
    require(timeExpression != null)
    val parameters = timeExpression.trim().split(" ")
    val timeStr    = parameters(0)
    val modifiers  = parameters.drop(1).filterNot { x => x.isEmpty() }

    val parsedDates = Formats.AllFormats.map { format => Try(format.formatter.parse(timeStr)) }.filter { parsedDate => parsedDate.isSuccess }
    if (parsedDates.isEmpty) {
      calculateTime(LocalDateTime.now(), modifiers)
    } else {
      var referenceTimes = parsedDates.map { parsedDate => Try(LocalDateTime.from(parsedDate.get)) }.filter { parsedDate => parsedDate.isSuccess }
      if (referenceTimes.isEmpty) {
        referenceTimes = parsedDates.map { parsedDate => Try(LocalDate.from(parsedDate.get).atStartOfDay()) }.filter { parsedDate => parsedDate.isSuccess }
      }
      if (referenceTimes.isEmpty) {
        throw new RuntimeException("Impossible to create a time stamp instance using the parsed objects. Investigation required: " + parsedDates.toString)
      } else {
        calculateTime(referenceTimes.last.get, modifiers)
      }
    }
  }

  /**
   * Function that calculate a time stamp using a list of modifiers.
   * Pay attention to modifiers order since the operation are not commutative.
   * @param reference a time stamp
   * @param modifiers a string listing time modifiers
   * @return a time stamp
   * @throws IllegalArgumentException when the modifiers are incorrect
   */
  def calculateTime(reference: LocalDateTime, modifiers: String): LocalDateTime = {
    calculateTime(reference, modifiers.trim().split(" "))
  }

  /**
   * Function that calculate a time stamp using a list of modifiers.
   * Pay attention to modifiers order since the operation are not commutative.
   * @param reference a time stamp
   * @param modifiers a list of modifiers
   * @return a time stamp
   * @throws IllegalArgumentException when the modifiers are incorrect
   */
  def calculateTime(reference: LocalDateTime, modifiers: Array[String] = Array.empty): LocalDateTime = {
    modifiers.foldLeft(reference) { (date, modifier) =>
      if (!modifier.isEmpty) {
        modifier.isTruncateModifier match {
          case true => date.truncate(modifier)
          case false => {
            val parts = modifier.split("=")
            parts.length match {
              case 1 => date.applyDelta(parts(0))
              case 2 => date.applySetter(parts(0), parts(1))
              case _ => throw new IllegalArgumentException(modifier)
            }
          }
        }
      } else {
        date
      }
    }
  }
}
