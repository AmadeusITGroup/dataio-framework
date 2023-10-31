package com.amadeus.dataio.core.time

import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.{BASIC_ISO_DATE, ISO_LOCAL_DATE, ISO_ORDINAL_DATE, ISO_WEEK_DATE}
import java.time.temporal.ChronoField
import java.time.temporal.ChronoField._

/**
 * Collection of [[com.amadeus.bdp.api.functions.time.Formats.LabeledFormatter]].
 * Formats are related to dates and can be used as placeholder in a [[com.amadeus.bdp.api.functions.paths.TemplatePath]].
 *
 * @see Documentation for pattern symbols: [[https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html]]
 */
object Formats {

  /**
   * Label: iso-ordinal-date
   *
   * Format example: 2012-337Z
   */
  val IsoOrdinalDate = LabeledFormatter("iso-ordinal-date", ISO_ORDINAL_DATE, """(\d{4}-\d{3}Z)""", Set(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH))

  /**
   * Label: iso-week-date
   *
   * Format example: 2012-W48-6Z
   */
  val IsoWeekDate = LabeledFormatter("iso-week-date", ISO_WEEK_DATE, """(\d{4}-W\d{2}-\d{1}Z)""", Set(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH))

  /**
   * Label: iso-date-compact
   *
   * Format example: 20111203
   */
  val BasicIsoDate = LabeledFormatter("iso-date-compact", BASIC_ISO_DATE, """(\d{8})""", Set(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH))

  /**
   * Label: iso-date
   *
   * Format example: 2011-12-03
   */
  val IsoLocalDate = LabeledFormatter("iso-date", ISO_LOCAL_DATE, """(\d{4}-\d{2}-\d{2})""", Set(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH))

  /**
   * Label: iso-time
   *
   * Format example: 10.15.30
   */
  val IsoLocalTime = LabeledFormatter("iso-time", DateTimeFormatter.ofPattern("HH.mm.ss"), """(\d{2}.\d{2}.\d{2})""", Set(HOUR_OF_DAY, MINUTE_OF_HOUR, SECOND_OF_MINUTE))

  /**
   * Label: iso-8601
   *
   * Format example: 2011-12-03T10.15.30Z
   */
  val Iso8601 = LabeledFormatter(
    "iso-8601",
    DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH.mm.ss'Z'"),
    """(\d{4}-\d{2}-\d{2}T\d{2}.\d{2}.\d{2}Z)""",
    Set(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY, MINUTE_OF_HOUR, SECOND_OF_MINUTE)
  )

  /**
   * Label: iso-instant
   *
   * Format example: 2011-12-03T10.15.30Z
   */
  //the isoInstant of java has a format : 2011-12-03T10:15:30Z. It isn't wanted for hdfs directory. The ':' is problematic ...
  val IsoInstant = LabeledFormatter(
    "iso-instant",
    DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH.mm.ss'Z'"),
    """(\d{4}-\d{2}-\d{2}T\d{2}.\d{2}.\d{2}Z)""",
    Set(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY, MINUTE_OF_HOUR, SECOND_OF_MINUTE)
  )

  /**
   * Label: year
   *
   * Format example: 2017
   */
  val LbxYear = LabeledFormatter("year", DateTimeFormatter.ofPattern("uuuu"), """(\d{4})""", Set(YEAR))

  /**
   * Label: month
   *
   * Format example: 06
   */
  val LbxMonth = LabeledFormatter("month", DateTimeFormatter.ofPattern("MM"), """(\d{2})""", Set(MONTH_OF_YEAR))

  val LbxWeek = LabeledFormatter("week", DateTimeFormatter.ofPattern("ww"), """(\d{2})""", Set.empty)

  /**
   * Label: day
   *
   * Format example: 09
   */
  val LbxDay = LabeledFormatter("day", DateTimeFormatter.ofPattern("dd"), """(\d{2})""", Set(DAY_OF_MONTH))

  /**
   * Label: hour
   *
   * Format example: 15
   */
  val LbxHour = LabeledFormatter("hour", DateTimeFormatter.ofPattern("HH"), """(\d{2})""", Set(HOUR_OF_DAY))

  /**
   * Label: minute
   *
   * Format example: 41
   */
  val LbxMinute = LabeledFormatter("minute", DateTimeFormatter.ofPattern("mm"), """(\d{2})""", Set(MINUTE_OF_HOUR))

  /**
   * Label: second
   *
   * Format example: 29
   */
  val LbxSecond = LabeledFormatter("second", DateTimeFormatter.ofPattern("ss"), """(\d{2})""", Set(SECOND_OF_MINUTE))

  /**
   * Set of all [[com.amadeus.bdp.api.functions.time.Formats.LabeledFormatter LabeledFormatter]] defined into [[com.amadeus.bdp.api.functions.time.Formats Formats]] object
   */
  val AllFormats: Set[LabeledFormatter] =
    Set(BasicIsoDate, IsoLocalDate, IsoLocalTime, IsoOrdinalDate, IsoWeekDate, Iso8601, IsoInstant, LbxYear, LbxMonth, LbxWeek, LbxDay, LbxHour, LbxMinute, LbxSecond)

  /**
   * A labeled formatter is represented by a tuple with label, time formatter, a regular expression and a set of supported ChronoFields
   */
  case class LabeledFormatter(label: String, formatter: DateTimeFormatter, regex: String, chronoFields: Set[ChronoField])

  /**
   * Map of the formatters keyed by their label.
   */
  private val labelToFormatterMap = AllFormats.map(formatter => (formatter.label, formatter)).toMap

  /**
   * Gets a [[com.amadeus.bdp.api.functions.time.Formats.LabeledFormatter LabeledFormatter]] from its label.
   *
   * @param label the label
   * @return an [[scala.Option Option]] containing the formatter or [[scala.None None]] if there is no formatter for the given label
   */
  def getFromLabel(label: String): Option[LabeledFormatter] = labelToFormatterMap.get(label)
}
