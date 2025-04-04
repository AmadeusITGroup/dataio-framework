package com.amadeus.dataio.config.fields

import com.amadeus.dataio.core.time.DateRange
import com.typesafe.config.Config

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Calendar
import scala.util.{Failure, Success, Try}

/** Trait for configuring and resolving paths with template variables.
  *
  * This trait provides functionality to parse paths from configuration objects,
  * supporting template variables for dates, UUIDs, and datetime values.
  *
  * Template variables supported:
  *  - %{uuid}: Replaced with a random UUID
  *  - %{from}: Replaced with the first day of a configured date range
  *  - %{to}: Replaced with the last day of a configured date range
  *  - %{datetime}: Replaced with a configured datetime
  *  - %{year}: Replaced with the year from a configured date
  *  - %{month}: Replaced with the month from a configured date
  *  - %{day}: Replaced with the day from a configured date
  */
trait PathConfigurator {

  /** @param config The typesafe Config object holding the configuration.
    * @return The path, detemplatized if necessary.
    */
  def getPath(implicit config: Config): Option[String] = {
    val pathTry = Try {
      config.getString("path")
    } orElse Try {
      handleTemplate
    }

    pathTry.toOption
  }

  private def handleTemplate(implicit config: Config): String = {
    var template = config.getString("path.template")

    template = detemplatizeFromDate(template)
    template = detemplatizeToDate(template)
    template = detemplatizeDate(template)
    template = detemplatizeUuid(template)
    template = detemplatizeDateComponents(template)

    template
  }

  /** Replaces %{from} from a string template by the first day of the date range provided in the configuration.
    * @param template The string template to interpolate.
    * @param config The typesafe Config object holding the configuration.
    * @return The interpolated string template.
    * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
    *                                             for the expected fields.
    */
  private def detemplatizeFromDate(template: String)(implicit config: Config): String = {
    if (!template.contains("%{from}")) return template

    val referenceDate = config.getString("path.date_reference")
    val offset        = config.getString("path.date_offset")
    val pattern       = config.getString("path.date_pattern")

    Try {
      val dateRange = DateRange(referenceDate, offset)
      val formatter = DateTimeFormatter.ofPattern(pattern)
      val fromDate  = dateRange.from.format(formatter)

      template.replace("%{from}", fromDate)
    } match {
      case Failure(e) =>
        throw new Exception(
          s"Can not interpolate %{from} for $template using ($referenceDate, $offset). " +
            s"The error is: ${e.getMessage}"
        )
      case Success(v) => v
    }
  }

  /** Replaces %{to} from a string template by the last day of the date range provided in the configuration.
    * @param template The string template to interpolate.
    * @param config The typesafe Config object holding the configuration.
    * @return The interpolated string template.
    * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
    *                                             for the expected fields.
    */
  private def detemplatizeToDate(template: String)(implicit config: Config): String = {
    if (!template.contains("%{to}")) return template

    val referenceDate = config.getString("path.date_reference")
    val offset        = config.getString("path.date_offset")
    val pattern       = config.getString("path.date_pattern")

    Try {
      val dateRange = DateRange(referenceDate, offset)
      val formatter = DateTimeFormatter.ofPattern(pattern)
      val toDate    = dateRange.until.format(formatter)

      template.replace("%{to}", toDate)
    } match {
      case Failure(e) =>
        throw new Exception(
          s"Can not interpolate %{to} for $template using ($referenceDate, $offset). " +
            s"The error is: ${e.getMessage}"
        )
      case Success(v) => v
    }
  }

  /** Replaces %{date} from a string template by the datetime from the configuration
    * @param template The string template to interpolate.
    * @param config The typesafe Config object holding the configuration.
    * @return The interpolated string template.
    */
  private def detemplatizeDate(template: String)(implicit config: Config): String = {
    if (!template.contains("%{date}")) return template

    val timestamp = parseDateAsString
    template.replace("%{date}", timestamp)
  }

  /** Replaces %{uuid} from a string template by a random UUID.
    * @param template The string template to interpolate.
    * @return The interpolated string template.
    */
  private def detemplatizeUuid(template: String): String = {
    import java.util.UUID.randomUUID

    if (!template.contains("%{uuid}")) return template

    val uuid = randomUUID().toString

    template.replace("%{uuid}", uuid)
  }

  /** Replaces %{year}, %[month}, %{day} in a string with the values from a date, specified via the configuration
    * @param template The string template to interpolate.
    * @param config The typesafe Config object holding the configuration.
    * @return The interpolated string template.
    */
  private def detemplatizeDateComponents(template: String)(implicit config: Config): String = {

    val allowedPlaceholders = Seq("%{year}", "%{month}", "%{day}")

    // there must be at least one of the allowed placeholders in order to proceed
    if (!allowedPlaceholders.map(p => template.contains(p)).foldLeft(false)(_ || _)) return template

    val localDateTime = parseDateAsLocalDateTime

    // fill the template
    // extend with other fields as needed
    template
      .replace("%{year}", "%04d".format(localDateTime.getYear))
      .replace("%{month}", "%02d".format(localDateTime.getMonthValue))
      .replace("%{day}", "%02d".format(localDateTime.getDayOfMonth))
  }

  /** Extract the date from the configuration, and return it as a formatted string.
    * If path.date_reference and path.date_pattern are both present, they drive the date and the format of the string.
    * If only path.date_pattern is present, the date defaults to Now(), with the given format.
    * If only path.date_reference is present, the format defaults to yyyyMMdd with the given date.
    * In any other case, the date defaults to Now() with the default format yyyyMMdd.
    *
    * @param config The typesafe Config object holding the configuration.
    * @return The formatted string holding the date
    */
  private def parseDateAsString(implicit config: Config): String = {
    val timestamp: Option[String] = Try(config.getString("path.date")).toOption
    val pattern: Option[String]   = Try(config.getString("path.date_pattern")).toOption
    val defaultFormatter          = DateTimeFormatter.ofPattern("yyyyMMdd")
    val currentTime               = Calendar.getInstance.getTime.toInstant.atZone(ZoneId.systemDefault()).toLocalDateTime

    (timestamp, pattern) match {
      case (Some(t), Some(p)) =>
        val formatter        = DateTimeFormatter.ofPattern(p)
        val l: LocalDateTime = DateRange(t).from
        formatter.format(l)
      case (None, Some(p)) =>
        val formatter = DateTimeFormatter.ofPattern(p)
        val l         = Calendar.getInstance.getTime.toInstant.atZone(ZoneId.systemDefault()).toLocalDateTime
        formatter.format(l)
      case (Some(t), _) => defaultFormatter.format(DateRange(t).from)
      case (_, _)       => defaultFormatter.format(currentTime)

    }
  }

  /** Extract the date from the configuration, and return it as a LocalDateTime
    * If Path.Date and Path.DatePattern are both present, they drive the value of the LocalDateTimr
    * In any other case, the date defaults to Now()
    * @param config The typesafe Config object holding the configuration.
    * @return The LocalDateTime object holding the date
    */
  private def parseDateAsLocalDateTime(implicit config: Config): LocalDateTime = {
    val timestamp: Option[String] = Try(config.getString("path.date")).toOption
    timestamp match {
      case Some(t) => DateRange(t).from
      case _       => Calendar.getInstance.getTime.toInstant.atZone(ZoneId.systemDefault()).toLocalDateTime
    }
  }

}
