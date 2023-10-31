package com.amadeus.dataio.config.fields

import com.amadeus.dataio.core.time.DateRange
import com.typesafe.config.Config

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Calendar
import scala.util.{Failure, Success, Try}

trait PathConfigurator {

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return The path, detemplatized if necessary.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   * @throws IllegalArgumentException If one of the templates additional arguments (ex: DateOffset) is not formatted
   *                                  properly.
   */
  def getPath(implicit config: Config): String = {
    Try {
      config.getString("Path")
    } getOrElse {
      handleTemplate
    }
  }

  /**
   * Handle a templated path with the relevant text replacements
   * @param config The typesafe Config object holding the configuration.
   * @return the path with the relevant replacements
   */
  private def handleTemplate(implicit config: Config): String = {
    var template = config.getString("Path.Template")

    template = detemplatizeFromDate(template)
    template = detemplatizeToDate(template)
    template = detemplatizeDatetime(template)
    template = detemplatizeUuid(template)
    template = detemplatizeDate(template)

    template
  }

  /**
   * Replaces %{from} from a string template by the first day of the date range provided in the configuration.
   * @param template The string template to interpolate.
   * @param config The typesafe Config object holding the configuration.
   * @return The interpolated string template.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  private def detemplatizeFromDate(template: String)(implicit config: Config): String = {
    if (!template.contains("%{from}")) return template

    val referenceDate = config.getString("Path.Date")
    val offset        = config.getString("Path.DateOffset")
    val pattern       = config.getString("Path.DatePattern")

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

  /**
   * Replaces %{to} from a string template by the last day of the date range provided in the configuration.
   * @param template The string template to interpolate.
   * @param config The typesafe Config object holding the configuration.
   * @return The interpolated string template.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  private def detemplatizeToDate(template: String)(implicit config: Config): String = {
    if (!template.contains("%{to}")) return template

    val referenceDate = config.getString("Path.Date")
    val offset        = config.getString("Path.DateOffset")
    val pattern       = config.getString("Path.DatePattern")

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

  /**
   * Replaces %{datetime} from a string template by the datetime from the configuration
   * @param template The string template to interpolate.
   * @param config The typesafe Config object holding the configuration.
   * @return The interpolated string template.
   */
  private def detemplatizeDatetime(template: String)(implicit config: Config): String = {

    if (!template.contains("%{datetime}")) return template

    val timestamp = parseDateAsString
    template.replace("%{datetime}", timestamp)

  }

  /**
   * Replaces %{uuid} from a string template by a random UUID.
   * @param template The string template to interpolate.
   * @return The interpolated string template.
   */
  private def detemplatizeUuid(template: String): String = {
    import java.util.UUID.randomUUID

    if (!template.contains("%{uuid}")) return template

    val uuid = randomUUID().toString

    template.replace("%{uuid}", uuid)
  }

  /**
   * Replaces %{year}, %[month}, %{day} in a string with the values from a date, specified via the configuration
   * @param template The string template to interpolate.
   * @param config The typesafe Config object holding the configuration.
   * @return The interpolated string template.
   */
  private def detemplatizeDate(template: String)(implicit config: Config): String = {

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

  /**
   * Extract the date from the configuration, and return it as a formatted string
   * If Path.Date and Path.DatePattern are both present, they drive the date and the format of the string
   * If only Path.DatePattern is present, the date defaults to Now(), with the given format
   * In any other case, the date defaults to Now() with the default format yyyy-MM-ddTHH:mm:ss.SSS
   * @param config The typesafe Config object holding the configuration.
   * @return The formatted string holding the date
   */
  private def parseDateAsString(implicit config: Config): String = {
    val timestamp: Option[String] = Try(config.getString("Path.Date")).toOption
    val pattern: Option[String]   = Try(config.getString("Path.DatePattern")).toOption

    (timestamp, pattern) match {
      case (Some(t), Some(p)) =>
        val formatter        = DateTimeFormatter.ofPattern(p)
        val l: LocalDateTime = DateRange(t).from
        formatter.format(l)
      case (None, Some(p)) =>
        val formatter = DateTimeFormatter.ofPattern(p)
        val l         = Calendar.getInstance.getTime.toInstant.atZone(ZoneId.systemDefault()).toLocalDateTime
        formatter.format(l)
      case (_, _) =>
        val l         = Calendar.getInstance.getTime.toInstant.atZone(ZoneId.systemDefault()).toLocalDateTime
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
        formatter.format(l)

    }
  }

  /**
   * Extract the date from the configuration, and return it as a LocalDateTime
   * If Path.Date and Path.DatePattern are both present, they drive the value of the LocalDateTimr
   * In any other case, the date defaults to Now()
   * @param config The typesafe Config object holding the configuration.
   * @return The LocalDateTime object holding the date
   */
  private def parseDateAsLocalDateTime(implicit config: Config): LocalDateTime = {
    val timestamp: Option[String] = Try(config.getString("Path.Date")).toOption

    timestamp match {
      case Some(t) => DateRange(t).from
      case _ => Calendar.getInstance.getTime.toInstant.atZone(ZoneId.systemDefault()).toLocalDateTime
    }
  }

}
