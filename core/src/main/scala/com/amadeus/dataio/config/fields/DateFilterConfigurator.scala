package com.amadeus.dataio.config.fields

import com.amadeus.dataio.core.Logging
import com.amadeus.dataio.core.time.DateRange
import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import java.time.LocalDate
import scala.util.Try

trait DateFilterConfigurator extends Logging{
  def getDateFilterConfig(implicit config: Config): Option[DateFilterConfig] = {
    if (!config.hasPath("date_filter")) {
      return None
    }

    val filterConfig = config.getConfig("date_filter")

    val referenceOffsetConfig = getReferenceOffset(filterConfig)
    val fromUntilConfig = getFromUntil(filterConfig)

    (referenceOffsetConfig, fromUntilConfig) match {
      case (Some(_), Some(_)) =>
        throw new IllegalArgumentException(
          "date_filter: Cannot use both reference+offset and from/until syntaxes simultaneously"
        )

      case (Some(config), None) =>
        Some(config)

      case (None, Some(config)) =>
        Some(config)

      case (None, None) =>
        logger.warn("date_filter: configuration found but no valid syntax detected (expected reference+offset or from/until).")
        None
    }
  }

  private def getFromUntil(config: Config): Option[DateFilterConfig] = {
    val hasFrom = config.hasPath("from")
    val hasUntil = config.hasPath("until")

    (hasFrom, hasUntil) match {
      case (true, false) =>
        val from = LocalDate.parse(config.getString("from"))
        Some(DateFilterConfig.FromOnly(from))
      case (false, true) =>
        val until = LocalDate.parse(config.getString("until"))
        Some(DateFilterConfig.UntilOnly(until))
      case (true, true) =>
        val from = LocalDate.parse(config.getString("from"))
        val until = LocalDate.parse(config.getString("until"))

        if (!from.isBefore(until)) {
          throw new IllegalArgumentException(
            s"date_filter: 'from' ($from) must be before 'until' ($until). " +
              s"For a single day, use: from = '$from', until = '${from.plusDays(1)}'"
          )
        }
        Some(DateFilterConfig.Range(DateRange(from, until)))
      case _ => None
    }
  }

  private def getReferenceOffset(config: Config): Option[DateFilterConfig] = {
    val hasReference = config.hasPath("reference")
    val hasOffset = config.hasPath("offset")

    (hasReference, hasOffset) match {
      case (true, true) =>
        val reference = config.getString("reference")
        val offset = config.getString("offset")
        Some(DateFilterConfig.Range(DateRange(reference, offset)))
      case (true, false) | (false, true) =>
        throw new IllegalArgumentException(
          "date_filter with reference/offset requires both 'reference' and 'offset'"
        )
      case _ =>
        None
    }
  }

  /** @param config The typesafe Config object holding the configuration.
    * @return The column to filter by dates with, or None.
    */
  def getDateFilterColumn(implicit config: Config): Option[Column] = {
    Try {
      col(config.getString("date_filter.column"))
    }.toOption
  }
}

sealed trait DateFilterConfig

object DateFilterConfig {
  case class Range(dateRange: DateRange) extends DateFilterConfig
  case class FromOnly(from: LocalDate) extends DateFilterConfig  // >= from (inclusive)
  case class UntilOnly(until: LocalDate) extends DateFilterConfig // < until (exclusive)
}
