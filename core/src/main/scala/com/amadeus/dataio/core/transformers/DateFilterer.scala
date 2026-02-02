package com.amadeus.dataio.core.transformers

import com.amadeus.dataio.config.fields.DateFilterConfig
import com.amadeus.dataio.core.Logging
import com.amadeus.dataio.core.time.DateRange
import org.apache.spark.sql.{Column, Dataset}

import java.sql.Date
import java.time.{LocalDate, LocalTime}

trait DateFilterer extends Logging {
  val dateFilterConfig: Option[DateFilterConfig]
  val dateColumn: Option[Column]

  def applyDateFilter[T](ds: Dataset[T]): Dataset[T] = {
    (dateFilterConfig, dateColumn) match {
      case (Some(config), Some(column)) =>
        applyFilter(ds, column, config)
      case (Some(_), None) =>
        throw new Exception("date_filter requires a date column")
      case (None, Some(_)) =>
        throw new Exception("date_filter requires a date configuration")
      case (_, _) =>
        ds
    }
  }

  private def applyFilter[T](ds: Dataset[T], column: Column, filterConfig: DateFilterConfig): Dataset[T] = {
    filterConfig match {
      case DateFilterConfig.Range(range) =>
        applyRangeFilter(ds, column, range)

      case DateFilterConfig.FromOnly(fromStr) =>
        applyFromFilter(ds, column, fromStr)

      case DateFilterConfig.UntilOnly(untilStr) =>
        applyUntilFilter(ds, column, untilStr)
    }
  }

  private def applyRangeFilter[T](ds: Dataset[T], column: Column, range: DateRange): Dataset[T] = {
    val dateFrom = Date.valueOf(range.from.toLocalDate)

    val dateUntil =
      if (range.until.toLocalTime.isAfter(LocalTime.of(0, 0)))
        Date.valueOf(range.until.plusDays(1).toLocalDate)
      else
        Date.valueOf(range.until.toLocalDate)

    logger.info(s"date_filter: $column >= $dateFrom AND $column < $dateUntil")
    ds.filter(column >= dateFrom && column < dateUntil)
  }

  private def applyFromFilter[T](ds: Dataset[T], column: Column, dateFrom: LocalDate): Dataset[T] = {
    val from = Date.valueOf(dateFrom)

    logger.info(s"date_filter: $column >= $from")
    ds.filter(column >= from)
  }

  private def applyUntilFilter[T](ds: Dataset[T], column: Column, dateUntil: LocalDate): Dataset[T] = {
    val until = Date.valueOf(dateUntil)

    logger.info(s"date_filter: $column < $until")
    ds.filter(column < until)
  }
}

object DateFilterer {
  def apply[T](filterConfig: Option[DateFilterConfig], column: Option[Column]): Dataset[T] => Dataset[T] = {
    new DateFilterer {
      override val dateFilterConfig: Option[DateFilterConfig] = filterConfig
      override val dateColumn: Option[Column]   = column
    }.applyDateFilter
  }
}
