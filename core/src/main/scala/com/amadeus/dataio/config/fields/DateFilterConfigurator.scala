package com.amadeus.dataio.config.fields

import com.amadeus.dataio.core.time.DateRange
import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.util.Try

trait DateFilterConfigurator {

  /** @param config The typesafe Config object holding the configuration.
    * @return The date range, or None.
    * @throws IllegalArgumentException If the data was found but is not formatted properly.
    */
  def getDateFilterRange(implicit config: Config): Option[DateRange] = {
    // If only reference or offset is present for a given syntax, this is incorrect
    val syntaxTwoOk = testArguments("date_filter.reference", "date_filter.offset")

    // if any is false, we have an incomplete conf
    if (!syntaxTwoOk) {
      throw new IllegalArgumentException("Configuration incomplete for date filter reference/offset")
    } else {
      Try {
        getArguments("date_filter.reference", "date_filter.offset")
      }.toOption
    }
  }

  /** @param dateReferencePath the path in the config holding Date Reference
    * @param offsetPath the path in the config holding Date Offset
    * @return True if the config holds either both keys or none
    */
  private def testArguments(dateReferencePath: String, offsetPath: String)(implicit config: Config): Boolean = {
    // there must be both keys or none => true
    (config.hasPath(dateReferencePath) && config.hasPath(offsetPath)) ||
    (!config.hasPath(dateReferencePath) && !config.hasPath(offsetPath))

  }

  /** @param dateReferencePath the path in the config holding Date Reference
    * @param offsetPath        the path in the config holding Date Offset
    * @return The DateRange corresponding to the reference and the offset
    */
  private def getArguments(dateReferencePath: String, offsetPath: String)(implicit config: Config): DateRange = {
    val referenceDate = config.getString(dateReferencePath)
    val offset        = config.getString(offsetPath)
    DateRange(referenceDate, offset)
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
