package com.amadeus.dataio.config.fields

import com.amadeus.dataio.core.Logging
import com.amadeus.dataio.core.time.DateRange
import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.util.{Failure, Success, Try}

trait DateFilterConfigurator extends Logging {

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return The date range.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   * @throws IllegalArgumentException If the data was found but is not formatted properly.
   */
  def getDateFilterRange(implicit config: Config): DateRange = {
    // We can pass the DateRange with two different syntax
    // A syntax is considered functionally valid if both Reference and Offset are present, or if neither is
    // If only Reference or Offset is present for a given syntax, this is incorrect
    val syntaxOneOk = testArguments("DateReference", "DateOffset")
    val syntaxTwoOk = testArguments("DateFilter.Reference", "DateFilter.Offset")

    // if any is false, we have an incomplete conf
    if (!(syntaxOneOk && syntaxTwoOk)) {
      throw new IllegalArgumentException("Configuration incomplete for date reference/offset")
    }
    else {
      Try(getArguments("DateReference", "DateOffset")) match {
        case Success(d) => d
        case Failure(_) => getArguments("DateFilter.Reference", "DateFilter.Offset")
      }
    }
  }

  /**
   * @param dateReferencePath the path in the config holding Date Reference
   * @param offsetPath the path in the config holding Date Offset
   * @return True if the config holds either both keys or none
   */
  def testArguments(dateReferencePath: String, offsetPath: String)(implicit config: Config): Boolean = {
    // there must be both keys or none => true
    (config.hasPath(dateReferencePath) && config.hasPath(offsetPath)) ||
      (!config.hasPath(dateReferencePath) && !config.hasPath(offsetPath))

  }

  /**
   * @param dateReferencePath the path in the config holding Date Reference
   * @param offsetPath        the path in the config holding Date Offset
   * @return The DateRange corresponding to the reference and the offset
   */
  def getArguments(dateReferencePath: String, offsetPath: String)(implicit config: Config) : DateRange = {
      val referenceDate = config.getString(dateReferencePath)
      val offset = config.getString(offsetPath)
      DateRange(referenceDate, offset)
  }

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return The column to filter by dates with.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getDateFilterColumn(implicit config: Config): Column = {
    Try {
      col(config.getString("DateColumn"))
    } getOrElse {
      col(config.getString("DateFilter.Column"))
    }
  }
}
