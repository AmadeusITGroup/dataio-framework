package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * Retrieve timeout value from configuration
 *
 * for backward compatibility timeout can be defined as
 *
 * Timeout = "2"
 * Which means 2 hours
 *
 * or using scala.concurrent.duration.Duration
 * Timeout = "2 minutes"
 */
trait TimeoutConfigurator {

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return the timeout value in millisecond
   */
  def getTimeout(implicit config: Config): Long = {
    Try {
      val timeoutInHours = config.getInt("Timeout")
      convertHoursInMillisecond(timeoutInHours)
    } getOrElse {
      Duration(config.getString("Timeout")).toMillis
    }
  }

  /**
   * convertHoursInMillisecond
   *
   * @param hours hours to convert.
   * @return hours converted in milliseconds.
   */
  private def convertHoursInMillisecond(hours: Int): Long = {
    hours * 60 * 60 * 1000
  }
}
