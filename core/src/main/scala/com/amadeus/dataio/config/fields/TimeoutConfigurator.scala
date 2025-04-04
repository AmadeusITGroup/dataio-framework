package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

import scala.concurrent.duration.Duration
import scala.util.Try

/** Retrieve timeout value from configuration
  * === Example ===
  * timeout = "2 minutes"
  * @see [[Duration]]
  */
trait TimeoutConfigurator {

  /** @param config The typesafe Config object holding the configuration.
    * @return the timeout value in millisecond
    */
  def getTimeout(implicit config: Config): Option[Long] = {
    Try {
      println(config.getString("timeout"))
      Duration(config.getString("timeout")).toMillis
    }.toOption
  }
}
