package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

import scala.util.Try

trait NameConfigurator {

  /** @param config The typesafe Config object holding the configuration.
    * @return The name, or None.
    */
  def getName(implicit config: Config): Option[String] = {
    Try {
      config.getString("name")
    }.toOption
  }
}
