package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

import scala.util.Try

/** Retrieve schema full qualified name
  */
trait SchemaConfigurator {

  /** @param config The typesafe Config object holding the configuration.
    * @return The schema full qualified name, or None.
    */
  def getSchema(implicit config: Config): Option[String] = {
    Try {
      config.getString("schema")
    }.toOption
  }
}
