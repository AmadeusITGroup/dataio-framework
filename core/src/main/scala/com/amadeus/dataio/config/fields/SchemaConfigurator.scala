package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

/**
 * Retrieve schema full qualified name
 */
trait SchemaConfigurator {

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return the schema full qualified name.
   */
  def getSchema(implicit config: Config): String = {
    config.getString("Schema")
  }
}
