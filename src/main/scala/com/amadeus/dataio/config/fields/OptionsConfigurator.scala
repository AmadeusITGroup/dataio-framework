package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.util.Try

trait OptionsConfigurator {

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return A Map[String, String] of the options.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getOptions(implicit config: Config): Map[String, String] = {
    val optionsConfig = config.getConfig("Options")
    optionsConfig.root.keySet.map(k => (k, optionsConfig.getString("\"" + k + "\""))).toMap
  }
}
