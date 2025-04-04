package com.amadeus.dataio.config.fields

import com.typesafe.config.Config

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.util.Try

trait OptionsConfigurator {

  /** @param config The typesafe Config object holding the configuration.
    * @return A Map[String, String] of the options.
    * @throws java.lang.IllegalArgumentException If the format of the config node is not valid.
    */
  def getOptions(implicit config: Config): Map[String, String] = {
    val optionsConfig = Try {
      config.getConfig("options")
    }.toOption

    try {
      optionsConfig match {
        case Some(o) => o.root.keySet.map(k => (k, o.getString("\"" + k + "\""))).toMap
        case None    => Map[String, String]()
      }
    } catch {
      case e: Exception => throw new IllegalArgumentException("Format of options is not valid", e)
    }
  }
}
