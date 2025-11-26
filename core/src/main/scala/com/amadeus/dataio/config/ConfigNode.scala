package com.amadeus.dataio.config

import com.typesafe.config.Config

import java.util.UUID.randomUUID
import scala.util.Try

/** Generic node of configuration. It is used to store the configured of an entity of the application (input, output,
  * data processor, etc.)
  * @param name The name of the entity
  * @param typeName The name of the class of the entity.
  * @param config The configuration of the entity.
  */
case class ConfigNode(
    name: String,
    typeName: String,
    config: Config
) {
  override def toString: String = s"$name; $typeName; $config"
}

object ConfigNode {

  /** <p>Creates a ConfigNode from a typesafe Config object.</p>
    *
    * <p>Config fields (* mandatory):
    * <ul>
    *   <li>Type*: String </li>
    *   <li>Name: String</li>
    * </ul>
    * </p>
    * @param config The typesafe Config object holding the configuration. The `Type` field is mandatory, but if the
    *               `Name` field is not provided a name will be generated automatically: typeName-randomUUID. If
    *               additional fields are provided, they will be contained in the config field of the ConfigNode.
    * @return A new ConfigNode containing the configuration.
    * @throws Exception if the Type field is missing.
    */
  def apply(config: Config): ConfigNode = {
    val typeName = Try(config.getString("type")).toOption match {
      case Some(value) => value
      case _           => throw new Exception("Configuration missing a `type` field.")
    }

    val name = Try(config.getString("name")).getOrElse(s"$typeName-${randomUUID().toString}")

    ConfigNode(name, typeName, config.withoutPath("type"))
  }
}
