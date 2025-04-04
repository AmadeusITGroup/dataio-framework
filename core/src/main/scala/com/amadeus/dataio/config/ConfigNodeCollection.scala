package com.amadeus.dataio.config

import com.amadeus.dataio.core.Logging
import com.typesafe.config.Config

import scala.util.{Failure, Success, Try}

/** Holds a sequence of ConfigNode objects.
  * @param nodes The sequence of ConfigNode.
  */
case class ConfigNodeCollection(
    nodes: Seq[ConfigNode]
) {

  override def toString: String = {
    s"[${nodes.map(c => s"{${c.name}: ${c.typeName}}").mkString(", ")}]"
  }
}

object ConfigNodeCollection extends Logging {

  /** @param nodeName The name of the nodes collection in the config argument.
    * @param config The typesafe Config holding the configuration.
    * @return A new ConfigNodeCollection. The nodes equal to `Nil` if the nodeName can't be found or is not valid in the
    *         config argument.
    */
  def apply(nodeName: String, config: Config): ConfigNodeCollection = {
    import collection.JavaConverters._

    if (!config.hasPath(nodeName))
      return ConfigNodeCollection(Nil)

    val rawConfigs: Seq[Config] =
      Try {
        config.getConfigList(nodeName).asScala
      } orElse Try {
        config.getConfig(nodeName) +: Nil
      } getOrElse {
        throw new Exception("A configuration node must be a List or an Object.")
      }

    if (rawConfigs == Nil) {
      logger.warn(s"A $nodeName node was found in the configuration, but it is empty.")
      return ConfigNodeCollection(Nil)
    }

    val configNodeCollection = ConfigNodeCollection(
      rawConfigs.map(rawConfig => {
        val node = Try(ConfigNode(rawConfig)) match {
          case Failure(ex)           => throw new Exception(s"$nodeName child node configuration failed", ex)
          case Success(parsedConfig) => parsedConfig
        }
        node
      })
    )

    logger.debug(s"$nodeName configuration: $configNodeCollection")

    configNodeCollection
  }
}
