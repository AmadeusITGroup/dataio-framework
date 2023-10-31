package com.amadeus.dataio.config

import com.amadeus.dataio.core.Logging
import com.typesafe.config.{Config, ConfigFactory}

import java.io.File
import java.net.URL
import scala.util.Try

/**
 * Contains the configuration of the application.
 * @param info A config object holding information about the application (PrettyName, etc.).
 * @param processing The configuration of the ProcessingHandler.
 * @param input The configuration of the InputHandler.
 * @param output The configuration of the OutputHandler.
 * @param distribution The configuration of the DistributionHandler.
 */
case class PipelineConfig(
    info: Option[Config],
    processing: ConfigNodeCollection,
    input: ConfigNodeCollection,
    output: ConfigNodeCollection,
    distribution: ConfigNodeCollection
)

case object PipelineConfig extends Logging {

  /**
   * Sets up a pipeline configuration from typesafe Config object.
   * @param config The typesafe Config object.
   * @return A new PipelineConfig with the extracted configuration.
   * @throws Exception If no Processor configuration was provided.
   */
  def apply(config: Config): PipelineConfig = {

    val info       = Try(config.getConfig("Info")).toOption
    val processing = ConfigNodeCollection("Processing", config)
    if (processing.nodes.isEmpty)
      logger.warn("No processor configuration found.")
    val inputConfig        = ConfigNodeCollection("Input", config)
    val outputConfig       = ConfigNodeCollection("Output", config)
    val distributionConfig = ConfigNodeCollection("Distribution", config)

    PipelineConfig(info, processing, inputConfig, outputConfig, distributionConfig)
  }

  /**
   * Sets up a pipeline configuration from a config file at a given location.
   * @param configLocation The URL or path of the config file.
   * @return A new PipelineConfig with the extracted configuration.
   * @throws Exception If no Processor configuration was provided.
   */
  def apply(configLocation: String): PipelineConfig = {
    val config = Try {
      ConfigFactory.parseURL(new URL(configLocation))
    } getOrElse {
      ConfigFactory.parseFile(new File(configLocation))
    }

    PipelineConfig(config)
  }
}
