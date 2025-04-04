package com.amadeus.dataio.pipes.kafka

import com.typesafe.config.Config

import scala.util.Try

/**
 * Basic kafka stream parameterization (brokers, topics)
 */
object KafkaConfigurator {

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return A Option[String] of the topic to read from kafka stream.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getTopic(implicit config: Config): Option[String] = {
    Try(config.getString("Topic")).toOption
  }

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return A String of the brokers to use for kafka.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getBroker(implicit config: Config): String = {
    config.getString("Brokers")
  }

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return A Option[String] of the pattern (subscribePattern) to read from kafka stream.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getPattern(implicit config: Config): Option[String] = {
    Try(config.getString("Pattern")).toOption
  }

  /**
   * @param config The typesafe Config object holding the configuration.
   * @return A Option[String] of the Specific TopicPartitions to consume from kafka stream.
   * @throws com.typesafe.config.ConfigException If data is missing in the config argument. See the user documentation
   *                                             for the expected fields.
   */
  def getAssign(implicit config: Config): Option[String] = {
    Try(config.getString("Assign")).toOption
  }
}
