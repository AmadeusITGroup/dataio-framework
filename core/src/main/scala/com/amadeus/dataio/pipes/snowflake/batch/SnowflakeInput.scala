package com.amadeus.dataio.pipes.snowflake.batch

import com.amadeus.dataio.core.{Input, Logging}
import com.amadeus.dataio.config.fields._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class for reading Snowflake input
 *
 * @param options the snowflake connector options.
 * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
 */
case class SnowflakeInput(
                           options: Map[String, String],
                           config: Config = ConfigFactory.empty()
                         ) extends Input
  with Logging {

  val SNOWFLAKE_CONNECTOR_NAME = "net.snowflake.spark.snowflake"

  /**
   * Reads a batch of data from snowflake.
   *
   * @param spark The SparkSession which will be used to read the data.
   * @return The data that was read.
   * @throws Exception If the exactly one of the dateRange/dateColumn fields is None.
   */
  override def read(implicit spark: SparkSession): DataFrame = {
   spark.read.format(SNOWFLAKE_CONNECTOR_NAME).options(options).load()
  }

}

object SnowflakeInput {

  /**
   * Creates a new instance of SnowflakeInput from a typesafe Config object.
   *
   * @param config typesafe Config object containing the configuration fields.
   * @return a new SnowflakeInput object.
   * @throws com.typesafe.config.ConfigException If any of the mandatory fields is not available in the config argument.
   */
  def apply(implicit config: Config): SnowflakeInput = {
    SnowflakeInput(options = getOptions , config = config)
  }
}