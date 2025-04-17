package com.amadeus.dataio.pipes.snowflake.batch

import com.amadeus.dataio.core.{Input, Logging}
import com.amadeus.dataio.config.fields._
import com.amadeus.dataio.pipes.snowflake.SnowflakeCommons.SNOWFLAKE_CONNECTOR_NAME
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/** Class for reading Snowflake input.
 *
 * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
 */
case class SnowflakeInput(
                           name: String,
                           options: Map[String, String] = Map(),
    config: Config = ConfigFactory.empty()
) extends Input
    with Logging {
  override def read(implicit spark: SparkSession): DataFrame = {
    logger.info(s"reading: $name")
    if (options.nonEmpty) logger.info(s"options: $options")

    spark.read.format(SNOWFLAKE_CONNECTOR_NAME).options(options).load()
  }

}

object SnowflakeInput {

  /** Creates a new instance of SnowflakeInput from a typesafe Config object.
   *
   * @param config typesafe Config object containing the configuration fields.
   * @return a new SnowflakeInput object.
   * @throws com.typesafe.config.ConfigException If any of the mandatory fields is not available in the config argument.
   */
  def apply(implicit config: Config): SnowflakeInput = {
    val name = Try {
      config.getString("name")
    } getOrElse {
      throw new Exception("Missing required `name` field in configuration.")
    }

    SnowflakeInput(name, options = getOptions, config = config)
  }
}
