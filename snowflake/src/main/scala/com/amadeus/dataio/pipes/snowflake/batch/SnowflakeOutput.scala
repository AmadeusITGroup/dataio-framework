package com.amadeus.dataio.pipes.snowflake.batch

import com.amadeus.dataio.config.fields._
import com.amadeus.dataio.core.{Logging, Output}
import com.amadeus.dataio.pipes.snowflake.SnowflakeCommons.SNOWFLAKE_CONNECTOR_NAME
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

/** Allows to write data to Snowflake.
 *
 * @param config the config object.
 */
case class SnowflakeOutput(
                            name: String,
    mode: String,
                            options: Map[String, String] = Map(),
    config: Config = ConfigFactory.empty()
) extends Output
    with Logging {

  override def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {
    logger.info(s"reading: $name")
    if (options.nonEmpty) logger.info(s"options: $options")
    logger.info(s"mode: $mode")

    data.write
      .format(SNOWFLAKE_CONNECTOR_NAME)
      .options(options)
      .mode(mode)
      .save()
  }
}

object SnowflakeOutput {

  /** Creates a new instance of SnowflakeOutput from a typesafe Config object.
   *
   * @param config typesafe Config object containing the configuration fields.
   * @return a new SnowflakeOutput object.
   * @throws com.typesafe.config.ConfigException If any of the mandatory fields is not available in the config argument.
   */
  def apply(implicit config: Config): SnowflakeOutput = {
    val name = Try {
      config.getString("name")
    } getOrElse {
      throw new Exception("Missing required `name` field in configuration.")
    }

    val mode = Try {
      config.getString("mode")
    } getOrElse {
      throw new Exception("Missing required `mode` field in configuration.")
    }

    SnowflakeOutput(name, mode = mode, options = getOptions, config = config)
  }

}
