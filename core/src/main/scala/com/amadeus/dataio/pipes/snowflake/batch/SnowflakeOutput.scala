package com.amadeus.dataio.pipes.snowflake.batch

import com.amadeus.dataio.config.fields._
import com.amadeus.dataio.core.{Logging, Output}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Allows to write data to Snowflake.
 *
 * @param mode the mode to use.
 * @param options the snowflake connector options.
 * @param config the config object.
 */
case class SnowflakeOutput(
                            mode: String,
                            options: Map[String, String],
                            config: Config = ConfigFactory.empty()
                          ) extends Output
  with Logging {

  val SNOWFLAKE_CONNECTOR_NAME = "net.snowflake.spark.snowflake"


  /**
   * Writes data to this output.
   *
   * @param data  The data to write.
   * @param spark The SparkSession which will be used to write the data.
   */
  override def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {

    data.write
      .format(SNOWFLAKE_CONNECTOR_NAME)
      .options(options)
      .mode(mode)
      .save()
  }
}

object SnowflakeOutput {

  /**
   * Creates a new instance of SnowflakeOutput from a typesafe Config object.
   *
   * @param config typesafe Config object containing the configuration fields.
   * @return a new SnowflakeOutput object.
   * @throws com.typesafe.config.ConfigException If any of the mandatory fields is not available in the config argument.
   */
  def apply(implicit config: Config): SnowflakeOutput = {

    val mode = config.getString("Mode")

    SnowflakeOutput(mode = mode, options = getOptions, config = config)
  }

}