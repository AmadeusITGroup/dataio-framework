package com.amadeus.dataio.pipes.snowflake.batch

import com.amadeus.dataio.config.fields.getOptions
import com.amadeus.dataio.core.{Logging, Output}
import com.amadeus.dataio.pipes.snowflake.SnowflakeConfigurator._
import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

case class SnowflakeOutput(
                          url : String,
                          user : String,
                          database : String,
                          schema : String,
                          table : String,
                          mode : String,
                          options: Map[String, String] = Map()
                          ) extends Output with Logging {

  private val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

  /**
   * Writes data to this output.
   *
   * @param data  The data to write.
   * @param spark The SparkSession which will be used to write the data.
   */
  override def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {

    val snowflakeOptions = Map(
      "sfUrl" -> url,
      "sfUser" -> user,
      "sfDatabase" -> database,
      "sfSchema" -> schema,
    ) ++ options

    data.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(snowflakeOptions)
      .option("dbtable", table)
      .mode(mode)
      .save()

  }

}

object SnowflakeOutput{

  /**
   * Creates a new instance of SnowflakeOutput from a typesafe Config object.
   *
   * @param config typesafe Config object containing the configuration fields.
   * @return a new SnowflakeOutput object.
   * @throws com.typesafe.config.ConfigException If any of the mandatory fields is not available in the config argument.
   */
  def apply(implicit config: Config) : SnowflakeOutput = {
    val options = Try(getOptions).getOrElse(Map())
    val mode    = config.getString("Mode")
    val table = config.getString("Table")

    SnowflakeOutput(
      url = getSfUrl,
      user = getSfUser,
      database = getSfDatabase,
      schema = getSfSchema,
      table = table,
      mode = mode,
      options = options)
  }

}


