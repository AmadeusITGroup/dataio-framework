package com.amadeus.dataio.pipes.snowflake.batch

import com.amadeus.dataio.core.{Input, Logging}
import com.amadeus.dataio.config.fields.getOptions
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.amadeus.dataio.pipes.snowflake.SnowflakeConfigurator._

import scala.util.Try

/**
 * Class for reading snowflake input
 *
 * @param url snowflake url
 * @param user snowflake user
 * @param database database used to query
 * @param schema schema
 * @param table table to use in snowflake
 * @param query query
 * @param options options
 * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
 */
case class SnowflakeInput(
                           url: String,
                           user: String,
                           database: String,
                           schema: String,
                           table: Option[String],
                           query: Option[String],
                           options: Map[String, String] = Map(),
                           config: Config = ConfigFactory.empty()
                         ) extends Input with Logging {

  private val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

  override def read(implicit spark: SparkSession): DataFrame = {

    val snowflakeOptions = Map(
      "sfUrl" -> url,
      "sfUser" -> user,
      "sfDatabase" -> database,
      "sfSchema" -> schema,
    ) ++ options

    var dfReader = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(snowflakeOptions)
    dfReader = (table, query) match {
      case (None, None) => throw new IllegalArgumentException("No \"table\" or \"query\" option selected for Snowflake Source")
      case (Some(table), Some(query)) => throw new IllegalArgumentException("Only one of \"table\" or \"query\" options can be selected in Snowflake Source")
      case (Some(table), None) => dfReader.option("dbtable", table)
      case (None, Some(query)) => dfReader.option("query", query)
    }

    dfReader.load()

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
  def apply(implicit config: Config) : SnowflakeInput = {
    val options = Try(getOptions).getOrElse(Map())
    val table = Some(config.getString("Table"))
    val query = Some(config.getString("Query"))

    SnowflakeInput(
      url = getUrl,
      user = getUser,
      database = getDatabase,
      schema = getSchema,
      table = table,
      query = query,
      options = options)
  }
}

