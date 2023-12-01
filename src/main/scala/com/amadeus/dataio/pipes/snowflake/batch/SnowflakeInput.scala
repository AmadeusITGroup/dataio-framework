package com.amadeus.dataio.pipes.snowflake.batch

import com.amadeus.dataio.core.{Input, Logging}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

case class SnowflakeInput(
                           url: String,
                           user: String,
                           database: String,
                           schema: String,
                           table: Option[String],
                           query: Option[String],
                           options: Map[String, String] = Map()
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

  def apply(implicit config: Config) : SnowflakeInput = {
    
  }


}

