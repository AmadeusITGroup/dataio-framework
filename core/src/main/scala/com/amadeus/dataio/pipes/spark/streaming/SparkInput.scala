package com.amadeus.dataio.pipes.spark.streaming

import com.amadeus.dataio.core.time.DateRange
import com.amadeus.dataio.core.transformers.{Coalescer, DateFilterer, Repartitioner}
import com.amadeus.dataio.core.{Input, Logging, SchemaRegistry}
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.Try

/** Reads a stream of data using the Spark DataStreamReader.
  * @param dateRange The date range from which the data should be read.
  * @param dateColumn The name of the date column to use to filter by date range.
  * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
  */
case class SparkInput(
    name: String,
    path: Option[String],
    format: Option[String] = None,
    options: Map[String, String] = Map(),
    dateRange: Option[DateRange],
    dateColumn: Option[Column],
    repartitionExprs: Option[String],
    repartitionNum: Option[Int],
    coalesce: Option[Int],
    schema: Option[String],
    config: Config = ConfigFactory.empty()
) extends Input
    with Repartitioner
    with Coalescer
    with DateFilterer
    with Logging {

  override def read(implicit spark: SparkSession): DataFrame = {
    logger.info(s"reading stream: $name")
    if (path.isDefined) logger.info(s"path: ${path.get}")
    if (options.nonEmpty) logger.info(s"options: $options")
    if (format.isDefined) logger.info(s"format: ${format.get}")
    if (schema.isDefined) logger.info(s"schema: ${schema.get}")

    var dsReader = spark.readStream.options(options)

    dsReader = format match {
      case Some(f) => dsReader.format(f)
      case None    => dsReader
    }

    dsReader = schema match {
      case Some(s) => dsReader.schema(SchemaRegistry.getSchema(s))
      case None    => dsReader
    }

    val df = path match {
      case Some(p) => dsReader.load(p)
      case _       => dsReader.load()
    }

    df
      .transform(applyDateFilter)
      .transform(applyRepartition)
      .transform(applyCoalesce)
  }
}

object SparkInput {
  import com.amadeus.dataio.config.fields._

  /** Creates a new instance of SparkInput from a typesafe Config object.
    * @param config typesafe Config object containing the configuration fields.
    * @return a new SparkInput object.
    */
  def apply(implicit config: Config): SparkInput = {
    val name = Try {
      config.getString("name")
    } getOrElse {
      throw new Exception("Missing required `name` field in configuration.")
    }
    val path   = getPath
    val format = Try(config.getString("format")).toOption

    val options          = getOptions
    val dateRange        = getDateFilterRange
    val dateColumn       = getDateFilterColumn
    val repartitionExprs = getRepartitionExprs
    val repartitionNum   = getRepartitionNum
    val coalesce         = getCoalesceNumber
    val schema           = getSchema

    SparkInput(
      name,
      path,
      format,
      options,
      dateRange,
      dateColumn,
      repartitionExprs,
      repartitionNum,
      coalesce,
      schema,
      config
    )
  }
}
