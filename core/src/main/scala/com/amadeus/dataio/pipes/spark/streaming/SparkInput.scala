package com.amadeus.dataio.pipes.spark.streaming

import com.amadeus.dataio.core.time.DateRange
import com.amadeus.dataio.core.transformers.{Coalescer, DateFilterer, Repartitioner}
import com.amadeus.dataio.core.{Input, Logging, SchemaRegistry}
import com.amadeus.dataio.pipes.spark.{SparkPathSource, SparkSource, SparkTableSource}
import com.amadeus.dataio.pipes.spark.batch.SparkInput.getSparkSource
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.Try

/** Reads a stream of data using the Spark DataStreamReader.
  * @param source The source from which the data should be read. It can be a table or a path.
  * @param dateRange The date range from which the data should be read.
  * @param dateColumn The name of the date column to use to filter by date range.
  * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
  */
case class SparkInput(
    name: String,
    source: Option[SparkSource] = None,
    options: Map[String, String] = Map(),
    dateRange: Option[DateRange] = None,
    dateColumn: Option[Column] = None,
    repartitionExprs: Option[String] = None,
    repartitionNum: Option[Int] = None,
    coalesce: Option[Int] = None,
    schema: Option[String] = None,
    config: Config = ConfigFactory.empty()
) extends Input
    with Repartitioner
    with Coalescer
    with DateFilterer
    with Logging {

  override def read(implicit spark: SparkSession): DataFrame = {
    logger.info(s"reading stream: $name")
    if (options.nonEmpty) logger.info(s"options: $options")
    if (schema.isDefined) logger.info(s"schema: ${schema.get}")

    var dsReader = spark.readStream.options(options)

    dsReader = schema match {
      case Some(s) => dsReader.schema(SchemaRegistry.getSchema(s))
      case None    => dsReader
    }

    val df = source match {
      case Some(SparkTableSource(table)) =>
        logger.info(s"table: $table")
        dsReader.table(table)

      case Some(SparkPathSource(path, formatOpt)) =>
        logger.info(s"path: $path")
        formatOpt.foreach(f => logger.info(s"format: $f"))
        val reader = if (formatOpt.isDefined) {
          dsReader.format(formatOpt.get)
        } else {
          dsReader
        }

        reader.load(path)

      case _ => dsReader.load()
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
    val source = getSparkSource

    val options          = getOptions
    val dateRange        = getDateFilterRange
    val dateColumn       = getDateFilterColumn
    val repartitionExprs = getRepartitionExprs
    val repartitionNum   = getRepartitionNum
    val coalesce         = getCoalesceNumber
    val schema           = getSchema

    SparkInput(
      name,
      source,
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
