package com.amadeus.dataio.pipes.storage.streaming

import com.amadeus.dataio.core.time.DateRange
import com.amadeus.dataio.core.transformers.{Coalescer, DateFilterer, Repartitioner}
import com.amadeus.dataio.core.{Input, Logging, SchemaRegistry}
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.Try

/**
 * StorageInput
 *
 * @param path path
 * @param format format
 * @param options options
 * @param dateRange The date range from which the data should be read.
 * @param dateColumn The name of the date column to use to filter by date range.
 * @param repartitionColumn The column name for the spark coalesce() function.
 * @param repartitionNumber The number of partitions for the spark repartition() function.
 * @param coalesce The number of partitions for the spark coalesce() function.
 * @param schema the schema of the dataframe.
 * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
 */
case class StorageInput(
    path: String,
    format: Option[String] = None,
    options: Map[String, String] = Map(),
    dateRange: Option[DateRange],
    dateColumn: Option[Column],
    repartitionColumn: Option[String],
    repartitionNumber: Option[Int],
    coalesce: Option[Int],
    schema: Option[String],
    config: Config = ConfigFactory.empty()
) extends Input
    with Repartitioner
    with Coalescer
    with DateFilterer
    with Logging {

  /**
   * readStream
   *
   * @param spark spark
   * @return the corresponding Dataframe
   */
  override def read(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Load dataframe from file stream [$path].")

    var dataFrameReader = spark.readStream.options(options)

    dataFrameReader = format match {
      case Some(definedFormat) => dataFrameReader.format(definedFormat)
      case None                => dataFrameReader
    }

    dataFrameReader = schema match {
      case Some(definedSchema) =>
        logger.info(s"Use schema [$definedSchema].")
        dataFrameReader.schema(SchemaRegistry.getSchema(definedSchema))
      case None => dataFrameReader
    }

    val dataFrame = dataFrameReader.load(path)

    dataFrame
      .transform(applyDateFilter)
      .transform(applyRepartition)
      .transform(applyCoalesce)
  }
}

object StorageInput {
  import com.amadeus.dataio.config.fields._

  /**
   * Creates a new instance of StorageInput from a typesafe Config object.
   * @param config typesafe Config object containing the configuration fields.
   * @return a new StorageInput object.
   * @throws com.typesafe.config.ConfigException If any of the mandatory fields is not available in the config argument.
   */
  def apply(implicit config: Config): StorageInput = {
    val path   = config.getString("Path")
    val format = Try(config.getString("Format")).toOption

    val options              = Try(getOptions).getOrElse(Map())
    val dateRange = try {
      Some(getDateFilterRange)
    } catch {
      case _: ConfigException => None
      case e: Throwable => throw e
    }
    
    val dateColumn           = Try(getDateFilterColumn).toOption
    val repartitioningColumn = Try(getRepartitionColumn).toOption
    val repartitioningNumber = Try(getRepartitionNumber).toOption
    val coalesce             = Try(getCoalesceNumber).toOption
    val schema               = Try(getSchema).toOption

    StorageInput(
      path,
      format,
      options,
      dateRange,
      dateColumn,
      repartitioningColumn,
      repartitioningNumber,
      coalesce,
      schema,
      config
    )
  }
}
