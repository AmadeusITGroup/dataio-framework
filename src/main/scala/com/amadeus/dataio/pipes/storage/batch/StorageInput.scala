package com.amadeus.dataio.pipes.storage.batch

import com.amadeus.dataio.core.time.DateRange
import com.amadeus.dataio.core.transformers.{Coalescer, DateFilterer, Repartitioner}
import com.amadeus.dataio.core.{Input, Logging, SchemaRegistry}
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.Try

/**
 * Allows to read data from a distributed filesystem.
 * @param path The directory where the data is stored.
 * @param format The format to use to read the data. If None, will use the default value of the SparkSession
 *               (e.g. parquet, delta, csv, etc.).
 * @param options A map of options to use while reading with Spark. They will be added to the SparkSession's default
 *                options.
 * @param dateRange The date range from which the data should be read. This field is critical, as keeping it as None
 *                  while reading large datasets means processing potentially years of data.
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
    with DateFilterer
    with Repartitioner
    with Coalescer
    with Logging {

  /**
   * Reads a batch of data from a distributed filesystem.
   * @param spark The SparkSession which will be used to read the data.
   * @return The data that was read.
   * @throws Exception If the exactly one of the dateRange/dateColumn fields is None.
   */
  override def read(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading $path.")

    var dfReader = format match {
      case Some(f) => spark.read.format(f)
      case None    => spark.read
    }

    dfReader = schema match {
      case Some(definedSchema) =>
        logger.info(s"Use schema [$definedSchema].")
        dfReader.schema(SchemaRegistry.getSchema(definedSchema))
      case None => dfReader
    }

    val df = dfReader.options(options).load(path)

    df
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

    val options           = Try(getOptions).getOrElse(Map())
    val dateRange         = try {
      Some(getDateFilterRange)
    } catch {
      case _: ConfigException => None
      case e: Throwable=> throw e
    }

    val dateColumn        = Try(getDateFilterColumn).toOption
    val repartitionColumn = Try(getRepartitionColumn).toOption
    val repartitionNumber = Try(getRepartitionNumber).toOption
    val coalesce          = Try(getCoalesceNumber).toOption
    val schema            = Try(getSchema).toOption

    StorageInput(
      path,
      format,
      options,
      dateRange,
      dateColumn,
      repartitionColumn,
      repartitionNumber,
      coalesce,
      schema,
      config
    )
  }
}
