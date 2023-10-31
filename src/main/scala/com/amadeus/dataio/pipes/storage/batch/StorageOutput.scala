package com.amadeus.dataio.pipes.storage.batch

import com.amadeus.dataio.core.transformers._
import com.amadeus.dataio.core.{Logging, Output}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

/**
 * Allows to write data to a distributed filesystem.
 * @param path The directory where the data should be written.
 * @param format The format to write the data. If none, will use the default value of the SparkSession (e.g. parquet, delta, csv, etc.).
 * @param partitionByColumns The list of columns to use to partition the data to write.
 * @param options A map of options to use while writing with Spark. They will be added to the SparkSession's default
 *                options.
 * @param dropDuplicatesActive true if the spark dropDuplicates() function should be applied.
 * @param dropDuplicatesColumns The list of column names for the spark dropDuplicates function.
 * @param repartitionNumber The number of partitions for the spark repartition() function.
 * @param repartitionColumn The column name for the spark coalesce() function.
 * @param repartitionByRangeNumber The number of partitions for the spark repartitionByRange() function.
 * @param repartitionByRangeColumn The column name for the spark repartitionByRange() function.
 * @param sortWithinPartitionColumns The sequence of column names for the spark sortWithinPartitions() function.
 * @param coalesce The number of partitions for the spark coalesce() function.
 * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
 */
case class StorageOutput(
    path: String,
    format: Option[String] = None,
    partitionByColumns: Seq[String],
    options: Map[String, String] = Map(),
    dropDuplicatesActive: Option[Boolean],
    dropDuplicatesColumns: Seq[String],
    repartitionNumber: Option[Int],
    repartitionColumn: Option[String],
    repartitionByRangeNumber: Option[Int],
    repartitionByRangeColumn: Option[String],
    sortWithinPartitionColumns: Seq[String],
    coalesce: Option[Int],
    writeMode: String,
    config: Config = ConfigFactory.empty()
) extends Output
    with DuplicatesDropper
    with Repartitioner
    with RangeRepartitioner
    with PartitionsSorter
    with Coalescer
    with Logging {

  /**
   * Writes a batch of data to a distributed filesystem.
   * @param data The data to write.
   * @param spark The SparkSession which will be used to write the data.
   */
  override def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {
    logger.info(s"Writing to $path using format $format.")

    val ds = data
      .transform(applyDropDuplicates)
      .transform(applyRepartition)
      .transform(applyRepartitionByRange)
      .transform(applySortWithinPartitions)
      .transform(applyCoalesce)

    val dsWriter = format match {
      case Some(f) => ds.write.format(f)
      case None    => ds.write
    }

    partitionByColumns match {
      case _ +: _ =>
        logger.info(s"Partitioning by: $partitionByColumns.")
        dsWriter.partitionBy(partitionByColumns: _*).mode(writeMode).options(options).save(path)
      case _ => dsWriter.mode(writeMode).options(options).save(path)
    }
  }
}

object StorageOutput {
  import com.amadeus.dataio.config.fields._

  /**
   * Creates a new instance of StorageOutput from a typesafe Config object.
   * @param config typesafe Config object containing the configuration fields.
   * @return a new StorageOutput object.
   * @throws com.typesafe.config.ConfigException If any of the mandatory fields is not available in the config argument.
   */
  def apply(implicit config: Config): StorageOutput = {
    val path               = getPath
    val format             = Try(config.getString("Format")).toOption
    val partitionByColumns = Try(getPartitionByColumns).getOrElse(Nil)

    val options                     = Try(getOptions).getOrElse(Map())
    val dropDuplicatesActive        = Try(getDropDuplicatesActive).toOption
    val dropDuplicatesColumns       = Try(getDropDuplicatesColumns).getOrElse(Nil)
    val repartitionColumn           = Try(getRepartitionColumn).toOption
    val repartitionNumber           = Try(getRepartitionNumber).toOption
    val repartitionByRangeNumber    = Try(getRepartitionByRangeNumber).toOption
    val repartitionByRangeColumn    = Try(getRepartitionByRangeColumn).toOption
    val sortWithinPartitionsColumns = Try(getSortWithinPartitionsColumns).getOrElse(Nil)
    val coalesce                    = Try(getCoalesceNumber).toOption

    // default to "append" to avoid deleting datasets by mistake
    val writeMode = Try(config.getString("Mode")).getOrElse("append")

    StorageOutput(
      path,
      format,
      partitionByColumns,
      options,
      dropDuplicatesActive,
      dropDuplicatesColumns,
      repartitionNumber,
      repartitionColumn,
      repartitionByRangeNumber,
      repartitionByRangeColumn,
      sortWithinPartitionsColumns,
      coalesce,
      writeMode,
      config
    )
  }
}
