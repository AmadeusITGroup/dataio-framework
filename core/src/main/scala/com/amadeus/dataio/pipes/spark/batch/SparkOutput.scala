package com.amadeus.dataio.pipes.spark.batch

import com.amadeus.dataio.core.transformers._
import com.amadeus.dataio.core.{Logging, Output}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

/** Writes a batch of data using the Spark DataFrameWriter.
  * @param dropDuplicates true if the spark dropDuplicates() function should be applied.
  * @param dropDuplicatesColumns The list of column names for the spark dropDuplicates function.
  * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
  */
case class SparkOutput(
    name: String,
    path: Option[String],
    format: Option[String] = None,
    partitionByColumns: Seq[String],
    options: Map[String, String] = Map(),
    dropDuplicates: Boolean,
    dropDuplicatesColumns: Seq[String],
    repartitionNum: Option[Int],
    repartitionExprs: Option[String],
    repartitionByRangeNum: Option[Int],
    repartitionByRangeExprs: Option[String],
    sortWithinPartitionExprs: Seq[String],
    coalesce: Option[Int],
    mode: String,
    config: Config = ConfigFactory.empty()
) extends Output
    with DuplicatesDropper
    with Repartitioner
    with RangeRepartitioner
    with PartitionsSorter
    with Coalescer
    with Logging {

  override def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {
    logger.info(s"writing: $name")
    if (path.isDefined) logger.info(s"path: ${path.get}")
    if (options.nonEmpty) logger.info(s"options: $options")
    if (format.isDefined) logger.info(s"format: ${format.get}")
    logger.info(s"mode: $mode")

    val ds = data
      .transform(applyDropDuplicates)
      .transform(applyRepartition)
      .transform(applyRepartitionByRange)
      .transform(applySortWithinPartitions)
      .transform(applyCoalesce)

    var dfWriter = format match {
      case Some(f) =>
        ds.write
          .mode(mode)
          .options(options)
          .format(f)
      case None =>
        ds.write
          .mode(mode)
          .options(options)
    }

    dfWriter = partitionByColumns match {
      case _ +: _ =>
        logger.info(s"partition_by: $partitionByColumns")
        dfWriter.partitionBy(partitionByColumns: _*)
      case _ => dfWriter
    }

    path match {
      case Some(p) => dfWriter.save(p)
      case _       => dfWriter.save()
    }
  }
}

object SparkOutput {
  import com.amadeus.dataio.config.fields._

  /** Creates a new instance of SparkOutput from a typesafe Config object.
    * @param config typesafe Config object containing the configuration fields.
    * @return a new SparkOutput object.
    */
  def apply(implicit config: Config): SparkOutput = {
    val name = Try {
      config.getString("name")
    } getOrElse {
      throw new Exception("Missing required `name` field in configuration.")
    }
    val path               = getPath
    val format             = Try(config.getString("format")).toOption
    val partitionByColumns = getPartitionByColumns

    val options = getOptions

    val repartitionColumn = getRepartitionExprs
    val repartitionNumber = getRepartitionNum
    val coalesce          = getCoalesceNumber

    val dropDuplicatesActive        = getDropDuplicatesActive
    val dropDuplicatesColumns       = getDropDuplicatesColumns
    val repartitionByRangeNumber    = getRepartitionByRangeNum
    val repartitionByRangeColumn    = getRepartitionByRangeExprs
    val sortWithinPartitionsColumns = getSortWithinPartitionsExprs

    val mode = Try {
      config.getString("mode")
    } getOrElse {
      throw new Exception("Missing required `mode` field in configuration.")
    }

    SparkOutput(
      name,
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
      mode,
      config
    )
  }
}
