package com.amadeus.dataio.pipes.storage.streaming

import com.amadeus.dataio.core.{Logging, Output}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

import java.io.File
import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * Allows to write stream data to HDFS.
 *
 * @param format the output format.
 * @param path the path.
 * @param partitioningColumns the columns to partition by.
 * @param processingTimeTrigger processingTimeTrigger.
 * @param timeout timeout in milliseconds.
 * @param mode mode.
 * @param options options.
 * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
 * @param outputName the output name used to define the streaming query name.
 */
case class StorageOutput(
    format: String,
    path: String,
    partitioningColumns: Seq[String],
    processingTimeTrigger: Trigger,
    timeout: Long,
    mode: String,
    options: Map[String, String] = Map(),
    config: Config = ConfigFactory.empty(),
    outputName: Option[String]
) extends Output
    with Logging {

  /**
   * Writes data to this output.
   *
   * @param data  The data to write.
   * @param spark The SparkSession which will be used to write the data.
   */
  def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {
    logger.info(s"Write dataframe to storage [$path]")

    val queryName = createQueryName()

    var streamWriter = data.writeStream
      .queryName(queryName)
      .outputMode(mode)
      .format(format)
      .options(options)

    if (partitioningColumns.nonEmpty) {
      logger.info(s"Partitioning by: $partitioningColumns.")
      streamWriter = streamWriter.partitionBy(partitioningColumns: _*)
    }

    val streamingQuery = streamWriter
      .trigger(processingTimeTrigger)
      .start(path)

    streamingQuery.awaitTermination(timeout)
    streamingQuery.stop()
  }

  /**
   * Create a unique query name based on output path if exists.
   *
   * @return a unique query name.
   */
  private[streaming] def createQueryName(): String = {
    val directory = Try { path.split(File.separatorChar).reverse.head }.toOption

    val queryName: String = (directory, outputName) match {
      case (Some(directoryName), Some(name)) => s"QN_${name}_${directoryName}_${java.util.UUID.randomUUID}"
      case (Some(directoryName), _)          => s"QN_${directoryName}_${java.util.UUID.randomUUID}"
      case _                                 => java.util.UUID.randomUUID.toString
    }

    queryName
  }
}

object StorageOutput {
  import com.amadeus.dataio.config.fields._

  /**
   * Creates an StorageOutput based on a given configuration.
   *
   * @param config The collection of config nodes that will be used to instantiate KafkaOutput.
   * @return a new instance of StorageOutput.
   */
  def apply(implicit config: Config): StorageOutput = {
    val format = config.getString("Format")
    val path   = getPath

    val name = Try(config.getString("Name")).toOption

    val partitioningColumns = getPartitionByColumns

    val mode = config.getString("Mode")

    val duration              = Duration(config.getString("Duration"))
    val processingTimeTrigger = Trigger.ProcessingTime(duration)

    val timeout = getTimeout

    val options = Try(getOptions).getOrElse(Map())

    StorageOutput(
      format,
      path,
      partitioningColumns,
      processingTimeTrigger,
      timeout,
      mode,
      options,
      config,
      name
    )
  }
}
