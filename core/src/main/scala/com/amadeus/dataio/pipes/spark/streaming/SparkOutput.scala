package com.amadeus.dataio.pipes.spark.streaming

import com.amadeus.dataio.core.{Logging, Output}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

import java.io.File
import scala.util.Try

/** Writes a stream of data using the Spark DataStreamWriter.
  * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
  */
case class SparkOutput(
    name: String,
    path: Option[String],
    format: Option[String] = None,
    partitionByColumns: Seq[String],
    trigger: Option[Trigger],
    timeout: Option[Long],
    mode: String,
    options: Map[String, String] = Map(),
    config: Config = ConfigFactory.empty()
) extends Output
    with Logging {

  /** Writes data to this output.
    *
    * @param data  The data to write.
    * @param spark The SparkSession which will be used to write the data.
    */
  def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {
    logger.info(s"writing stream: $name")
    if (path.isDefined) logger.info(s"path: ${path.get}")
    if (options.nonEmpty) logger.info(s"options: $options")
    if (format.isDefined) logger.info(s"format: ${format.get}")
    logger.info(s"mode: $mode")

    val queryName = createQueryName()

    var dsWriter = format match {
      case Some(f) =>
        data.writeStream
          .queryName(queryName)
          .outputMode(mode)
          .options(options)
          .format(f)
      case _ =>
        data.writeStream
          .queryName(queryName)
          .outputMode(mode)
          .options(options)
    }

    dsWriter = partitionByColumns match {
      case _ +: _ =>
        logger.info(s"partition_by: $partitionByColumns")
        dsWriter.partitionBy(partitionByColumns: _*)
      case _ => dsWriter
    }

    dsWriter = trigger match {
      case Some(t) =>
        logger.info(s"trigger: $t")
        dsWriter.trigger(t)
      case _ => dsWriter
    }

    val streamingQuery = path match {
      case Some(p) => dsWriter.start(p)
      case _       => dsWriter.start()
    }

    if (timeout.isDefined) streamingQuery.awaitTermination(timeout.get)
    streamingQuery.stop()
  }

  /** Create a unique query name based on output path if exists.
    * @return a unique query name.
    */
  private[streaming] def createQueryName(): String = {

    val directory = Try { path.get.split(File.separatorChar).reverse.head }.toOption
    val queryName: String = directory match {
      case Some(directoryName) => s"QN_${name}_${directoryName}_${java.util.UUID.randomUUID}"
      case _                   => s"QN_${name}_${java.util.UUID.randomUUID}"
    }

    queryName
  }
}

object SparkOutput {
  import com.amadeus.dataio.config.fields._

  def apply(implicit config: Config): SparkOutput = {
    val name               = config.getString("name")
    val path               = getPath
    val format             = Try(config.getString("format")).toOption
    val partitionByColumns = getPartitionByColumns

    val options = getOptions

    val trigger = getStreamingTrigger
    val timeout = getTimeout

    val mode = config.getString("mode")

    SparkOutput(
      name,
      path,
      format,
      partitionByColumns,
      trigger,
      timeout,
      mode,
      options,
      config
    )
  }
}
