package com.amadeus.dataio.pipes.elk.streaming

import com.amadeus.dataio.core.{Logging, Output}
import com.amadeus.dataio.pipes.elk.ElkOutputCommons
import com.amadeus.dataio.pipes.elk.ElkOutputCommons.{DefaultSuffixDatePattern, checkNodesIsDefined, checkPortIsDefined}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * Allows to write stream data to Elasticsearch with automatic date sub-indexing.
 *
 * @param index the Index to write to.
 * @param processingTimeTrigger processingTimeTrigger.
 * @param timeout timeout in milliseconds.
 * @param mode mode.
 * @param options options.
 * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
 * @param dateField The date field to use for sub index partitioning.
 * @param suffixDatePattern the date suffix pattern to use for the full index.
 * @param outputName the output name used to define the streaming query name.
 */
case class ElkOutput(
    index: String,
    processingTimeTrigger: Trigger,
    timeout: Long,
    mode: String,
    options: Map[String, String] = Map.empty,
    config: Config = ConfigFactory.empty(),
    dateField: String,
    suffixDatePattern: String,
    outputName: Option[String]
) extends Output
    with Logging
    with ElkOutputCommons {

  /**
   * Writes data to this output.
   *
   * @param data  The data to write.
   * @param spark The SparkSession which will be used to write the data.
   */
  def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {
    val fullIndexName = computeFullIndexName()
    logger.info(s"Write dataframe to Elasticsearch index [$fullIndexName]")

    val queryName = createQueryName()

    val streamWriter = data.writeStream
      .queryName(queryName)
      .outputMode(mode)
      .format(Format)
      .options(options)

    val streamingQuery = streamWriter
      .trigger(processingTimeTrigger)
      .start(fullIndexName)

    streamingQuery.awaitTermination(timeout)
    streamingQuery.stop()
  }

  /**
   * Create a unique query name based on output path if exists.
   *
   * @return a unique query name.
   */
  private[streaming] def createQueryName(): String = {

    outputName match {
      case Some(name) => s"QN_${name}_${index}_${java.util.UUID.randomUUID}"
      case _          => s"QN_${index}_${java.util.UUID.randomUUID}"
    }

  }
}

object ElkOutput {
  import com.amadeus.dataio.config.fields._
  import com.amadeus.dataio.pipes.elk.ElkConfigurator._

  /**
   * Creates an ElkOutput based on a given configuration.
   *
   * @param config The collection of config nodes that will be used to instantiate KafkaOutput.
   * @return a new instance of ElkOutput.
   */
  def apply(implicit config: Config): ElkOutput = {

    val index = getIndex

    val mode = config.getString("Mode")

    val duration              = Duration(config.getString("Duration"))
    val processingTimeTrigger = Trigger.ProcessingTime(duration)

    val timeout = getTimeout

    val options = Try(getOptions).getOrElse(Map())

    checkNodesIsDefined(options)
    checkPortIsDefined(options)

    val dateField = getDateField

    val suffixDatePattern = getSubIndexDatePattern.getOrElse(DefaultSuffixDatePattern)

    val name = Try(config.getString("Name")).toOption

    ElkOutput(
      index = index,
      processingTimeTrigger = processingTimeTrigger,
      timeout = timeout,
      mode = mode,
      options = options,
      config = config,
      dateField = dateField,
      suffixDatePattern = suffixDatePattern,
      outputName = name
    )
  }
}
