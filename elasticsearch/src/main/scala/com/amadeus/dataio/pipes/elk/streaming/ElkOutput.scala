package com.amadeus.dataio.pipes.elk.streaming

import com.amadeus.dataio.core.{Logging, Output}
import com.amadeus.dataio.pipes.elk.ElkOutputCommons
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

/** Allows to write stream data to Elasticsearch with automatic date sub-indexing.
  *
  * @param index the Index to write to.
  * @param trigger the trigger to be used for the streaming query.
  * @param timeout timeout in milliseconds.
  * @param mode mode.
  * @param options options.
  * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
  * @param dateField The date field to use for sub index partitioning.
  * @param suffixDatePattern the date suffix pattern to use for the full index.
  * @param outputName the output name used to define the streaming query name.
  */
case class ElkOutput(
    name: String,
    index: String,
    trigger: Option[Trigger],
    timeout: Long,
    mode: String,
    options: Map[String, String] = Map.empty,
    config: Config = ConfigFactory.empty(),
    dateField: String,
    suffixDatePattern: String
) extends Output
    with Logging
    with ElkOutputCommons {

  /** Writes data to this output.
    *
    * @param data  The data to write.
    * @param spark The SparkSession which will be used to write the data.
    */
  def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {
    val fullIndexName = computeFullIndexName()
    logger.info(s"Write dataframe to Elasticsearch index [$fullIndexName] using trigger [$trigger]")

    val queryName = createQueryName()

    var streamWriter = data.writeStream
      .queryName(queryName)
      .outputMode(mode)
      .format(Format)
      .options(options)

    streamWriter = trigger match {
      case Some(trigger) => streamWriter.trigger(trigger)
      case _             => streamWriter
    }

    val streamingQuery = streamWriter.start(fullIndexName)

    streamingQuery.awaitTermination(timeout)
    streamingQuery.stop()
  }

  /** Create a unique query name based on output path if exists.
    *
    * @return a unique query name.
    */
  private[streaming] def createQueryName(): String = s"QN_${index}_${java.util.UUID.randomUUID}"
}

object ElkOutput {
  import com.amadeus.dataio.config.fields._
  import com.amadeus.dataio.pipes.elk.ElkConfigurator._
  import com.amadeus.dataio.pipes.elk.ElkOutputCommons.{DefaultSuffixDatePattern, checkNodesIsDefined, checkPortIsDefined}

  /** Creates an ElkOutput based on a given configuration.
    *
    * @param config The collection of config nodes that will be used to instantiate KafkaOutput.
    * @return a new instance of ElkOutput.
    */
  def apply(implicit config: Config): ElkOutput = {
    val name = Try {
      config.getString("name")
    } getOrElse {
      throw new Exception("Missing required `name` field in configuration.")
    }

    val index = getIndex

    val mode = config.getString("mode")

    val trigger = getStreamingTrigger

    val timeout = getTimeout

    val options = Try(getOptions).getOrElse(Map())

    checkNodesIsDefined(options)
    checkPortIsDefined(options)

    val dateField = getDateField

    val suffixDatePattern = getSubIndexDatePattern.getOrElse(DefaultSuffixDatePattern)

    ElkOutput(
      name = name,
      index = index,
      trigger = trigger,
      timeout = timeout.get,
      mode = mode,
      options = options,
      config = config,
      dateField = dateField,
      suffixDatePattern = suffixDatePattern
    )
  }
}
