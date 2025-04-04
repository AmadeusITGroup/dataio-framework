package com.amadeus.dataio.pipes.elk.batch

import com.amadeus.dataio.core.{Logging, Output}
import com.amadeus.dataio.pipes.elk.ElkOutputCommons
import com.amadeus.dataio.pipes.elk.ElkOutputCommons.{DefaultSuffixDatePattern, checkNodesIsDefined, checkPortIsDefined}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

/**
 * Allows to write batch data to Elasticsearch with automatic date sub-indexing.
 *
 * @param index the Index to write to.
 * @param mode mode.
 * @param dateField The date field to use for sub index partitioning.
 * @param suffixDatePattern the date suffix pattern to use for the full index.
 * @param options options.
 * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
 */
case class ElkOutput(
    index: String,
    mode: String,
    dateField: String,
    suffixDatePattern: String,
    options: Map[String, String] = Map.empty,
    config: Config = ConfigFactory.empty()
) extends Output
    with Logging
    with ElkOutputCommons {

  /**
   * Writes data to this output.
   *
   * @param data  The data to write.
   * @param spark The SparkSession which will be used to write the data.
   */
  override def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {
    val fullIndexName = computeFullIndexName()
    logger.info(s"Write dataframe to Elasticsearch index [$fullIndexName]")

    data.write.format(Format).mode(mode).options(options).save(fullIndexName)
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

    val options = Try(getOptions).getOrElse(Map())

    checkNodesIsDefined(options)
    checkPortIsDefined(options)

    val dateField = getDateField

    val suffixDatePattern = getSubIndexDatePattern.getOrElse(DefaultSuffixDatePattern)

    ElkOutput(index = index, mode = mode, dateField = dateField, suffixDatePattern = suffixDatePattern, options = options, config = config)
  }

}
