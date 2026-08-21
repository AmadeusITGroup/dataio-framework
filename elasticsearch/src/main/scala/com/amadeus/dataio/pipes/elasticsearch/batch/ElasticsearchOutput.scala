package com.amadeus.dataio.pipes.elasticsearch.batch

import com.amadeus.dataio.core.{Logging, Output}
import com.amadeus.dataio.pipes.elasticsearch.ElasticsearchOutputCommons
import com.amadeus.dataio.pipes.elasticsearch.ElasticsearchOutputCommons.{
  DefaultSuffixDatePattern,
  checkNodesIsDefined,
  checkPortIsDefined
}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

/**
 * Allows to write batch data to Elasticsearch with automatic date sub-indexing.
 *
 * @param name the name of the output.
 * @param index the Index to write to.
 * @param mode mode.
 * @param dateField The date field to use for sub index partitioning.
 * @param suffixDatePattern the date suffix pattern to use for the full index.
 * @param options options.
 * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
 */
case class ElasticsearchOutput(
    name: String,
    index: String,
    mode: String,
    dateField: String,
    suffixDatePattern: String,
    options: Map[String, String] = Map.empty,
    config: Config = ConfigFactory.empty()
) extends Output
    with Logging
    with ElasticsearchOutputCommons {

  /**
   * Writes data to this output.
   *
   * @param data  The data to write.
   * @param spark The SparkSession which will be used to write the data.
   */
  override def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {
    val fullIndexName = computeFullIndexName()
    logger.info(s"writing to elasticsearch: $name")
    if (options.nonEmpty) logger.info(s"options: $options")
    logger.info(s"index: $fullIndexName")
    logger.info(s"mode: $mode")

    data.write.format(Format).mode(mode).options(options).save(fullIndexName)
  }
}

object ElasticsearchOutput {

  import com.amadeus.dataio.config.fields._
  import com.amadeus.dataio.pipes.elasticsearch.ElasticsearchConfigurator._

  /**
   * Creates an ElasticsearchOutput based on a given configuration.
   *
   * @param config The collection of config nodes that will be used to instantiate ElasticsearchOutput.
   * @return a new instance of ElasticsearchOutput.
   */
  def apply(implicit config: Config): ElasticsearchOutput = {
    val name = Try {
      config.getString("name")
    } getOrElse {
      throw new Exception("Missing required `name` field in configuration.")
    }

    val index = getIndex

    val mode = Try {
      config.getString("mode")
    } getOrElse {
      throw new Exception("Missing required `mode` field in configuration.")
    }

    val options = Try(getOptions).getOrElse(Map())

    checkNodesIsDefined(options)
    checkPortIsDefined(options)

    val dateField = getDateField

    val suffixDatePattern = getSubIndexDatePattern.getOrElse(DefaultSuffixDatePattern)

    ElasticsearchOutput(
      name = name,
      index = index,
      mode = mode,
      dateField = dateField,
      suffixDatePattern = suffixDatePattern,
      options = options,
      config = config
    )
  }

}
