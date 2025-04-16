package com.amadeus.dataio.pipes.kafka.streaming

import com.amadeus.dataio.core.transformers.{Coalescer, Repartitioner}
import com.amadeus.dataio.core.{Input, Logging}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/** Class for reading kafka dataframe
 *
 * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
 */
case class KafkaInput(
                       name: String,
                       options: Map[String, String] = Map(),
                       repartitionExprs: Option[String],
                       repartitionNum: Option[Int],
                       coalesce: Option[Int],
                       config: Config = ConfigFactory.empty()
) extends Input
    with Repartitioner
    with Coalescer
    with Logging {

  override def read(implicit spark: SparkSession): DataFrame = {
    logger.info(s"reading from kafka: $name")
    if (options.nonEmpty) logger.info(s"options: $options")

    val dsReader = spark.readStream
      .format("kafka")
      .options(options)

    val dataFrame = dsReader.load()

    dataFrame
      .transform(applyRepartition)
      .transform(applyCoalesce)
  }
}

object KafkaInput {
  import com.amadeus.dataio.config.fields._

  /** Creates a new instance of KafkaInput from a typesafe Config object.
   *
   * @param config typesafe Config object containing the configuration fields.
   * @return a new KafkaInput object.
   */
  def apply(implicit config: Config): KafkaInput = {
    val name = Try {
      config.getString("name")
    } getOrElse {
      throw new Exception("Missing required `name` field in configuration.")
    }

    val options = getOptions
    val repartitionExprs = getRepartitionExprs
    val repartitionNum = getRepartitionNum
    val coalesce = getCoalesceNumber

    KafkaInput(
      name,
      options,
      repartitionExprs,
      repartitionNum,
      coalesce,
      config
    )
  }
}
