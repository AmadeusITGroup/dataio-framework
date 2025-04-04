package com.amadeus.dataio.pipes.kafka.batch

import com.amadeus.dataio.core.transformers.{Coalescer, Repartitioner}
import com.amadeus.dataio.core.{Input, Logging, SchemaRegistry}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/**
 * Class for reading kafka dataframe
 *
 * @param brokers brokers
 * @param topic topic
 * @param options options
 * @param repartitionExprs The column name for the spark coalesce() function.
 * @param repartitionNum The number of partitions for the spark repartition() function.
 * @param coalesce The number of partitions for the spark coalesce() function.
 * @param schema the schema of the dataframe.
 * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
 */
case class KafkaInput(
                       brokers: String,
                       topic: Option[String],
                       pattern: Option[String],
                       assign: Option[String],
                       options: Map[String, String] = Map(),
                       repartitionExprs: Option[String],
                       repartitionNum: Option[Int],
                       coalesce: Option[Int],
                       schema: Option[String],
                       config: Config = ConfigFactory.empty()
) extends Input
    with Repartitioner
    with Coalescer
    with Logging {

  /**
   * read
   *
   * @param spark spark
   * @return the corresponding Dataframe
   */
  override def read(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Load dataframe from Kafka topic [$topic].")

    val dfr = spark.read
      .format("kafka")
      .options(options)
      .option("kafka.bootstrap.servers", brokers)

    var dataFrameReader = (topic, pattern, assign) match {
      case (Some(topic), None, None)   => dfr.option("subscribe", topic)
      case (None, Some(pattern), None) => dfr.option("subscribePattern", pattern)
      case (None, None, Some(assign))  => dfr.option("assign", assign)
      case (None, None, None)          => throw new IllegalArgumentException("No \"assign\", \"subscribe\" or \"subscribePattern\" specified for Kafka source")
      case (_, _, _)                   => throw new IllegalArgumentException("Only one of \"assign\", \"subscribe\" or \"subscribePattern\" options can be specified for Kafka source")
    }

    dataFrameReader = schema match {
      case Some(definedSchema) =>
        logger.info(s"Use schema [$definedSchema].")
        dataFrameReader.schema(SchemaRegistry.getSchema(definedSchema))
      case None => dataFrameReader
    }

    val dataFrame = dataFrameReader.load()

    dataFrame
      .transform(applyRepartition)
      .transform(applyCoalesce)
  }

}

object KafkaInput {
  import com.amadeus.dataio.config.fields._
  import com.amadeus.dataio.pipes.kafka.KafkaConfigurator._

  /**
   * Creates a new instance of KafkaInput from a typesafe Config object.
   * @param config typesafe Config object containing the configuration fields.
   * @return a new KafkaInput object.
   * @throws com.typesafe.config.ConfigException If any of the mandatory fields is not available in the config argument.
   */
  def apply(implicit config: Config): KafkaInput = {
    val brokers              = getBroker
    val topic                = getTopic
    val pattern              = getPattern
    val assign               = getAssign
    val options              = Try(getOptions).getOrElse(Map())
    val repartitioningColumn = Try(getRepartitionColumn).toOption
    val repartitioningNumber = Try(getRepartitionNumber).toOption
    val coalesce             = Try(getCoalesceNumber).toOption
    val schema               = Try(getSchema).toOption

    KafkaInput(
      brokers,
      topic,
      pattern,
      assign,
      options,
      repartitioningColumn,
      repartitioningNumber,
      coalesce,
      schema,
      config
    )
  }
}
