package com.amadeus.dataio.pipes.kafka.streaming

import com.amadeus.dataio.core.{Logging, Output}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * Class for reading kafka dataframe
 *
 * @param brokers brokers
 * @param topic topic
 * @param processingTimeTrigger processingTimeTrigger.
 * @param timeout timeout in milliseconds.
 * @param mode mode.
 * @param options options
 * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
 * @param outputName the output name used to define the streaming query name.
 */
case class KafkaOutput(
    brokers: String,
    topic: String,
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
    logger.info(s"Write dataframe to kafka [$topic]")

    val queryName = createQueryName()

    val streamWriter = data.writeStream
      .queryName(queryName)
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", topic)
      .options(options)
      .outputMode(mode)

    val streamingQuery = streamWriter
      .trigger(processingTimeTrigger)
      .start()

    streamingQuery.awaitTermination(timeout)
    streamingQuery.stop()
  }

  /**
   * Create a unique query name based on output topic.
   *
   * @return a unique query name.
   */
  private[streaming] def createQueryName(): String = {

    outputName match {
      case Some(name) => s"QN_${name}_${topic}_${java.util.UUID.randomUUID}"
      case _          => s"QN_${topic}_${java.util.UUID.randomUUID}"
    }

  }
}

object KafkaOutput {
  import com.amadeus.dataio.config.fields._
  import com.amadeus.dataio.pipes.kafka.KafkaConfigurator._

  /**
   * Creates an KafkaOutput based on a given configuration.
   *
   * @param config The collection of config nodes that will be used to instantiate KafkaOutput.
   * @return a new instance of KafkaOutput.
   * @throws com.typesafe.config.ConfigException If any of the mandatory fields is not available in the config argument.
   */
  def apply(implicit config: Config): KafkaOutput = {
    val brokers = getBroker
    val topic = getTopic match {
      case Some(topicToUse) if topicToUse.nonEmpty => topicToUse
      case _                                       => throw new IllegalArgumentException("No topic specified for Kafka source")
    }

    val duration              = Duration(config.getString("Duration"))
    val processingTimeTrigger = Trigger.ProcessingTime(duration)

    val timeout = getTimeout
    val mode    = config.getString("Mode")
    val options = Try(getOptions).getOrElse(Map())

    val name = Try(config.getString("Name")).toOption

    KafkaOutput(
      brokers,
      topic,
      processingTimeTrigger,
      timeout,
      mode,
      options,
      config,
      name
    )
  }
}
