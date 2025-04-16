package com.amadeus.dataio.pipes.kafka.streaming

import com.amadeus.dataio.core.{Logging, Output}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

/** Class for reading kafka dataframe
 *
 * @param config Contains the Typesafe Config object that was used at instantiation to configure this entity.
 */
case class KafkaOutput(
                        name: String,
    trigger: Option[Trigger],
                        timeout: Option[Long],
    mode: String,
    options: Map[String, String] = Map(),
                        config: Config = ConfigFactory.empty()
) extends Output
    with Logging {

  def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {
    logger.info(s"writing to kafka: $name")
    if (options.nonEmpty) logger.info(s"options: $options")
    logger.info(s"mode: $mode")

    val queryName = createQueryName()

    var dsWriter = data.writeStream
      .queryName(queryName)
      .format("kafka")
      .options(options)
      .outputMode(mode)

    dsWriter = trigger match {
      case Some(t) =>
        logger.info(s"trigger: $t")
        dsWriter.trigger(t)
      case _ => dsWriter
    }

    val streamingQuery = dsWriter.start()

    if (timeout.isDefined) {
      val t = timeout.get
      logger.info(s"timeout: $t")
      streamingQuery.awaitTermination(t)
    }

    streamingQuery.stop()
  }

  /** Create a unique query name based on output topic.
   *
   * @return a unique query name.
   */
  private[streaming] def createQueryName(): String = {

    s"QN_${name}_${java.util.UUID.randomUUID}"
  }
}

object KafkaOutput {
  import com.amadeus.dataio.config.fields._

  /** Creates an KafkaOutput based on a given configuration.
   *
   * @param config The collection of config nodes that will be used to instantiate KafkaOutput.
   * @return a new instance of KafkaOutput.
   */
  def apply(implicit config: Config): KafkaOutput = {
    val name = Try {
      config.getString("name")
    } getOrElse {
      throw new Exception("Missing required `name` field in configuration.")
    }
    val trigger = getStreamingTrigger

    val timeout = getTimeout
    val mode    = config.getString("Mode")
    val options = Try(getOptions).getOrElse(Map())

    KafkaOutput(
      name,
      trigger,
      timeout,
      mode,
      options,
      config
    )
  }
}
