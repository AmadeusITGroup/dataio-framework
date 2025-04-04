package com.amadeus.dataio.pipes.snowflake.streaming

import com.amadeus.dataio.core.{Logging, Output}
import org.apache.spark.sql.streaming.Trigger
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scala.util.Try

/**
 * Allows to write stream data to Snowflake.
 *
 * \!/ It uses the snowflake connector and therefore guarantees only at least once delivery. \!/
 *
 * @param trigger the trigger to be used for the streaming query.
 * @param timeout the streaming query timeout.
 * @param mode the mode to use.
 * @param options the snowflake connector options.
 * @param addTimestampOnInsert if true, a timestamp column wit the current timestamp will be added to the data.
 * @param config the config object.
 * @param outputName the output name used to define the streaming query name.
 */
case class SnowflakeOutput(
    timeout: Long,
    trigger: Option[Trigger],
    mode: String,
    options: Map[String, String],
    addTimestampOnInsert: Boolean,
    config: Config = ConfigFactory.empty(),
    outputName: Option[String]
) extends Output
    with Logging {

  val SNOWFLAKE_CONNECTOR_NAME = "net.snowflake.spark.snowflake"

  /**
   * Writes data to this output.
   *
   * @param data  The data to write.
   * @param spark The SparkSession which will be used to write the data.
   */
  def write[T](data: Dataset[T])(implicit spark: SparkSession): Unit = {

    val dbTable: Option[String] = options.get("dbtable")
    dbTable.foreach(table => logger.info(s"Writing dataframe to snowflake table ${table}"))
    trigger.foreach(trigger => logger.info(s"Using trigger ${trigger}"))

    logger.info(s"Add timestamp on insert: $addTimestampOnInsert")

    val queryName = createQueryName()

    var streamWriter = data.writeStream.queryName(queryName)

    streamWriter = trigger match {
      case Some(trigger) => streamWriter.trigger(trigger)
      case _             => streamWriter
    }

    streamWriter.foreachBatch((batchDF: Dataset[T], _: Long) => {
      addTimestampOnInsert(batchDF).write
        .format(SNOWFLAKE_CONNECTOR_NAME)
        .options(options)
        .mode(mode)
        .save()
    })

    val streamingQuery = streamWriter.start()

    streamingQuery.awaitTermination(timeout)
    streamingQuery.stop()
  }

  /**
   * Add current timestamp to the data if needed.
   *
   * @param data the dataset to enrich.
   * @tparam T the dataset type.
   * @return the dataset with the timestamp column if needed.
   */
  private def addTimestampOnInsert[T](data: Dataset[T]): DataFrame = {
    if (addTimestampOnInsert) {
      data.withColumn("timestamp", current_timestamp())
    } else {
      data.toDF()
    }
  }

  /**
   * Create a unique query name based on output topic.
   *
   * @return a unique query name.
   */
  private[streaming] def createQueryName(): String = {

    val dbTable: Option[String] = options.get("dbtable")

    (outputName, dbTable) match {
      case (Some(name), Some(table)) => s"QN_${name}_${table}_${java.util.UUID.randomUUID}"
      case (Some(name), None)        => s"QN_${name}_${java.util.UUID.randomUUID}"
      case (None, Some(table))       => s"QN_${table}_${java.util.UUID.randomUUID}"
      case _                         => s"QN_${java.util.UUID.randomUUID}"
    }

  }
}

object SnowflakeOutput {
  import com.amadeus.dataio.config.fields._

  /**
   * Creates a new instance of SnowflakeOutput from a typesafe Config object.
   *
   * @param config typesafe Config object containing the configuration fields.
   * @return a new SnowflakeOutput object.
   * @throws com.typesafe.config.ConfigException If any of the mandatory fields is not available in the config argument.
   */
  def apply(implicit config: Config): SnowflakeOutput = {

    val mode                 = config.getString("Mode")
    val addTimestampOnInsert = Try(config.getBoolean("AddTimestampOnInsert")).getOrElse(false)

    val duration = Try(config.getString("Duration")).toOption
    val trigger  = duration.map(Trigger.ProcessingTime)

    val name = Try(config.getString("Name")).toOption

    SnowflakeOutput(
      timeout = getTimeout,
      trigger = trigger,
      mode = mode,
      options = getOptions,
      addTimestampOnInsert = addTimestampOnInsert,
      config = config,
      outputName = name
    )
  }
}
