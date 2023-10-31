package com.amadeus.dataio.pipelines

import com.amadeus.dataio.config.PipelineConfig
import com.amadeus.dataio.core.Logging
import com.amadeus.dataio.core.handler.HandlerAccessor
import com.amadeus.dataio.processing.Processor
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
 * Represents a data pipeline. It contains a unique <em>run()</em> method that glues all Data IO concepts together by:
 * <ol>
 *   <li>Building the IO components accessor,</li>
 *   <li>Instantiating the data processor,</li>
 *   <li>Running the data processor.</li>
 * </ol>
 * <br/>
 * Use as follows:
 *
 * {{{
 *   val spark: SparkSession = (...)
 *   val config: PipelineConfig = (...)
 *   val pipeline = Pipeline(config)
 *   pipeline.run(spark)
 * }}}
 */
case class Pipeline(config: PipelineConfig) extends Logging {

  /**
   * Instantiates components and runs the data processor of the pipeline.
   * @param spark The SparkSession to use for the entire pipeline.
   */
  def run(implicit spark: SparkSession): Unit = {
    logger.info("Creating IO components.")

    val handlerAccessor = HandlerAccessor(config)

    logger.info(s"Creating data processor.")
    val processor = Try {
      Processor(config.processing.nodes.head) // TODO: refactor the processing config node, having a collection is not necessary anymore
    }.getOrElse(
      throw new Exception(
        "Can not run a data pipeline if there are no configured data processors. " +
          "If no transformation of data is required in your job, you may use com.amadeus.dataio.processing.Identity (explicitly)."
      )
    )

    logger.info("Running data processor.")
    processor.run(handlerAccessor)
  }
}

object Pipeline {

  /**
   * Creates a pipeline directly from a typesafe Config object. It must follow Data IO
   * configuration structure.
   * @param c The configuration of the pipeline, as a typesafe Config object.
   * @return The new Data IO pipeline.
   */
  def apply(c: Config): Pipeline = new Pipeline(PipelineConfig(c))

  /**
   * Creates a pipeline directly from a configuration file path. It must follow Data IO
   * configuration structure.
   *
   * @param configLocation The path of the configuration file.
   * @return The new Data IO pipeline.
   */
  def apply(configLocation: String): Pipeline = new Pipeline(PipelineConfig(configLocation))
}
