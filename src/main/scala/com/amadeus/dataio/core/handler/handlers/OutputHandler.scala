package com.amadeus.dataio.core.handler.handlers

import com.amadeus.dataio.config.ConfigNodeCollection
import com.amadeus.dataio.core.{Logging, Output}
import com.amadeus.dataio.core.handler.Handler
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * <p>The OutputHandler is meant to serve as facade to write data as batch or
 * streaming.<br/>
 * It contains the list of configured outputs and passes the information about
 * the data to them each time one of the writing methods is called.</p>
 *
 * <p>Use as follows:
 * {{{
 *   val config: ConfigNodeCollection = (...)
 *   val outputHandler = OutputHandler(config)
 *   val data = (...)
 *   outputHandler.write("output-name", data)(sparkSession)
 * }}}
 * </p>
 */
case class OutputHandler() extends Handler[Output] {

  /**
   * Writes data to a given output.
   * @param outputName The name of the output to write to.
   * @param data The data to write.
   * @param spark The SparkSession which will be used to write the data.
   * @throws Exception if the output does not exist.
   */
  def write[T](outputName: String, data: Dataset[T])(implicit spark: SparkSession): Unit = {
    val output = getOne[Output](outputName)

    output.write(data)
  }
}

object OutputHandler extends Logging {

  /**
   * Creates an OutputHandler based on a given configuration.
   * @param outputConfig The collection of config nodes that will be used to instantiate outputs.
   * @return a new instance of OutputHandler.
   */
  def apply(outputConfig: ConfigNodeCollection): OutputHandler = {
    val configNodes = outputConfig.nodes
    val handler     = OutputHandler()
    configNodes.foreach(n => handler.add(n.name, Output(n)))

    handler
  }
}
