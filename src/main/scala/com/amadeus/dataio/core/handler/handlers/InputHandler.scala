package com.amadeus.dataio.core.handler.handlers

import com.amadeus.dataio.config.ConfigNodeCollection
import com.amadeus.dataio.core.{Input, Logging}
import com.amadeus.dataio.core.handler.Handler
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * <p>The InputHandler is meant to serve as facade to read data as batch or
 * streaming.<br/>
 * It contains the list of configured inputs.</p>
 *
 * <p>Use as follows:
 * {{{
 *   val config: ConfigNodeCollection = (...)
 *   val inputHandler = InputHandlerHandler(config)
 *   val data = (...)
 *   inputHandler.read("input-name")(sparkSession)
 * }}}
 * </p>
 */
case class InputHandler() extends Handler[Input] {

  /**
   * Reads data from a given input.
   * @param inputName The name of the input to read from.
   * @param spark The SparkSession which will be used to read the data.
   * @return The data that was read.
   * @throws Exception If the input does not exist.
   */
  def read(inputName: String)(implicit spark: SparkSession): DataFrame = {
    val input = getOne[Input](inputName)

    input.read
  }
}

object InputHandler extends Logging {

  /**
   * Creates an InputHandler based on a given configuration.
   * @param inputConfig The collection of config nodes that will be used to instantiate inputs.
   * @return A new instance of InputHandler.
   */
  def apply(inputConfig: ConfigNodeCollection): InputHandler = {
    val configNodes = inputConfig.nodes
    val handler     = InputHandler()
    configNodes.foreach(n => handler.add(n.name, Input(n)))

    handler
  }
}
