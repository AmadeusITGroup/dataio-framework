package com.amadeus.dataio.processing

import com.amadeus.dataio.config.ConfigNode
import com.amadeus.dataio.core.InstantiationHelper
import com.amadeus.dataio.HandlerAccessor
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

/**
 * Unified interface for jobs using this framework.
 *
 * Configuration fields provided in the configuration are available in the `config` member.
 *
 * e.g.
 * {{{
 *   class MyJob extends Processor {
 *     def run(handlers: HandlerAccessor)(implicit spark: SparkSession): Unit = {
 *       val input = handlers.input.read("my-input")
 *
 *       val myValue = config.getString("MyValue")
 *
 *       val result = input.filter(...)
 *
 *       handlers.output.write("my-output", result)
 *     }
 *   }
 * }}}
 */
trait Processor {

  // This is more of a c# convention for naming private fields associated with a public property (getter/setter). See below.
  private var _config: Config = ConfigFactory.empty()

  /**
   * Returns the configuration of this processor, as a typesafe Config object.
   *
   * e.g.
   * {{{
   *   val myValue = config.getString("MyValue")
   *   val myList = config.getStringList("MyList")
   * }}}
   *
   * @return the configuration of this processor.
   */
  def config: Config = _config /* This getter gives read-only access to the _config field, which can't be set as val as
  it needs to be set after instantiating the processor. */

  /**
   * Runs a full data processing pipeline. This is where most of the logic
   * must be called to process data.
   * @param handlers An instance of HandlerAccessor to access inputs, outputs, etc.
   * @param spark The SparkSession which will be used to process data.
   */
  def run(handlers: HandlerAccessor)(implicit spark: SparkSession): Unit
}

object Processor {

  /**
   * <p>Creates an instance of a Processor subclass.</p>
   * @param config The configuration of the Processor.
   * @return An instance of the Processor subclass provided in the config argument.
   */
  def apply(config: ConfigNode): Processor = {
    val processor = InstantiationHelper.instantiateWithEmptyConstructor[Processor](config.typeName)

    processor._config = config.config

    processor
  }
}
