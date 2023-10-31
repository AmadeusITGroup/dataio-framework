package com.amadeus

/**
 * Public APIs of Data IO
 */
package object dataio {

  /**
   * Provides a unified way to access the configured handlers of this application.
   *
   * @param input        The InputHandler, to easily read dataframes.
   * @param output       The OutputHandler, to easily write datasets.
   * @param distribution The DistributionHandler, to easily distribute reports to external destinations (email, etc.).
   */
  type HandlerAccessor = com.amadeus.dataio.core.handler.HandlerAccessor

  /**
   * This logging trait allows you to use a logger instance even in distributed applications.
   *
   * You must use this feature when you create an object that is going to be used into a Spark streaming application.
   *
   * Usage example:
   * {{{
   * class MyClass extends Logging {
   *   ...
   *   logger.debug("your message goes here")
   *   logger.info("your message goes here")
   *   logger.warn("your message goes here")
   *   logger.error("your message goes here")
   *   ...
   * }
   * }}}
   */
  type Logging = com.amadeus.dataio.core.Logging

  type Pipeline = com.amadeus.dataio.pipelines.Pipeline

  /**
   * Represents a data pipeline. It contains a unique <em>run()</em> method that glues all Data IO concepts together by:
   * <ol>
   *   <li>Building the IO components accessor, or HandlerAccessor,</li>
   *   <li>Instantiating the data processor,</li>
   *   <li>Running the data processor.</li>
   * </ol>
   * <br/>
   * Use as follows:
   *
   * {{{
   *   val spark: SparkSession = (...)
   *   val pipeline = Pipeline("/path/to/config")
   *   pipeline.run(spark)
   * }}}
   */
  val Pipeline: com.amadeus.dataio.pipelines.Pipeline.type = com.amadeus.dataio.pipelines.Pipeline

  /**
   * Unified interface for transformation logic within your pipelines.
   *
   * Configuration fields provided in the Processing configuration node are available in the `config` member.
   *
   * e.g.
   * {{{
   *   class MyTransformation extends Processor {
   *     def run(handlers: HandlerAccessor)(implicit spark: SparkSession): Unit = {
   *       val input = handlers.input.read("my-input")
   *
   *       val myValue = config.getString("MyValue")
   *
   *       val result = input.filter(col("my_column") === myValue)
   *
   *       handlers.output.write("my-output", result)
   *     }
   *   }
   * }}}
   */
  type Processor = com.amadeus.dataio.processing.Processor

  /**
   * Basic transformation logic, using a single input and a single output.
   *
   * e.g.
   * {{{
   *   class MyTransformation extends Transformer {
   *     type T = Row
   *
   *     def featurize(inputData: DataFrame): Dataset[T] = {
   *       val myValue = config.getString("MyValue")
   *
   *       inputData.filter(col("my_column") === myValue)
   *     }
   *   }
   * }}}
   */
  type Transformer = com.amadeus.dataio.processing.Transformer
}
