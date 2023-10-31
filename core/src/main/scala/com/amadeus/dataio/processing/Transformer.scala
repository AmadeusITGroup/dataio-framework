package com.amadeus.dataio.processing

import com.amadeus.dataio.HandlerAccessor
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Basic transform pipeline, using a single input and a single output.
 */
trait Transformer extends Processor {

  /**
   * Runs a basic transform pipeline, using a single input and a single output.
   * @param handlers An instance of HandlerAccessor to access inputs, outputs, etc.
   * @param spark The SparkSession which will be used to process data.
   */
  override def run(handlers: HandlerAccessor)(implicit spark: SparkSession): Unit = {
    if (handlers.input.getAll.isEmpty) throw new Exception("Can not run a transformer without an input configuration.")
    if (handlers.output.getAll.isEmpty) throw new Exception("Can not run a transformer without an output configuration.")

    val input     = handlers.input.getAll.head
    val inputData = input.read

    val transformedData = featurize(inputData)

    handlers.output.getAll.head.write(transformedData)
  }

  /**
   * The output type of the data. It will be used to defined the schema of the output dataset.
   */
  type T

  /**
   * Transforms some input data and returns it under a pre-defined schema.
   * @param inputData The data to transform.
   * @return A new Dataset[T] transformed.
   */
  def featurize(inputData: DataFrame): Dataset[T]
}
