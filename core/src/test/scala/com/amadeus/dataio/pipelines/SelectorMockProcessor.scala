package com.amadeus.dataio.pipelines

import com.amadeus.dataio.processing.Transformer
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * Test purpose processor which select from, date column and return a Dataset[TestTrip]
 */
class SelectorMockProcessor extends Transformer {

  /**
   * The output type of the data. It will be used to defined the schema of the output dataset.
   */
  override type T = TestSummaryTrip

  /**
   * Transforms some input data and returns it under a pre-defined schema.
   *
   * @param inputData The data to transform.
   * @return A new Dataset[T] transformed.
   */
  override def featurize(inputData: DataFrame): Dataset[TestSummaryTrip] = {
    import inputData.sparkSession.implicits._

    inputData.select($"from", $"date").as[TestSummaryTrip]
  }
}
