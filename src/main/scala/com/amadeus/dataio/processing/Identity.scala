package com.amadeus.dataio.processing
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class Identity extends Transformer {

  /**
   * The output type of the data. As it is a generic transformer, this is set as Row.
   */
  override type T = Row

  /**
   * Returns the input data without transforming it.
   * @param inputData The data.
   * @return The inputData.
   */
  override def featurize(inputData: DataFrame): Dataset[Row] = {
    inputData
  }
}
