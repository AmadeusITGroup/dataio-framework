package com.amadeus.dataio.test

import com.amadeus.dataio.processing.Transformer
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class DummyTransformer extends Transformer {
  override type T = Row

  override def featurize(inputData: DataFrame): Dataset[Row] = {
    import inputData.sparkSession.implicits._
    inputData.filter($"year" =!= 2021)
  }
}
