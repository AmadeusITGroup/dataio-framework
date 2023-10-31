package com.amadeus.dataio.testutils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

/**
 * Provides function for spark streaming tests
 *
 * Initialize the spark session before the tests
 * Automatically close the session after TestSuite is finished
 * Provides a wrapper to spark.implicits by importing sparkTestImplicits
 * Provides functions to collect results
 *
 * e.g.
 * {{{
 *   class MyClassTest extends AnyWordSpec with SparkStreamingSuite {
 *      // provided by SparkStreamingSuite:
 *      // sparkSession: SparkSession
 *      // sparkTestImplicits
 *   }
 * }}}
 */
trait SparkStreamingSuite extends SparkSuite {

  /**
   * enable spark streaming schema inference
   */
  def enableSparkStreamingSchemaInference(): Unit = sparkSession.sql("set spark.sql.streaming.schemaInference=true")

  /**
   * Collect data from dataframe read from stream
   *
   * Use an in memory sink to gather the data
   *
   * @param dataFrame the dataFrame to collect.
   * @return the String representation of each rows.
   */
  def collectDataStream(dataFrame: DataFrame): Array[String] = {

    val tmpTableName = "result_" + System.currentTimeMillis()
    val streamWriter = dataFrame.writeStream
      .format("memory")
      .queryName(tmpTableName)
      .trigger(Trigger.Once())
      .start()

    streamWriter.awaitTermination(2000)

    val resultData = sparkSession.sql("select * from " + tmpTableName).collect()

    resultData.map(row => row.toString())
  }

}
