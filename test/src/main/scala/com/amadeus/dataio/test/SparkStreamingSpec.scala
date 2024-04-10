package com.amadeus.dataio.test

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

/**
 * Provides functions for Spark Streaming tests:
 * <ul>
 *   <li>Initializes the Spark Session before each test</li>
 *   <li>Automatically closes the session after each test</li>
 *   <li>Provides a wrapper to spark.implicits by importing sparkTestImplicits</li>
 *   <li>Provides functions to collect results from the FileSystem</li>
 * </ul>
 *
 * e.g.
 * {{{
 *   class MyClassTest extends AnyFlatSpec with SparkStreamingSpec {
 *      // provided by SparkStreamingSpec:
 *      // sparkSession: SparkSession
 *      // sparkTestImplicits
 *   }
 * }}}
 */
trait SparkStreamingSpec extends SparkSpec {

  /**
   * enable spark streaming schema inference
   */
  def enableSparkStreamingSchemaInference(): Unit = spark.sql("set spark.sql.streaming.schemaInference=true")

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

    val resultData = spark.sql("select * from " + tmpTableName).collect()

    resultData.map(row => row.toString())
  }

}
