package com.amadeus.dataio.testutils

import org.apache.spark.sql.SparkSession

/** A utility trait for simplified local Spark testing.
  *
  * This trait provides a convenient method to create and manage a local Spark session
  * for unit testing Spark-based code with minimal boilerplate.
  *
  * The trait automatically handles SparkSession creation, configuration, and cleanup,
  * making it easier to write concise Spark tests.
  *
  * === Example Usage ===
  * {{{
  * class MySparkTest extends AnyFlatSpec with LocalSpark {
  *   it should "process data correctly" in sparkTest { spark =>
  *     val data = Seq((1, "test"))
  *     val df = spark.createDataFrame(data).toDF("id", "value")
  *     assert(df.count() == 1)
  *   }
  * }
  * }}}
  *
  * @note The Spark session is created with:
  *       - Local master mode using all available cores
  *       - Spark UI disabled
  *       - Randomly generated application name
  */
trait LocalSpark {

  /** Creates and manages a local Spark session for testing.
    *
    * This method ensures:
    * - A SparkSession is created with local configuration
    * - The session is properly closed after the test
    *
    * @param test A function that receives the SparkSession and performs testing logic
    * @define sessionManagement
    * @define localTestingSupport
    */
  def sparkTest(test: SparkSession => Unit): Unit = {
    lazy val spark = buildLocalSparkSession()

    try {
      test(spark)
    } finally {
      spark.close()
    }
  }

  private def buildLocalSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .appName(new scala.util.Random().nextInt().toString)
      .getOrCreate()
  }
}
