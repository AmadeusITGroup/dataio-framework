package com.amadeus.dataio.test

import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.{BeforeAndAfter, TestSuite}

/**
 * Provides functions for Spark tests:
 * <ul>
 *   <li>Initializes the Spark Session before each test</li>
 *   <li>Automatically closes the session after each test</li>
 *   <li>Provides a wrapper to spark.implicits by importing sparkTestImplicits</li>
 *   <li>Provides functions to collect results from the FileSystem</li>
 * </ul>
 *
 * e.g.
 * {{{
 *   class MyClassTest extends AnyFlatSpec with SparkSpec {
 *      // provided by SparkSpec:
 *      // sparkSession: SparkSession
 *      // sparkTestImplicits
 *   }
 * }}}
 */
trait SparkSpec extends TestSuite with BeforeAndAfter {

  implicit var spark: SparkSession = _

  object sparkTestImplicits extends SQLImplicits with Serializable {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }

  before {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config(overrideSparkConf)
      .appName(getTestName)
      .getOrCreate()
  }

  after {
    spark.close()
  }

  /**
   * Override to edit the Spark configuration.
   * @return The map of new configuration values.
   */
  def overrideSparkConf: Map[String, String] = Map.empty

  /**
   * @return the test suite's name
   */
  def getTestName: String

  /**
   * Collects data from file system.
   *
   * @param path the data's path
   * @param format the data's format
   * @param schema the schema of dataframe to be read. Infer schema by default
   * @return the data read as a Array[String] using org.apache.spark.sql.Row toString
   */
  def collectData(path: String, format: String, schema: Option[String] = None): Array[String] = {
    var dataFrameReader = spark.read

    dataFrameReader = schema match {
      case Some(definedSchema) => dataFrameReader.schema(definedSchema)
      case None                => dataFrameReader.option("inferSchema", "true")
    }

    val dataFrame = dataFrameReader.format(format).load(path).collect()

    dataFrame.map(row => row.toString())
  }
}
