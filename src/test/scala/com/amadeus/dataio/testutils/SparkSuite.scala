package com.amadeus.dataio.testutils

import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.{BeforeAndAfter, Suite}

/**
 * Provides function for spark tests
 *
 * Initialize the spark session before the tests
 * Automatically close the session after TestSuite is finished
 * Provides a wrapper to spark.implicits by importing sparkTestImplicits
 * Provides functions to collect results
 *
 * e.g.
 * {{{
 *   class MyClassTest extends AnyWordSpec with SparkTest {
 *      // provided by SparkSuite:
 *      // sparkSession: SparkSession
 *      // sparkTestImplicits
 *   }
 * }}}
 */
trait SparkSuite extends Suite with BeforeAndAfter {

  implicit var sparkSession: SparkSession = _

  object sparkTestImplicits extends SQLImplicits with Serializable {
    protected override def _sqlContext: SQLContext = sparkSession.sqlContext
  }

  before {
    sparkSession = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .appName(getTestName)
      .getOrCreate()
  }

  after {
    sparkSession.close()
  }

  /**
   * Used to give app name to spark session
   *
   * @return the app name
   */
  def getTestName: String

  /**
   * Collect data from file system
   *
   * @param path the data's path
   * @param format the data's format
   * @param schema the schema of dataframe to be read. Infer schema by default
   * @return the data read as a Array[String] using org.apache.spark.sql.Row toString
   */
  def collectData(path: String, format: String, schema: Option[String] = None): Array[String] = {
    var dataFrameReader = sparkSession.read

    dataFrameReader = schema match {
      case Some(definedSchema) => dataFrameReader.schema(definedSchema)
      case None                => dataFrameReader.option("inferSchema", "true")
    }

    val dataFrame = dataFrameReader.format(format).load(path).collect()

    dataFrame.map(row => row.toString())
  }
}
