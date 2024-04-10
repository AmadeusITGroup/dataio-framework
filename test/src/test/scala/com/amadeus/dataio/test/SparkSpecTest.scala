package com.amadeus.dataio.test

import org.scalatest.flatspec.AnyFlatSpec

class SparkSpecTest extends AnyFlatSpec with SparkSpec {
  override def getTestName: String = "SparkSpecTest"

  val dataPath  = getClass.getResource("/spark-spec/data.csv").getPath
  val emptyPath = getClass.getResource("/spark-spec/empty.csv").getPath

  "SparkSpec" should "collect data from file system" in {
    // Collect data from CSV file
    val collectedData1 = collectData(dataPath, "csv")
    assert(collectedData1.length == 3) // Three lines excluding header
    assert(collectedData1.contains("[1,John,30]"))
    assert(collectedData1.contains("[2,Alice,25]"))
    assert(collectedData1.contains("[3,Bob,35]"))

    // Attempt to collect data from empty CSV file
    val collectedData2 = collectData(emptyPath, "csv")
    assert(collectedData2.isEmpty)
  }

  it should "collect data with defined schema from file system" in {
    // Assuming data is present at "path" in "format" with a defined schema
    val schema = "id INT, name STRING, age INT"

    // Collect data from CSV file with defined schema
    val collectedData1 = collectData(dataPath, "csv", Some(schema))
    // Verify collected data
    assert(collectedData1.length == 3) // Three lines excluding header
    assert(collectedData1.contains("[1,John,30]"))
    assert(collectedData1.contains("[2,Alice,25]"))
    assert(collectedData1.contains("[3,Bob,35]"))

    // Collect data from empty CSV file with defined schema
    val collectedData2 = collectData(emptyPath, "csv", Some(schema))
    // Verify collected data
    assert(collectedData2.isEmpty)
  }
}
