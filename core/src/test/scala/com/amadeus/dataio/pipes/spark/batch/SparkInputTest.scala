package com.amadeus.dataio.pipes.spark.batch

import com.amadeus.dataio.pipes.spark.{SparkPathSource, SparkTableSource}
import com.amadeus.dataio.testutils.SparkSpec

import java.nio.file.Paths

class SparkInputTest extends SparkSpec {

  behavior of "SparkInput.read"

  it should "load a CSV and match expected output" in sparkTest { implicit spark =>
    val basePath     = "/pipes/spark/batch/SparkInput/1_load/"
    val inputPath    = getClass.getResource(s"$basePath/input.csv").getPath
    val expectedPath = getClass.getResource(s"$basePath/expected.csv").getPath

    val input = SparkInput(
      name = "csv-test",
      source = Some(SparkPathSource(inputPath, Some("csv"))),
      options = Map("header" -> "true", "inferSchema" -> "true")
    )

    val actualDf = input.read
    val expectedDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(expectedPath)

    actualDf should notBeEmpty
    actualDf should haveTheSameSchemaAs(expectedDf)
    actualDf should containTheSameRowsAs(expectedDf)
  }

  it should "read from a table match expected output" in sparkTest { implicit spark =>
    val basePath     = "/pipes/spark/batch/SparkInput/2_table/"
    val inputPath    = getClass.getResource(s"$basePath/input.csv").getPath
    val expectedPath = getClass.getResource(s"$basePath/expected.csv").getPath

    val df = spark.read.option("header", "true").csv(inputPath)
    df.createOrReplaceTempView("input_table")

    val input = SparkInput(
      name = "csv-test",
      source = Some(SparkTableSource("input_table")),
      options = Map("header" -> "true")
    )

    val actualDf = input.read
    val expectedDf = spark.read
      .option("header", "true")
      .csv(expectedPath)

    actualDf should notBeEmpty
    actualDf should haveTheSameSchemaAs(expectedDf)
    actualDf should containTheSameRowsAs(expectedDf)
  }
}
