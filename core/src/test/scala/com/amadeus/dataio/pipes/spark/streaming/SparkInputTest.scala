package com.amadeus.dataio.pipes.spark.streaming

import com.amadeus.dataio.core.SchemaRegistry
import com.amadeus.dataio.pipes.spark.{SparkPathSource, SparkTableSource}
import com.amadeus.dataio.testutils.SparkSpec
import org.scalatest.BeforeAndAfterAll

class SparkInputTest extends SparkSpec with BeforeAndAfterAll {
  case class InputSchema(year: Int, city: String)

  override def beforeAll(): Unit = {
    super.beforeAll()
    SchemaRegistry.registerSchema[InputSchema]()
  }

  behavior of "SparkInput.read"

  it should "load a CSV and match expected output" in sparkTest { implicit spark =>
    val basePath     = "/pipes/spark/streaming/SparkInput/1_load/"
    val inputPath    = getClass.getResource(s"$basePath/input/").getPath
    val expectedPath = getClass.getResource(s"$basePath/expected.csv").getPath

    val input = SparkInput(
      name = "csv-test",
      source = Some(SparkPathSource(inputPath, Some("csv"))),
      options = Map("header" -> "true"),
      schema = Some("com.amadeus.dataio.pipes.spark.streaming.SparkInputTest.InputSchema")
    )

    val streamingDf = input.read
    val query = streamingDf.writeStream
      .format("memory")
      .queryName("stream_test")
      .outputMode("append")
      .start()
    query.processAllAvailable()

    val actualDf = spark.sql("select * from stream_test")

    val expectedDf = spark.read
      .option("header", "true")
      .schema(SchemaRegistry.getSchema("com.amadeus.dataio.pipes.spark.streaming.SparkInputTest.InputSchema"))
      .csv(expectedPath)

    actualDf should notBeEmpty
    actualDf should haveTheSameSchemaAs(expectedDf)
    actualDf should containTheSameRowsAs(expectedDf)
  }

  it should "read from a table match expected output" in sparkTest { implicit spark =>
    val basePath     = "/pipes/spark/streaming/SparkInput/2_table/"
    val inputPath    = getClass.getResource(s"$basePath/input/").getPath
    val expectedPath = getClass.getResource(s"$basePath/expected.csv").getPath

    val tableDf = spark.readStream
      .option("header", "true")
      .schema(SchemaRegistry.getSchema("com.amadeus.dataio.pipes.spark.streaming.SparkInputTest.InputSchema"))
      .csv(inputPath)
    tableDf.createOrReplaceTempView("input_table")

    val input = SparkInput(
      name = "csv-test",
      source = Some(SparkTableSource("input_table")),
      options = Map("header" -> "true"),
      schema = Some("com.amadeus.dataio.pipes.spark.streaming.SparkInputTest.InputSchema")
    )

    val streamingDf = input.read
    val query = streamingDf.writeStream
      .format("memory")
      .queryName("stream_test")
      .outputMode("append")
      .start()
    query.processAllAvailable()

    val actualDf = spark.sql("select * from stream_test")
    val expectedDf = spark.read
      .option("header", "true")
      .schema(SchemaRegistry.getSchema("com.amadeus.dataio.pipes.spark.streaming.SparkInputTest.InputSchema"))
      .csv(expectedPath)

    actualDf should notBeEmpty
    actualDf should haveTheSameSchemaAs(expectedDf)
    actualDf should containTheSameRowsAs(expectedDf)
  }
}
