package com.amadeus.dataio.pipelines

import com.amadeus.dataio.config.PipelineConfig
import com.amadeus.dataio.core.SchemaRegistry
import com.amadeus.dataio.testutils.{FileSystemSuite, SparkStreamingSuite}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PipelineTest extends AnyWordSpec with SparkStreamingSuite with FileSystemSuite with Matchers {

  "My test streaming Pipeline with schema" should {
    "return selected columns From,Date" in {

      enableSparkStreamingSchemaInference()

      SchemaRegistry.registerSchema[TestTrip]()

      val config = PipelineConfig(getClass.getResource("/pipelines/conf/SparkStreamingPipelineSchemaTest.conf").getPath)
      Pipeline(config).run

      val outputPath = config.output.nodes.head.config.getString("Path")

      val results  = collectData(outputPath, "parquet", Option("from string, date string")).sortWith(_ < _)
      val expected = Array("[Paris,2021-08-19]", "[Sydney,2022-06-15]", "[Turin,2022-03-02]")
      results shouldEqual expected

    }
  }

  "My test streaming Pipeline without schema" should {
    "return selected columns From,Date" in {

      enableSparkStreamingSchemaInference()

      val config = PipelineConfig(getClass.getResource("/pipelines/conf/SparkStreamingPipelineTest.conf").getPath)
      Pipeline(config).run

      val outputPath = config.output.nodes.head.config.getString("Path")

      val results  = collectData(outputPath, "parquet").sortWith(_ < _)
      val expected = Array("[Paris,2021-08-19]", "[Turin,2022-03-02]")
      results shouldEqual expected

    }
  }

  "My test streaming Pipeline without partitionColumns" should {
    "return selected columns From,Date" in {

      enableSparkStreamingSchemaInference()

      SchemaRegistry.registerSchema[TestTrip]()

      val config = PipelineConfig(getClass.getResource("/pipelines/conf/SparkStreamingPipelinePartitionTest.conf").getPath)
      Pipeline(config).run

      val outputPath = config.output.nodes.head.config.getString("Path")

      val results  = collectData(outputPath, "parquet", Option("from string, date string")).sortWith(_ < _)
      val expected = Array("[Paris,2021-08-19]", "[Sydney,2022-06-15]", "[Turin,2022-03-02]")
      results shouldEqual expected

    }
  }

  "My test batch Pipeline" should {
    "return selected columns From,Date" in {

      val config = PipelineConfig(getClass.getResource("/pipelines/conf/SparkBatchPipelineTest.conf").getPath)
      Pipeline(config).run

      val outputPath = config.output.nodes.head.config.getString("Path")

      val results  = collectData(outputPath, "parquet").sortWith(_ < _)
      val expected = Array("[Paris,2021-08-19]", "[Turin,2022-03-02]")
      results shouldEqual expected

    }
  }

  "My test batch Pipeline with schema" should {
    "return selected columns From,Date" in {

      SchemaRegistry.registerSchema[TestTrip]()

      val config = PipelineConfig(getClass.getResource("/pipelines/conf/SparkBatchPipelineSchemaTest.conf").getPath)
      Pipeline(config).run

      val outputPath = config.output.nodes.head.config.getString("Path")

      val results  = collectData(outputPath, "parquet").sortWith(_ < _)
      val expected = Array("[Paris,2021-08-19]", "[Turin,2022-03-02]")
      results shouldEqual expected

    }
  }

  /**
   * Used to give app name to spark session
   *
   * @return the app name
   */
  override def getTestName: String = "PipelineSparkTest"
}
