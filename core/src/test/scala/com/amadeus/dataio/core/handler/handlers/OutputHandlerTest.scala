package com.amadeus.dataio.core.handler.handlers

import com.amadeus.dataio.config.ConfigNodeCollection
import com.amadeus.dataio.pipes.spark.batch
import com.amadeus.dataio.pipes.spark.streaming.SparkOutput
import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.Duration

class OutputHandlerTest extends AnyWordSpec with Matchers {

  "Output handler" should {

    "return my SparkOutput" in {
      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Map(
            "type"         -> "com.amadeus.dataio.pipes.spark.streaming.SparkOutput",
            "name"         -> "my-test-spark",
            "path"         -> "output/fileStreamOutputConfigTest",
            "format"       -> "delta",
            "partition_by" -> "upd_date,version",
            "mode"         -> "update",
            "timeout"      -> "24h",
            "options" -> Map(
              "\"spark.sql.parquet.compression.codec\"" -> "snappy",
              "checkpointLocation"                      -> "maprfs://bktv001//test/checkpoint",
              "mergeSchema"                             -> "true"
            )
          )
        )
      )

      val configCollection = ConfigNodeCollection("Output", config)
      val outputHandler    = OutputHandler(configCollection)

      val storageStreamOutput = outputHandler.getOne("my-test-spark").asInstanceOf[SparkOutput]

      storageStreamOutput.format shouldEqual Some("delta")
      storageStreamOutput.path shouldEqual Some("output/fileStreamOutputConfigTest")
      storageStreamOutput.partitionByColumns shouldEqual Seq("upd_date", "version")
      storageStreamOutput.trigger shouldEqual None
      storageStreamOutput.timeout shouldEqual Some(86400000)
      storageStreamOutput.mode shouldEqual "update"
      storageStreamOutput.options shouldEqual Map(
        "spark.sql.parquet.compression.codec" -> "snappy",
        "checkpointLocation"                  -> "maprfs://bktv001//test/checkpoint",
        "mergeSchema"                         -> "true"
      )

    }

    "return my streaming SparkInput and my batch SparkInput " in {
      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Seq(
            Map(
              "type"         -> "com.amadeus.dataio.pipes.spark.streaming.SparkOutput",
              "name"         -> "my-streaming-spark",
              "path"         -> "output/fileStreamOutputConfigTest",
              "format"       -> "delta",
              "partition_by" -> "upd_date,version",
              "mode"         -> "update",
              "duration"     -> "60 seconds",
              "timeout"      -> "24h",
              "options" -> Map(
                "\"spark.sql.parquet.compression.codec\"" -> "snappy",
                "checkpointLocation"                      -> "maprfs://bktv001//test/checkpoint",
                "mergeSchema"                             -> "true"
              )
            ),
            Map(
              "type"   -> "com.amadeus.dataio.pipes.spark.batch.SparkOutput",
              "name"   -> "my-batch-spark",
              "path"   -> "output/fileBatchOutputConfigTest",
              "format" -> "parquet"
            )
          )
        )
      )

      val configCollection = ConfigNodeCollection("Output", config)
      val outputHandler    = OutputHandler(configCollection)

      var batchStorageFound     = false
      var streamingStorageFound = false

      for (output <- outputHandler.getAll) {
        output match {
          case storage: batch.SparkOutput =>
            storage.path shouldEqual Some("output/fileBatchOutputConfigTest")
            storage.format shouldEqual Some("parquet")
            batchStorageFound = true

          case streamingStorage: SparkOutput =>
            streamingStorage.format shouldEqual Some("delta")
            streamingStorage.path shouldEqual Some("output/fileStreamOutputConfigTest")
            streamingStorage.partitionByColumns shouldEqual Seq("upd_date", "version")
            streamingStorage.trigger shouldEqual Some(Trigger.ProcessingTime(Duration("60 seconds")))
            streamingStorage.timeout shouldEqual Some(86400000)
            streamingStorage.mode shouldEqual "update"
            streamingStorage.options shouldEqual Map(
              "spark.sql.parquet.compression.codec" -> "snappy",
              "checkpointLocation"                  -> "maprfs://bktv001//test/checkpoint",
              "mergeSchema"                         -> "true"
            )
            streamingStorageFound = true
        }
      }

      batchStorageFound shouldEqual true
      streamingStorageFound shouldEqual true
    }
  }

}
