package com.amadeus.dataio.core.handler.handlers

import com.amadeus.dataio.config.ConfigNodeCollection
import com.amadeus.dataio.pipes.storage.batch
import com.amadeus.dataio.pipes.storage.streaming.StorageOutput
import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.Duration

class OutputHandlerTest extends AnyWordSpec with Matchers {

  "Output handler" should {

    "return my StorageOutput" in {
      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Map(
            "Type"               -> "com.amadeus.dataio.pipes.storage.streaming.StorageOutput",
            "Name"               -> "my-test-storage",
            "Path"               -> "output/fileStreamOutputConfigTest",
            "Format"             -> "delta",
            "PartitioningColumn" -> "upd_date,version",
            "Mode"               -> "update",
            "Timeout"            -> "24",
            "Options" -> Map(
              "\"spark.sql.parquet.compression.codec\"" -> "snappy",
              "checkpointLocation"                      -> "maprfs://bktv001//test/checkpoint",
              "mergeSchema"                             -> "true"
            )
          )
        )
      )

      val configCollection = ConfigNodeCollection("Output", config)
      val outputHandler    = OutputHandler(configCollection)

      val storageStreamOutput = outputHandler.getOne("my-test-storage").asInstanceOf[StorageOutput]

      storageStreamOutput.format shouldEqual "delta"
      storageStreamOutput.path shouldEqual "output/fileStreamOutputConfigTest"
      storageStreamOutput.partitioningColumns shouldEqual Seq("upd_date", "version")
      storageStreamOutput.trigger shouldEqual None
      storageStreamOutput.timeout shouldEqual 86400000
      storageStreamOutput.mode shouldEqual "update"
      storageStreamOutput.options shouldEqual Map(
        "spark.sql.parquet.compression.codec" -> "snappy",
        "checkpointLocation"                  -> "maprfs://bktv001//test/checkpoint",
        "mergeSchema"                         -> "true"
      )

    }

    "return my streaming StorageInput and my batch StorageInput " in {
      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Seq(
            Map(
              "Type"               -> "com.amadeus.dataio.pipes.storage.streaming.StorageOutput",
              "Name"               -> "my-streaming-storage",
              "Path"               -> "output/fileStreamOutputConfigTest",
              "Format"             -> "delta",
              "PartitioningColumn" -> "upd_date,version",
              "Mode"               -> "update",
              "Duration"           -> "60 seconds",
              "Timeout"            -> "24",
              "Options" -> Map(
                "\"spark.sql.parquet.compression.codec\"" -> "snappy",
                "checkpointLocation"                      -> "maprfs://bktv001//test/checkpoint",
                "mergeSchema"                             -> "true"
              )
            ),
            Map(
              "Type"   -> "com.amadeus.dataio.pipes.storage.batch.StorageOutput",
              "Name"   -> "my-batch-storage",
              "Path"   -> "output/fileBatchOutputConfigTest",
              "Format" -> "parquet"
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
          case storage: batch.StorageOutput =>
            storage.path shouldEqual "output/fileBatchOutputConfigTest"
            storage.format.get shouldEqual "parquet"
            batchStorageFound = true

          case streamingStorage: StorageOutput =>
            streamingStorage.format shouldEqual "delta"
            streamingStorage.path shouldEqual "output/fileStreamOutputConfigTest"
            streamingStorage.partitioningColumns shouldEqual Seq("upd_date", "version")
            streamingStorage.trigger shouldEqual Some(Trigger.ProcessingTime(Duration("60 seconds")))
            streamingStorage.timeout shouldEqual 86400000
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
