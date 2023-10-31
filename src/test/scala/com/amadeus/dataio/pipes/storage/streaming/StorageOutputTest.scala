package com.amadeus.dataio.pipes.storage.streaming

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File
import scala.concurrent.duration.Duration

class StorageOutputTest extends AnyWordSpec with Matchers {

  "StorageOutput" should {
    "be initialized according to configuration" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Map(
            "Type"               -> "com.amadeus.dataio.pipes.storage.streaming.StorageOutput",
            "Name"               -> "my-test-storage",
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
          )
        )
      )

      val storageStreamOutput = StorageOutput.apply(config.getConfig("Output"))

      storageStreamOutput.outputName shouldEqual Some("my-test-storage")
      storageStreamOutput.format shouldEqual "delta"
      storageStreamOutput.path shouldEqual "output/fileStreamOutputConfigTest"
      storageStreamOutput.partitioningColumns shouldEqual Seq("upd_date", "version")
      Option(storageStreamOutput.processingTimeTrigger) shouldNot be(None)
      storageStreamOutput.processingTimeTrigger shouldEqual Trigger.ProcessingTime(Duration("60 seconds"))
      storageStreamOutput.timeout shouldEqual 86400000
      storageStreamOutput.mode shouldEqual "update"
      storageStreamOutput.options shouldEqual Map(
        "spark.sql.parquet.compression.codec" -> "snappy",
        "checkpointLocation"                  -> "maprfs://bktv001//test/checkpoint",
        "mergeSchema"                         -> "true"
      )
    }

    "be initialized with empty option Map" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Map(
            "Type"               -> "com.amadeus.dataio.pipes.storage.streaming.StorageOutput",
            "Name"               -> "my-test-storage",
            "Path"               -> "output/fileStreamOutputConfigTest",
            "Format"             -> "delta",
            "PartitioningColumn" -> "upd_date,version",
            "Mode"               -> "update",
            "Duration"           -> "6 hours",
            "Timeout"            -> "24"
          )
        )
      )

      val storageStreamOutput = StorageOutput.apply(config.getConfig("Output"))

      storageStreamOutput.format shouldEqual "delta"
      storageStreamOutput.path shouldEqual "output/fileStreamOutputConfigTest"
      storageStreamOutput.partitioningColumns shouldEqual Seq("upd_date", "version")
      Option(storageStreamOutput.processingTimeTrigger) shouldNot be(None)
      storageStreamOutput.processingTimeTrigger shouldEqual Trigger.ProcessingTime(Duration("6 hours"))
      storageStreamOutput.timeout shouldEqual 86400000
      storageStreamOutput.mode shouldEqual "update"
      storageStreamOutput.options shouldEqual Map()
    }

    "be initialized according to configuration without Name" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Map(
            "Type"               -> "com.amadeus.dataio.output.streaming.StorageOutput",
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
          )
        )
      )

      val storageStreamOutput = StorageOutput.apply(config.getConfig("Output"))

      storageStreamOutput.outputName shouldEqual None
      storageStreamOutput.format shouldEqual "delta"
      storageStreamOutput.path shouldEqual "output/fileStreamOutputConfigTest"
      storageStreamOutput.partitioningColumns shouldEqual Seq("upd_date", "version")
      Option(storageStreamOutput.processingTimeTrigger) shouldNot be(None)
      storageStreamOutput.processingTimeTrigger shouldEqual Trigger.ProcessingTime(Duration("60 seconds"))
      storageStreamOutput.timeout shouldEqual 86400000
      storageStreamOutput.mode shouldEqual "update"
      storageStreamOutput.options shouldEqual Map(
        "spark.sql.parquet.compression.codec" -> "snappy",
        "checkpointLocation"                  -> "maprfs://bktv001//test/checkpoint",
        "mergeSchema"                         -> "true"
      )
    }
  }

  "createQueryName" should {

    val uuidPattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

    "return a query name based on path" in {

      val directory = "testStreaming"
      val path      = "test" + File.separatorChar + directory

      val storageOutput = StorageOutput(path = path, format = "format", partitioningColumns = Seq(""), processingTimeTrigger = null, timeout = 0L, mode = "", outputName = None)

      val queryName = storageOutput.createQueryName()

      queryName should fullyMatch regex "^QN_" + directory + "_" + uuidPattern + "$"
    }

    "return a query name based on path and output name" in {

      val directory = "testStreaming"
      val path      = "test" + File.separatorChar + directory

      val storageOutput =
        StorageOutput(path = path, format = "format", partitioningColumns = Seq(""), processingTimeTrigger = null, timeout = 0L, mode = "", outputName = Some("myTestOutput"))

      val queryName = storageOutput.createQueryName()

      queryName should fullyMatch regex "^QN_myTestOutput_" + directory + "_" + uuidPattern + "$"
    }

    "return a query name based only on UUID" in {

      val storageOutput = StorageOutput(path = null, format = "format", partitioningColumns = Seq(""), processingTimeTrigger = null, timeout = 0L, mode = "", outputName = None)

      val queryName = storageOutput.createQueryName()

      queryName should fullyMatch regex "^" + uuidPattern + "$"

    }
  }
}
