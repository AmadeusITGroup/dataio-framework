package com.amadeus.dataio.pipes.elk.streaming

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.Duration

class ElkOutputTest extends AnyWordSpec with Matchers {

  "ElkOutput" should {
    "be initialized according to configuration" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Map(
            "Type"      -> "com.amadeus.dataio.pipes.elk.streaming.ElkOutput",
            "Name"      -> "my-test-elk",
            "Nodes"     -> "bktv001, bktv002.amadeus.net",
            "Ports"     -> "9200",
            "Index"     -> "test.index",
            "DateField" -> "docDate",
            "Mode"      -> "append",
            "Duration"  -> "6 hours",
            "Timeout"   -> "24",
            "Options" -> Map(
              "\"es.net.ssl.cert.allow.self.signed\"" -> true,
              "\"es.index.auto.create\""              -> true,
              "\"es.mapping.id\""                     -> "docId",
              "\"es.port\""                           -> "9200",
              "\"es.nodes\""                          -> "bktv001, bktv002.amadeus.net"
            )
          )
        )
      )

      val elkStreamingOutput = ElkOutput.apply(config.getConfig("Output"))

      elkStreamingOutput.outputName shouldEqual Some("my-test-elk")
      elkStreamingOutput.index shouldEqual "test.index"
      elkStreamingOutput.dateField shouldEqual "docDate"
      elkStreamingOutput.suffixDatePattern shouldEqual "yyyy.MM"
      elkStreamingOutput.mode shouldEqual "append"
      elkStreamingOutput.processingTimeTrigger shouldEqual Trigger.ProcessingTime(Duration("6 hours"))
      elkStreamingOutput.timeout shouldEqual 86400000
      elkStreamingOutput.options shouldEqual Map(
        "es.net.ssl.cert.allow.self.signed" -> "true",
        "es.index.auto.create"              -> "true",
        "es.mapping.id"                     -> "docId",
        "es.port"                           -> "9200",
        "es.nodes"                          -> "bktv001, bktv002.amadeus.net"
      )

    }

    "be initialized according to configuration without output name" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Map(
            "Type"      -> "com.amadeus.dataio.output.streaming.ElkOutput",
            "Nodes"     -> "bktv001, bktv002.amadeus.net",
            "Ports"     -> "9200",
            "Index"     -> "test.index",
            "DateField" -> "docDate",
            "Mode"      -> "append",
            "Duration"  -> "6 hours",
            "Timeout"   -> "24",
            "Options" -> Map(
              "\"es.net.ssl.cert.allow.self.signed\"" -> true,
              "\"es.index.auto.create\""              -> true,
              "\"es.mapping.id\""                     -> "docId",
              "\"es.port\""                           -> "9200",
              "\"es.nodes\""                          -> "bktv001, bktv002.amadeus.net"
            )
          )
        )
      )

      val elkStreamingOutput = ElkOutput.apply(config.getConfig("Output"))

      elkStreamingOutput.outputName shouldEqual None
      elkStreamingOutput.index shouldEqual "test.index"
      elkStreamingOutput.dateField shouldEqual "docDate"
      elkStreamingOutput.suffixDatePattern shouldEqual "yyyy.MM"
      elkStreamingOutput.mode shouldEqual "append"
      elkStreamingOutput.processingTimeTrigger shouldEqual Trigger.ProcessingTime(Duration("6 hours"))
      elkStreamingOutput.timeout shouldEqual 86400000
      elkStreamingOutput.options shouldEqual Map(
        "es.net.ssl.cert.allow.self.signed" -> "true",
        "es.index.auto.create"              -> "true",
        "es.mapping.id"                     -> "docId",
        "es.port"                           -> "9200",
        "es.nodes"                          -> "bktv001, bktv002.amadeus.net"
      )

    }

    "be initialized according to configuration with date suffix pattern" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Map(
            "Type"                -> "com.amadeus.dataio.pipes.elk.streaming.ElkOutput",
            "Name"                -> "my-test-elk",
            "Nodes"               -> "bktv001, bktv002.amadeus.net",
            "Ports"               -> "9200",
            "Index"               -> "test.index",
            "DateField"           -> "docDate",
            "SubIndexDatePattern" -> "yyyy.MM.dd",
            "Mode"                -> "append",
            "Duration"            -> "6 hours",
            "Timeout"             -> "24",
            "Options" -> Map(
              "\"es.net.ssl.cert.allow.self.signed\"" -> true,
              "\"es.index.auto.create\""              -> true,
              "\"es.mapping.id\""                     -> "docId",
              "\"es.port\""                           -> "9200",
              "\"es.nodes\""                          -> "bktv001, bktv002.amadeus.net"
            )
          )
        )
      )

      val elkStreamingOutput = ElkOutput.apply(config.getConfig("Output"))

      elkStreamingOutput.outputName shouldEqual Some("my-test-elk")
      elkStreamingOutput.index shouldEqual "test.index"
      elkStreamingOutput.dateField shouldEqual "docDate"
      elkStreamingOutput.suffixDatePattern shouldEqual "yyyy.MM.dd"
      elkStreamingOutput.mode shouldEqual "append"
      elkStreamingOutput.processingTimeTrigger shouldEqual Trigger.ProcessingTime(Duration("6 hours"))
      elkStreamingOutput.timeout shouldEqual 86400000
      elkStreamingOutput.options shouldEqual Map(
        "es.net.ssl.cert.allow.self.signed" -> "true",
        "es.index.auto.create"              -> "true",
        "es.mapping.id"                     -> "docId",
        "es.port"                           -> "9200",
        "es.nodes"                          -> "bktv001, bktv002.amadeus.net"
      )

    }
  }

  "createQueryName" should {

    val uuidPattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

    "return a query name based on index name" in {

      val elkOutput =
        ElkOutput(index = "test.index", processingTimeTrigger = null, timeout = 0L, mode = "", dateField = "docDate", suffixDatePattern = "yyyy.MM", outputName = None)

      val queryName = elkOutput.createQueryName()

      queryName should fullyMatch regex "^QN_test.index_" + uuidPattern + "$"

    }

    "return a query name based on output name" in {

      val elkOutput = ElkOutput(
        index = "test.index",
        processingTimeTrigger = null,
        timeout = 0L,
        mode = "",
        dateField = "docDate",
        suffixDatePattern = "yyyy.MM",
        outputName = Some("myTestOutput")
      )

      val queryName = elkOutput.createQueryName()

      queryName should fullyMatch regex "^QN_myTestOutput_test.index_" + uuidPattern + "$"
    }
  }
}
