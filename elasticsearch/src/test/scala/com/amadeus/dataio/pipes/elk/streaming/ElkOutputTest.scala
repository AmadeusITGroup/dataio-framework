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
          "output" -> Map(
            "type"      -> "com.amadeus.dataio.output.streaming.ElkOutput",
            "name"      -> "my-test-elk",
            "nodes"     -> "bktv001, bktv002.amadeus.net",
            "ports"     -> "9200",
            "index"     -> "test.index",
            "dateField" -> "docDate",
            "mode"      -> "append",
            "duration"  -> "6 hours",
            "timeout"   -> "24 hours",
            "options" -> Map(
              "\"es.net.ssl.cert.allow.self.signed\"" -> true,
              "\"es.index.auto.create\""              -> true,
              "\"es.mapping.id\""                     -> "docId",
              "\"es.port\""                           -> "9200",
              "\"es.nodes\""                          -> "bktv001, bktv002.amadeus.net"
            )
          )
        )
      )

      val elkStreamingOutput = ElkOutput.apply(config.getConfig("output"))

      elkStreamingOutput.name shouldEqual "my-test-elk"
      elkStreamingOutput.index shouldEqual "test.index"
      elkStreamingOutput.dateField shouldEqual "docDate"
      elkStreamingOutput.suffixDatePattern shouldEqual "yyyy.MM"
      elkStreamingOutput.mode shouldEqual "append"
      elkStreamingOutput.trigger shouldEqual Some(Trigger.ProcessingTime(Duration("6 hours")))
      elkStreamingOutput.timeout shouldEqual 86400000
      elkStreamingOutput.options shouldEqual Map(
        "es.net.ssl.cert.allow.self.signed" -> "true",
        "es.index.auto.create"              -> "true",
        "es.mapping.id"                     -> "docId",
        "es.port"                           -> "9200",
        "es.nodes"                          -> "bktv001, bktv002.amadeus.net"
      )

    }

    "raise exception given missing output name" in {

      val config = ConfigFactory.parseMap(
        Map(
          "output" -> Map(
            "type"      -> "com.amadeus.dataio.output.streaming.ElkOutput",
            "nodes"     -> "bktv001, bktv002.amadeus.net",
            "ports"     -> "9200",
            "index"     -> "test.index",
            "dateField" -> "docDate",
            "mode"      -> "append",
            "trigger"   -> "Continuous",
            "duration"  -> "6 hours",
            "timeout"   -> "24 hours",
            "options" -> Map(
              "\"es.net.ssl.cert.allow.self.signed\"" -> true,
              "\"es.index.auto.create\""              -> true,
              "\"es.mapping.id\""                     -> "docId",
              "\"es.port\""                           -> "9200",
              "\"es.nodes\""                          -> "bktv001, bktv002.amadeus.net"
            )
          )
        )
      )

      intercept[Exception] {
        val elkStreamingOutput = ElkOutput.apply(config.getConfig("output"))

        fail("Expected an exception to be thrown due to missing required `name` field in configuration.")
      }

    }

    "be initialized according to configuration with date suffix pattern" in {

      val config = ConfigFactory.parseMap(
        Map(
          "output" -> Map(
            "type"                -> "com.amadeus.dataio.output.streaming.ElkOutput",
            "name"                -> "my-test-elk",
            "nodes"               -> "bktv001, bktv002.amadeus.net",
            "ports"               -> "9200",
            "index"               -> "test.index",
            "dateField"           -> "docDate",
            "subIndexDatePattern" -> "yyyy.MM.dd",
            "mode"                -> "append",
            "trigger"             -> "AvailableNow",
            "timeout"             -> "24 hours",
            "options" -> Map(
              "\"es.net.ssl.cert.allow.self.signed\"" -> true,
              "\"es.index.auto.create\""              -> true,
              "\"es.mapping.id\""                     -> "docId",
              "\"es.port\""                           -> "9200",
              "\"es.nodes\""                          -> "bktv001, bktv002.amadeus.net"
            )
          )
        )
      )

      val elkStreamingOutput = ElkOutput.apply(config.getConfig("output"))

      elkStreamingOutput.name shouldEqual "my-test-elk"
      elkStreamingOutput.index shouldEqual "test.index"
      elkStreamingOutput.dateField shouldEqual "docDate"
      elkStreamingOutput.suffixDatePattern shouldEqual "yyyy.MM.dd"
      elkStreamingOutput.mode shouldEqual "append"
      elkStreamingOutput.trigger shouldEqual Some(Trigger.AvailableNow())
      elkStreamingOutput.timeout shouldEqual 86400000
      elkStreamingOutput.options shouldEqual Map(
        "es.net.ssl.cert.allow.self.signed" -> "true",
        "es.index.auto.create"              -> "true",
        "es.mapping.id"                     -> "docId",
        "es.port"                           -> "9200",
        "es.nodes"                          -> "bktv001, bktv002.amadeus.net"
      )

    }

    "be initialized according to configuration without trigger" in {

      val config = ConfigFactory.parseMap(
        Map(
          "output" -> Map(
            "type"                -> "com.amadeus.dataio.output.streaming.ElkOutput",
            "name"                -> "my-test-elk",
            "nodes"               -> "bktv001, bktv002.amadeus.net",
            "ports"               -> "9200",
            "index"               -> "test.index",
            "dateField"           -> "docDate",
            "subIndexDatePattern" -> "yyyy.MM.dd",
            "mode"                -> "append",
            "timeout"             -> "24 hours",
            "options" -> Map(
              "\"es.net.ssl.cert.allow.self.signed\"" -> true,
              "\"es.index.auto.create\""              -> true,
              "\"es.mapping.id\""                     -> "docId",
              "\"es.port\""                           -> "9200",
              "\"es.nodes\""                          -> "bktv001, bktv002.amadeus.net"
            )
          )
        )
      )

      val elkStreamingOutput = ElkOutput.apply(config.getConfig("output"))

      elkStreamingOutput.name shouldEqual "my-test-elk"
      elkStreamingOutput.index shouldEqual "test.index"
      elkStreamingOutput.dateField shouldEqual "docDate"
      elkStreamingOutput.suffixDatePattern shouldEqual "yyyy.MM.dd"
      elkStreamingOutput.mode shouldEqual "append"
      elkStreamingOutput.trigger shouldEqual None
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
        ElkOutput(
          index = "test.index",
          trigger = None,
          timeout = 0L,
          mode = "",
          dateField = "docDate",
          suffixDatePattern = "yyyy.MM",
          name = ""
        )

      val queryName = elkOutput.createQueryName()

      queryName should fullyMatch regex "^QN_test.index_" + uuidPattern + "$"

    }
  }
}
