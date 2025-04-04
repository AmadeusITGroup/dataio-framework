package com.amadeus.dataio.pipes.elk.batch

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ElkOutputTest extends AnyWordSpec with Matchers {

  "ElkOutput" should {
    "be initialized according to configuration" in {

      val config = ConfigFactory.parseMap(
        Map(
          "Output" -> Map(
            "Type"      -> "com.amadeus.dataio.pipes.elk.batch.ElkOutput",
            "Name"      -> "my-test-elk",
            "Nodes"     -> "bktv001, bktv002.amadeus.net",
            "Ports"     -> "9200",
            "Index"     -> "test.index",
            "DateField" -> "docDate",
            "Mode"      -> "append",
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

      elkStreamingOutput.index shouldEqual "test.index"
      elkStreamingOutput.dateField shouldEqual "docDate"
      elkStreamingOutput.suffixDatePattern shouldEqual "yyyy.MM"
      elkStreamingOutput.mode shouldEqual "append"
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
            "Type"                -> "com.amadeus.dataio.pipes.elk.batch.ElkOutput",
            "Name"                -> "my-test-elk",
            "Nodes"               -> "bktv001, bktv002.amadeus.net",
            "Ports"               -> "9200",
            "Index"               -> "test.index",
            "DateField"           -> "docDate",
            "SubIndexDatePattern" -> "yyyy.MM.dd",
            "Mode"                -> "append",
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

      elkStreamingOutput.index shouldEqual "test.index"
      elkStreamingOutput.dateField shouldEqual "docDate"
      elkStreamingOutput.suffixDatePattern shouldEqual "yyyy.MM.dd"
      elkStreamingOutput.mode shouldEqual "append"
      elkStreamingOutput.options shouldEqual Map(
        "es.net.ssl.cert.allow.self.signed" -> "true",
        "es.index.auto.create"              -> "true",
        "es.mapping.id"                     -> "docId",
        "es.port"                           -> "9200",
        "es.nodes"                          -> "bktv001, bktv002.amadeus.net"
      )

    }
  }
}
