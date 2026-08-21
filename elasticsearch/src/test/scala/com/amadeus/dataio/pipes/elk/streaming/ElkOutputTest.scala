package com.amadeus.dataio.pipes.elk.streaming

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.Duration

class ElkOutputTest extends AnyWordSpec with Matchers {

  val options: Map[String, Any] = Map(
    "\"es.net.ssl.cert.allow.self.signed\"" -> true,
    "\"es.index.auto.create\""              -> true,
    "\"es.mapping.id\""                     -> "docId",
    "\"es.port\""                           -> "9200",
    "\"es.nodes\""                          -> "bktv001, bktv002.amadeus.net"
  )

  val expectedOptions: Map[String, String] = Map(
    "es.net.ssl.cert.allow.self.signed" -> "true",
    "es.index.auto.create"              -> "true",
    "es.mapping.id"                     -> "docId",
    "es.port"                           -> "9200",
    "es.nodes"                          -> "bktv001, bktv002.amadeus.net"
  )

  "ElkOutput" should {
    "be initialized according to configuration" in {

      val config = ConfigFactory.parseMap(
        Map(
          "output" -> Map(
            "type"       -> "com.amadeus.dataio.pipes.elk.streaming.ElkOutput",
            "name"       -> "my-test-elk",
            "index"      -> "test.index",
            "date_field" -> "docDate",
            "mode"       -> "append",
            "duration"   -> "6 hours",
            "timeout"    -> "24 hours",
            "options"    -> options
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
      elkStreamingOutput.timeout shouldEqual Some(86400000)
      elkStreamingOutput.options shouldEqual expectedOptions
    }

    "be initialized according to configuration with a continuous trigger" in {

      val config = ConfigFactory.parseMap(
        Map(
          "output" -> Map(
            "type"       -> "com.amadeus.dataio.pipes.elk.streaming.ElkOutput",
            "name"       -> "my-test-elk",
            "index"      -> "test.index",
            "date_field" -> "docDate",
            "mode"       -> "append",
            "trigger"    -> "Continuous",
            "duration"   -> "6 hours",
            "timeout"    -> "24 hours",
            "options"    -> options
          )
        )
      )

      val elkStreamingOutput = ElkOutput.apply(config.getConfig("output"))

      elkStreamingOutput.name shouldEqual "my-test-elk"
      elkStreamingOutput.trigger shouldEqual Some(Trigger.Continuous(Duration("6 hours")))
      elkStreamingOutput.timeout shouldEqual Some(86400000)
      elkStreamingOutput.options shouldEqual expectedOptions
    }

    "be initialized according to configuration with date suffix pattern" in {

      val config = ConfigFactory.parseMap(
        Map(
          "output" -> Map(
            "type"                   -> "com.amadeus.dataio.pipes.elk.streaming.ElkOutput",
            "name"                   -> "my-test-elk",
            "index"                  -> "test.index",
            "date_field"             -> "docDate",
            "sub_index_date_pattern" -> "yyyy.MM.dd",
            "mode"                   -> "append",
            "trigger"                -> "AvailableNow",
            "timeout"                -> "24 hours",
            "options"                -> options
          )
        )
      )

      val elkStreamingOutput = ElkOutput.apply(config.getConfig("output"))

      elkStreamingOutput.suffixDatePattern shouldEqual "yyyy.MM.dd"
      elkStreamingOutput.trigger shouldEqual Some(Trigger.AvailableNow())
      elkStreamingOutput.timeout shouldEqual Some(86400000)
    }

    "be initialized according to configuration without trigger nor timeout" in {

      val config = ConfigFactory.parseMap(
        Map(
          "output" -> Map(
            "type"       -> "com.amadeus.dataio.pipes.elk.streaming.ElkOutput",
            "name"       -> "my-test-elk",
            "index"      -> "test.index",
            "date_field" -> "docDate",
            "mode"       -> "append",
            "options"    -> options
          )
        )
      )

      val elkStreamingOutput = ElkOutput.apply(config.getConfig("output"))

      elkStreamingOutput.trigger shouldEqual None
      elkStreamingOutput.timeout shouldEqual None
    }

    "throw an exception given a missing name" in {
      val config = ConfigFactory.parseMap(
        Map(
          "output" -> Map(
            "type"       -> "com.amadeus.dataio.pipes.elk.streaming.ElkOutput",
            "index"      -> "test.index",
            "date_field" -> "docDate",
            "mode"       -> "append",
            "options"    -> options
          )
        )
      )

      intercept[Exception] {
        ElkOutput.apply(config.getConfig("output"))
      }
    }
  }

  "createQueryName" should {

    val uuidPattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

    "return a query name based on the output name and index" in {

      val elkOutput = ElkOutput(
        name = "myTestOutput",
        index = "test.index",
        trigger = None,
        timeout = None,
        mode = "",
        dateField = "docDate",
        suffixDatePattern = "yyyy.MM"
      )

      val queryName = elkOutput.createQueryName()

      queryName should fullyMatch regex "^QN_myTestOutput_test.index_" + uuidPattern + "$"
    }
  }
}
