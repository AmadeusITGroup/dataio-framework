package com.amadeus.dataio.pipes.elasticsearch.batch

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ElasticsearchOutputTest extends AnyWordSpec with Matchers {

  "ElasticsearchOutput" should {
    "be initialized according to configuration" in {

      val config = ConfigFactory.parseMap(
        Map(
          "output" -> Map(
            "type"       -> "com.amadeus.dataio.pipes.elasticsearch.batch.ElasticsearchOutput",
            "name"       -> "my-test-elasticsearch",
            "index"      -> "test.index",
            "date_field" -> "docDate",
            "mode"       -> "append",
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

      val elasticsearchOutput = ElasticsearchOutput.apply(config.getConfig("output"))

      elasticsearchOutput.name shouldEqual "my-test-elasticsearch"
      elasticsearchOutput.index shouldEqual "test.index"
      elasticsearchOutput.dateField shouldEqual "docDate"
      elasticsearchOutput.suffixDatePattern shouldEqual "yyyy.MM"
      elasticsearchOutput.mode shouldEqual "append"
      elasticsearchOutput.options shouldEqual Map(
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
          "output" -> Map(
            "type"                   -> "com.amadeus.dataio.pipes.elasticsearch.batch.ElasticsearchOutput",
            "name"                   -> "my-test-elasticsearch",
            "index"                  -> "test.index",
            "date_field"             -> "docDate",
            "sub_index_date_pattern" -> "yyyy.MM.dd",
            "mode"                   -> "append",
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

      val elasticsearchOutput = ElasticsearchOutput.apply(config.getConfig("output"))

      elasticsearchOutput.name shouldEqual "my-test-elasticsearch"
      elasticsearchOutput.index shouldEqual "test.index"
      elasticsearchOutput.dateField shouldEqual "docDate"
      elasticsearchOutput.suffixDatePattern shouldEqual "yyyy.MM.dd"
      elasticsearchOutput.mode shouldEqual "append"
      elasticsearchOutput.options shouldEqual Map(
        "es.net.ssl.cert.allow.self.signed" -> "true",
        "es.index.auto.create"              -> "true",
        "es.mapping.id"                     -> "docId",
        "es.port"                           -> "9200",
        "es.nodes"                          -> "bktv001, bktv002.amadeus.net"
      )

    }

    "throw an exception given a missing name" in {
      val config = ConfigFactory.parseMap(
        Map(
          "output" -> Map(
            "type"       -> "com.amadeus.dataio.pipes.elasticsearch.batch.ElasticsearchOutput",
            "index"      -> "test.index",
            "date_field" -> "docDate",
            "mode"       -> "append",
            "options" -> Map(
              "\"es.port\""  -> "9200",
              "\"es.nodes\"" -> "bktv001"
            )
          )
        )
      )

      intercept[Exception] {
        ElasticsearchOutput.apply(config.getConfig("output"))
      }
    }

    "throw an exception given a missing mode" in {
      val config = ConfigFactory.parseMap(
        Map(
          "output" -> Map(
            "type"       -> "com.amadeus.dataio.pipes.elasticsearch.batch.ElasticsearchOutput",
            "name"       -> "my-test-elasticsearch",
            "index"      -> "test.index",
            "date_field" -> "docDate",
            "options" -> Map(
              "\"es.port\""  -> "9200",
              "\"es.nodes\"" -> "bktv001"
            )
          )
        )
      )

      intercept[Exception] {
        ElasticsearchOutput.apply(config.getConfig("output"))
      }
    }
  }
}
