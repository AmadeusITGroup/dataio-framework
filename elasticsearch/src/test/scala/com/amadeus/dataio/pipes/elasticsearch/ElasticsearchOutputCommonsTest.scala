package com.amadeus.dataio.pipes.elasticsearch

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ElasticsearchOutputCommonsTest extends AnyWordSpec with Matchers {

  case class TestElasticsearchOutput(
      index: String = "testIndex",
      dateField: String = "documentDate",
      suffixDatePattern: String = "yyyy.MM.dd",
      options: Map[String, String] = Map.empty
  ) extends ElasticsearchOutputCommons

  "computeFullIndexName" should {

    "return the index with date suffix" in {
      val result = TestElasticsearchOutput().computeFullIndexName()

      result shouldEqual "testIndex.{documentDate|yyyy.MM.dd}"
    }
  }

  "checkNodesIsDefined" should {
    "not throw an exception given options map with nodes" in {
      try {
        ElasticsearchOutputCommons.checkNodesIsDefined(Map("es.port" -> "9200", "es.nodes" -> "localhost1, localhost2"))
      } catch {
        case _: IllegalArgumentException => fail("no exceptions should have been thrown")
      }
    }

    "throw an exception given options map with empty nodes" in {
      intercept[IllegalArgumentException] {
        ElasticsearchOutputCommons.checkNodesIsDefined(Map("es.port" -> "9200", "es.nodes" -> ""))
      }

    }

    "return false given options map without nodes" in {
      intercept[IllegalArgumentException] {
        ElasticsearchOutputCommons.checkNodesIsDefined(Map("es.port" -> "9200"))
      }
    }
  }

  "checkPortIsDefined" should {
    "not throw an exception given options map with port" in {
      try {
        ElasticsearchOutputCommons.checkPortIsDefined(Map("es.port" -> "9200", "es.nodes" -> "localhost1, localhost2"))
      } catch {
        case _: IllegalArgumentException => fail("no exceptions should have been thrown")
      }
    }

    "throw an exception given options map with empty port" in {
      intercept[IllegalArgumentException] {
        ElasticsearchOutputCommons.checkPortIsDefined(Map("es.port" -> "", "es.nodes" -> "localhost1, localhost2"))
      }

    }

    "return false given options map without port" in {
      intercept[IllegalArgumentException] {
        ElasticsearchOutputCommons.checkPortIsDefined(Map("es.nodes" -> "localhost1, localhost2"))
      }
    }
  }
}
