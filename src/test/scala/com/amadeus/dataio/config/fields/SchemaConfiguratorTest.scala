package com.amadeus.dataio.config.fields

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class SchemaConfiguratorTest extends AnyWordSpec with Matchers {

  "getSchema" should {
    "return the schema fully qualified name" when {

      "given schema" in {
        val config = ConfigFactory.parseMap(
          Map("Schema" -> "com.example.Test")
        )

        getSchema(config) shouldEqual "com.example.Test"
      }
    }

    "return none" when {

      "given no schema" in {
        val config = ConfigFactory.parseMap(Map[String, String]())

        Try(getSchema(config)).toOption shouldEqual None
      }
    }
  }
}
