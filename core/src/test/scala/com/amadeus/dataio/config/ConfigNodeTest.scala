package com.amadeus.dataio.config

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConfigNodeTest extends AnyWordSpec with Matchers {
  "ConfigNode.apply" should {
    "return a ConfigNode(name = my-entity, typeName = com.Entity)" when {
      val expectedTypeName = "com.Entity"
      val expectedName     = "my-entity"
      "given Name = my-entity, Type = com.Entity" in {
        val config = ConfigFactory.parseMap(
          Map("Name" -> expectedName, "Type" -> expectedTypeName)
        )

        val result = ConfigNode(config)

        result.typeName shouldBe expectedTypeName
        result.name shouldBe expectedName
      }
    }

    "return a ConfigNode(name = com.Entity-<UUID>, typeName = com.Entity)" when {
      val expectedTypeName    = "com.Entity"
      val expectedNamePattern = (expectedTypeName + "-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$").r

      "given Type = com.Entity" in {
        val config = ConfigFactory.parseMap(
          Map("Type" -> expectedTypeName)
        )

        val result = ConfigNode(config)

        result.typeName shouldBe expectedTypeName
        (expectedNamePattern findAllIn result.name).size shouldBe 1
      }
    }

    "return a ConfigNode(name = my-entity, typeName = com.Entity, config = { Field1 = 5 }" when {
      "given Name = my-entity, Type = com.Entity, Field1 = 5" in {
        val config = ConfigFactory.parseMap(
          Map("Name" -> "my-entity", "Type" -> "com.Entity", "Field1" -> 5)
        )

        val result = ConfigNode(config)

        result.name shouldBe "my-entity"
        result.typeName shouldBe "com.Entity"
        result.config.getInt("Field1") shouldBe 5
        result.config.withoutPath("Field1").withoutPath("Name").isEmpty shouldBe true
      }
    }

    "throw an Exception" when {
      "given a Config without Type" in {
        val config = ConfigFactory.parseMap(
          Map("Name" -> "Error")
        )

        val ex = intercept[Exception] {
          ConfigNode(config)
        }
      }
    }
  }
}
