package com.amadeus.dataio.config

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ConfigNodeCollectionTest extends AnyWordSpec with Matchers {

  "A ConfigNodeCollection" should {
    "contain a sequence of ConfigNode objects in its nodes field" in {
      ConfigNodeCollection(Seq()).nodes.isInstanceOf[Seq[ConfigNode]] shouldBe true
    }
  }

  "ConfigNodeCollection.apply" should {
    "return an empty ConfigNodeCollection" when {
      "given a Config without the given nodeName" in {
        val config = ConfigFactory.parseMap(
          Map("ActualNodeName" -> Map())
        )

        val result = ConfigNodeCollection("MissingNode", config)
        result.nodes shouldBe empty
      }

      "given a Config with an empty array as nodeName" in {
        val config = ConfigFactory.parseMap(
          Map("NodeName" -> Seq())
        )

        val result = ConfigNodeCollection("NodeName", config)
        result.nodes shouldBe empty
      }
    }

    "return a ConfigNodeCollection with a single ConfigNode" when {
      "given an object as node" in {
        val config = ConfigFactory.parseMap(
          Map("NodeName" -> Map("Type" -> "com.Entity"))
        )

        val result = ConfigNodeCollection("NodeName", config)
        result.nodes.size shouldBe 1
      }

      "given an list with a single node" in {
        val config = ConfigFactory.parseMap(
          Map("NodeName" -> Seq(Map("Type" -> "com.Entity")))
        )

        val result = ConfigNodeCollection("NodeName", config)
        result.nodes.size shouldBe 1
      }
    }

    "return a ConfigNodeCollection with a two ConfigNode" when {
      "given an list with two nodes" in {
        val config = ConfigFactory.parseMap(
          Map("NodeName" -> Seq(Map("Type" -> "com.Entity"), Map("Type" -> "com.Entity")))
        )

        val result = ConfigNodeCollection("NodeName", config)
        result.nodes.size shouldBe 2
      }
    }

    "throw an Exception" when {
      "given a node that triggers an Exception during instantiation" in {
        val config = ConfigFactory.parseMap(
          Map("NodeName" -> Map())
        )

        intercept[Exception] {
          ConfigNodeCollection("NodeName", config)
        }
      }

      "given a node that is neither a List nor an Object" in {
        val config = ConfigFactory.parseString("NodeName = 5")

        intercept[Exception] {
          ConfigNodeCollection("NodeName", config)
        }
      }
    }
  }
}
