package com.amadeus.dataio.core.handler

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.amadeus.dataio.core.ConfigurableEntity
import com.typesafe.config.{Config, ConfigFactory}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class HandlerTest extends AnyWordSpec with Matchers with MockFactory {
  val e1: ConfigurableEntity = mock[ConfigurableEntity]
  val e2: ConfigurableEntity = mock[ConfigurableEntity]
  val e3: ConfigurableEntity = mock[ConfigurableEntity]

  "A Handler" when {
    "empty" should {
      val handler = new Handler[ConfigurableEntity] {}

      "return an empty sequence when getAll is invoked" in {
        val result = handler.getAll
        result.isEmpty shouldBe true
      }

      "throw an Exception when getOne is invoked" in {
        intercept[Exception] {
          handler.getOne("mykey")
        }
      }

      "throw an Exception when getConfig is invoked" in {
        intercept[Exception] {
          handler.getConfig("mykey")
        }
      }
    }

    "not empty" should {
      val handler = new Handler[ConfigurableEntity] {}
      handler.add("first", e1)
      handler.add("second", e2)
      handler.add("third", e3)

      "return the entities in the original order when getAll is invoked" in {
        val result = handler.getAll
        result shouldBe Seq(e1, e2, e3)
      }

      "return the entity when getOne is invoked with an existing name" in {
        val result = handler.getOne[ConfigurableEntity]("third")
        result shouldBe e3
      }

      "throw an exception when getOne is invoked with a non-existing name" in {
        intercept[Exception] {
          handler.getOne[ConfigurableEntity]("notfound")
        }
      }

      "return the entity's config object when getConfig is invoked with an existing name" in {
        val entity = new ConfigurableEntity {
          override val config: Config = ConfigFactory.parseMap(Map("EntityField" -> "EntityValue"))
        }
        handler.add("fourth", entity)
        val result = handler.getConfig("fourth")

        result shouldBe entity.config
      }

      "throw an exception when getConfig is invoked with a non-existing name" in {
        intercept[Exception] {
          handler.getConfig("notfound")
        }
      }
    }

    "adding entities" should {
      val handler = new Handler[ConfigurableEntity] {}
      handler.add("first", e1)

      "succeed when given a proper name" in {
        handler.add("second", e2)
      }

      "throw an Exception when given an empty string" in {
        intercept[Exception] {
          handler.add("", e3)
        }
      }

      "throw an Exception when given a name which already exists" in {
        intercept[Exception] {
          handler.add("first", e1)
        }
      }
    }
  }
}
