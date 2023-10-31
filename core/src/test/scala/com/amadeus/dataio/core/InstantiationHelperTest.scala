package com.amadeus.dataio.core

import com.amadeus.dataio.config.ConfigNode
import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class InstantiationHelperTest extends AnyWordSpec with Matchers {

  "instantiateWithCompanionObject" when {
    "instantiating a class that exists with the proper config" should {
      "succeed" in {
        val config: Config = ConfigFactory.parseMap(
          Map("Type" -> "com.amadeus.dataio.core.MockEntity", "Name" -> "test", "Field1" -> "value1")
        )
        val configNode: ConfigNode = ConfigNode(config)
        val entity                 = InstantiationHelper.instantiateWithCompanionObject[AnyRef](configNode)

        entity.getClass.getName shouldBe "com.amadeus.dataio.core.MockEntity"
      }
    }

    "instantiating a class which throws an exception" should {
      "throw the same exception" in {
        val config: Config = ConfigFactory.parseMap(
          Map("Type" -> "com.amadeus.dataio.core.MockEntity", "Name" -> "test")
        )
        val configNode: ConfigNode = ConfigNode(config)

        val thrownException = intercept[Exception] {
          InstantiationHelper.instantiateWithCompanionObject[AnyRef](configNode)
        }

        thrownException shouldBe MockEntity.missingFieldException
      }
    }

    "instantiating a class that does not exist" should {
      "throw a ClassNotFoundException with the name of the class" in {
        val config: Config = ConfigFactory.parseMap(
          Map("Type" -> "com.amadeus.dataio.core.UndefinedEntity", "Name" -> "test")
        )
        val configNode: ConfigNode = ConfigNode(config)

        val thrownException = intercept[ClassNotFoundException] {
          InstantiationHelper.instantiateWithCompanionObject[AnyRef](configNode)
        }

        thrownException.getMessage shouldBe "com.amadeus.dataio.core.UndefinedEntity"
      }
    }

    "instantiating based on an entity which does not have a companion object" should {
      "throw a ClassNotFoundException with information about the missing companion object" in {
        val config: Config = ConfigFactory.parseMap(
          Map("Type" -> "com.amadeus.dataio.core.DummyClassWithoutCompanionObject", "Name" -> "test")
        )
        val configNode: ConfigNode = ConfigNode(config)

        val thrownException = intercept[ClassNotFoundException] {
          InstantiationHelper.instantiateWithCompanionObject[AnyRef](configNode)
        }

        thrownException.getMessage shouldBe s"Cannot find the companion object of com.amadeus.dataio.core.DummyClassWithoutCompanionObject. This usually happens if a class without a companion object was provided as Type in the configuration."
      }
    }

    "instantiating without an apply method" should {
      "throw an Exception with information about the missing apply method" in {
        val config: Config = ConfigFactory.parseMap(
          Map("Type" -> "com.amadeus.dataio.core.DummyClassWithoutApplyMethod", "Name" -> "test")
        )
        val configNode: ConfigNode = ConfigNode(config)

        val thrownException = intercept[Exception] {
          InstantiationHelper.instantiateWithCompanionObject[AnyRef](configNode)
        }

        thrownException.getMessage shouldBe s"Cannot find the apply method to instantiate com.amadeus.dataio.core.DummyClassWithoutApplyMethod."
      }
    }

    "instantiating a distributor with a companion object missing the propre signature for its apply method" should {
      "throw an exception saying so" in {
        val config: Config = ConfigFactory.parseMap(
          Map("Type" -> "com.amadeus.dataio.core.MockEntityWithInvalidCompanionObject", "Name" -> "test")
        )
        val configNode: ConfigNode = ConfigNode(config)

        val thrownException = intercept[Exception] {
          InstantiationHelper.instantiateWithCompanionObject[AnyRef](configNode)
        }

        thrownException.getMessage shouldBe s"com.amadeus.dataio.core.MockEntityWithInvalidCompanionObject is not a proper Type. This usually happens if the companion object does not have an apply method with a valid signature."
      }
    }

    "instantiating based on an entity which does not extend the given parent type" should {
      "throw a ClassCastException" in {
        val config: Config = ConfigFactory.parseMap(
          Map("Type" -> "com.amadeus.dataio.core.DummyCaseClass", "Name" -> "test")
        )
        val configNode: ConfigNode = ConfigNode(config)

        intercept[ClassCastException] {
          InstantiationHelper.instantiateWithCompanionObject[MockEntity](configNode)
        }
      }
    }
  }

  "instantiateWithEmptyConstructor" when {
    "given a class that exists with an empty constructor" should {
      "succeed" in {
        InstantiationHelper.instantiateWithEmptyConstructor[AnyRef]("com.amadeus.dataio.core.DummyClassWithoutCompanionObject")
      }
    }

    "given a class which does not exist" should {
      "throw a ClassNotFoundException with the name of the class" in {
        val throwException = intercept[ClassNotFoundException] {
          InstantiationHelper.instantiateWithEmptyConstructor[AnyRef]("com.amadeus.dataio.core.UndefinedClass")
        }

        throwException.getMessage shouldBe "com.amadeus.dataio.core.UndefinedClass"
      }
    }

    "given a class which does not extend the given parent type" should {
      "throw a ClassCastException" in {
        intercept[ClassCastException] {
          InstantiationHelper.instantiateWithEmptyConstructor[MockEntity]("com.amadeus.dataio.core.DummyClassWithoutCompanionObject")
        }
      }
    }
  }
}

private case class MockEntityWithInvalidCompanionObject(field1: String)

private case class DummyCaseClass()

private class DummyClassWithoutCompanionObject

private class DummyClassWithoutApplyMethod
private object DummyClassWithoutApplyMethod
