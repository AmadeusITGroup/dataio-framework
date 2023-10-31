package com.amadeus.dataio.config.fields

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class OptionsConfiguratorTest extends AnyWordSpec with Matchers {

  "getOptions" should {
    "return Map(key1 -> val1, key2 -> val2)" when {
      "given Options = { key1 = val1, key2 = val2 }" in {
        val config = ConfigFactory.parseMap(
          Map("Options" -> Map("key1" -> "val1", "key2" -> "val2"))
        )

        getOptions(config) should contain theSameElementsAs Map("key1" -> "val1", "key2" -> "val2")
      }
    }

    "throw a ConfigException" when {
      "given a Config without Options" in {
        intercept[ConfigException] {
          val config = ConfigFactory.empty()

          getOptions(config)
        }
      }
    }
  }
}
