package com.amadeus.dataio.config.fields

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OptionsConfiguratorTest extends AnyFlatSpec with Matchers {
  behavior of "getOptions"

  it should "parse options from config" in {
    val configStr =
      """
        options {
          "header" = "true"
          "delimiter" = ","
        }
      """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getOptions
    result should be(Map("header" -> "true", "delimiter" -> ","))
  }

  it should "return empty map when options don't exist" in {
    val configStr =
      """
        some_other_config = "value"
      """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getOptions
    result should be(Map())
  }

  it should "throw exception when format is invalid" in {
    val configStr =
      """
    options {
      other_node {
        field = "value"
      }
    }
  """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    an[IllegalArgumentException] should be thrownBy {
      getOptions
    }
  }
}
