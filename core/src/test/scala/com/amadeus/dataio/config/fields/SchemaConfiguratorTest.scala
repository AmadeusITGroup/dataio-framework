package com.amadeus.dataio.config.fields

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SchemaConfiguratorTest extends AnyFlatSpec with Matchers {

  behavior of "getSchema"

  it should "return schema name when it exists in config" in {
    val configStr =
      """
       schema = "com.example.MySchema"
     """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getSchema
    result should be(Some("com.example.MySchema"))
  }

  it should "return None when schema doesn't exist in config" in {
    val configStr =
      """
       some_other_config = "value"
     """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getSchema
    result should be(None)
  }
}
