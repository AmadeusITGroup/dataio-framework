package com.amadeus.dataio.config.fields

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DropDuplicatesConfiguratorTest extends AnyFlatSpec with Matchers {
  behavior of "getDropDuplicatesColumns"
  it should "parse comma-separated string values" in {
    val configStr               = """
      drop_duplicates = "id, name, date"
    """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getDropDuplicatesColumns
    result should be(List("id", "name", "date"))
  }

  it should "parse string list values" in {
    val configStr               = """
      drop_duplicates = ["id", "name", "date"]
    """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getDropDuplicatesColumns
    result should be(List("id", "name", "date"))
  }

  it should "return empty sequence when drop_duplicates doesn't exist" in {
    val configStr               = """
      some_other_config = "value"
    """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getDropDuplicatesColumns
    result should be(Seq())
  }

  behavior of "getDropDuplicatesActive"
  it should "return true when drop_duplicates exists" in {
    val configStr               = """
      drop_duplicates = "id, name, date"
    """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getDropDuplicatesActive
    result should be(true)
  }

  it should "return false when drop_duplicates doesn't exist" in {
    val configStr               = """
      some_other_config = "value"
    """
    implicit val config: Config = ConfigFactory.parseString(configStr)

    val result = getDropDuplicatesActive
    result should be(false)
  }
}
