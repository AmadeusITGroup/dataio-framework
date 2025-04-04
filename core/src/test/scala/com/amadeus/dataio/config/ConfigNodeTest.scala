package com.amadeus.dataio.config

import com.amadeus.dataio.testutils.ConfigCreator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class ConfigNodeTest extends AnyFlatSpec with ConfigCreator with Matchers {
  "ConfigNode" should "create a ConfigNode with type and name from config" in {
    val configStr =
      """
        type = "processor"
        name = "dataFilter"
        param1 = "value1"
        param2 = 42
      """

    val config = createConfig(configStr)
    val node   = ConfigNode(config)

    node.typeName shouldBe "processor"
    node.name shouldBe "dataFilter"
    node.config.getString("param1") shouldBe "value1"
    node.config.getInt("param2") shouldBe 42

    // Verify that type field is removed from the resulting config
    node.config.hasPath("type") shouldBe false

    // Name is kept in the config as it might be needed for reference
    node.config.hasPath("name") shouldBe true
  }

  it should "generate a name when name is not provided in config" in {
    val configStr =
      """
        type = "transformer"
        param1 = "value1"
      """

    val config = createConfig(configStr)
    val node   = ConfigNode(config)

    node.typeName shouldBe "transformer"

    // Check that the name follows the expected pattern: typeName-UUID
    val nameParts = node.name.split("-")
    nameParts(0) shouldBe "transformer"

    // The remainder should be a valid UUID
    noException should be thrownBy UUID.fromString(nameParts.tail.mkString("-"))

    // Config should still contain original parameters
    node.config.getString("param1") shouldBe "value1"
  }

  it should "throw an exception when type field is missing" in {
    val configStr =
      """
        name = "processor"
        param1 = "value1"
      """

    val config = createConfig(configStr)

    val exception = intercept[Exception] {
      ConfigNode(config)
    }

    exception.getMessage shouldBe "Configuration missing a `type` field."
  }

  it should "preserve all other config fields except 'type'" in {
    val configStr =
      """
        type = "loader"
        name = "csvLoader"
        format = "csv"
        path = "/data/input"
        options {
          header = true
          delimiter = ","
          quote = "\""
        }
      """

    val config = createConfig(configStr)
    val node   = ConfigNode(config)

    // Check main fields
    node.typeName shouldBe "loader"
    node.name shouldBe "csvLoader"

    // Check that all other fields are preserved
    node.config.getString("format") shouldBe "csv"
    node.config.getString("path") shouldBe "/data/input"

    // Check nested config
    val options = node.config.getConfig("options")
    options.getBoolean("header") shouldBe true
    options.getString("delimiter") shouldBe ","
    options.getString("quote") shouldBe "\""
  }

  it should "handle type as the only field in config" in {
    val configStr =
      """
        type = "minimal"
      """

    val config = createConfig(configStr)
    val node   = ConfigNode(config)

    node.typeName shouldBe "minimal"

    // Name should be auto-generated
    node.name should startWith("minimal-")

    // Config should be empty (after removing "type")
    node.config.entrySet().size shouldBe 0
  }

  it should "handle non-string config values" in {
    val configStr =
      """
        type = "numeric"
        name = "numericProcessor"
        intValue = 42
        doubleValue = 3.14
        boolValue = true
        listValue = [1, 2, 3]
      """

    val config = createConfig(configStr)
    val node   = ConfigNode(config)

    node.typeName shouldBe "numeric"
    node.name shouldBe "numericProcessor"

    // Check various value types
    node.config.getInt("intValue") shouldBe 42
    node.config.getDouble("doubleValue") shouldBe 3.14
    node.config.getBoolean("boolValue") shouldBe true
    node.config.getIntList("listValue").size shouldBe 3
  }

  it should "handle case sensitivity correctly" in {
    val configStr =
      """
        TYPE = "wrong"
        type = "correct"
        Name = "wrongName"
        name = "correctName"
      """

    val config = createConfig(configStr)
    val node   = ConfigNode(config)

    // HOCON is case-sensitive, so it should pick the lowercase keys
    node.typeName shouldBe "correct"
    node.name shouldBe "correctName"
  }
}
