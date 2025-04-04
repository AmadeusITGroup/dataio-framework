package com.amadeus.dataio.config

import com.amadeus.dataio.testutils.ConfigCreator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigNodeCollectionTest extends AnyFlatSpec with ConfigCreator with Matchers {
  "ConfigNodeCollection" should "create a collection from a list of config nodes" in {
    val config     = createConfig("""
          processors = [
            {
              type = "filter"
              name = "filterA"
              condition = "value > 10"
            },
            {
              type = "transform"
              name = "transformB"
              mapping = "value * 2"
            }
          ]
        """)
    val collection = ConfigNodeCollection("processors", config)

    collection.nodes.length shouldBe 2

    // Check first node
    collection.nodes(0).name shouldBe "filterA"
    collection.nodes(0).typeName shouldBe "filter"
    collection.nodes(0).config.getString("condition") shouldBe "value > 10"

    // Check second node
    collection.nodes(1).name shouldBe "transformB"
    collection.nodes(1).typeName shouldBe "transform"
    collection.nodes(1).config.getString("mapping") shouldBe "value * 2"
  }

  it should "create a collection from a single config object" in {
    val config     = createConfig("""
          processor = {
            type = "filter"
            name = "singleFilter"
            condition = "value > 10"
          }
        """)
    val collection = ConfigNodeCollection("processor", config)

    collection.nodes.length shouldBe 1
    collection.nodes(0).name shouldBe "singleFilter"
    collection.nodes(0).typeName shouldBe "filter"
    collection.nodes(0).config.getString("condition") shouldBe "value > 10"
  }

  it should "return empty collection when nodeName is not found" in {
    val config     = createConfig("""
          other = {
            type = "filter"
            name = "filter"
          }
        """)
    val collection = ConfigNodeCollection("processors", config)

    collection.nodes shouldBe Nil
  }

  it should "throw exception when a node config is invalid (missing type)" in {
    val config = createConfig("""
          processors = [
            {
              name = "filterA"
              condition = "value > 10"
            }
          ]
        """)

    val exception = intercept[Exception] {
      ConfigNodeCollection("processors", config)
    }

    exception.getMessage should include("processors child node configuration failed")
    exception.getCause.getMessage shouldBe "Configuration missing a `type` field."
  }

  it should "throw exception when node is neither a list nor an object" in {
    val config = createConfig("""
          processors = "invalid"
        """)

    val exception = intercept[Exception] {
      ConfigNodeCollection("processors", config)
    }

    exception.getMessage shouldBe "A configuration node must be a List or an Object."
  }

  it should "handle empty list of nodes" in {
    val config     = createConfig("""
          processors = []
        """)
    val collection = ConfigNodeCollection("processors", config)

    collection.nodes shouldBe Nil
  }

  it should "handle mixed node configurations" in {
    val config     = createConfig("""
          processors = [
            {
              type = "filter"
              condition = "value > 10"
            },
            {
              type = "transform"
              name = "transformB"
              mapping = "value * 2"
            }
          ]
        """)
    val collection = ConfigNodeCollection("processors", config)

    collection.nodes.length shouldBe 2

    // First node should have auto-generated name
    collection.nodes(0).typeName shouldBe "filter"
    collection.nodes(0).name should startWith("filter-")
    collection.nodes(0).config.getString("condition") shouldBe "value > 10"

    // Second node has explicit name
    collection.nodes(1).name shouldBe "transformB"
  }

  it should "create correct string representation" in {
    val config     = createConfig("""
          processors = [
            {
              type = "filter"
              name = "filterA"
            },
            {
              type = "transform"
              name = "transformB"
            }
          ]
        """)
    val collection = ConfigNodeCollection("processors", config)

    collection.toString shouldBe "[{filterA: filter}, {transformB: transform}]"
  }

  it should "handle nested configurations within nodes" in {
    val config     = createConfig("""
          processors = [
            {
              type = "complex"
              name = "complexProcessor"
              settings {
                threshold = 10
                options {
                  enabled = true
                  mode = "strict"
                }
              }
            }
          ]
        """)
    val collection = ConfigNodeCollection("processors", config)

    collection.nodes.length shouldBe 1
    val node = collection.nodes(0)

    node.name shouldBe "complexProcessor"
    node.typeName shouldBe "complex"

    // Verify nested config is preserved
    val settings = node.config.getConfig("settings")
    settings.getInt("threshold") shouldBe 10

    val options = settings.getConfig("options")
    options.getBoolean("enabled") shouldBe true
    options.getString("mode") shouldBe "strict"
  }

  it should "handle multiple collections in the same config" in {
    val config = createConfig("""
          inputs = [
            {
              type = "file"
              name = "fileInput"
              path = "/data/input"
            }
          ]

          processors = [
            {
              type = "filter"
              name = "mainFilter"
            }
          ]

          outputs = [
            {
              type = "database"
              name = "dbOutput"
              connection = "jdbc:..."
            }
          ]
        """)

    val inputs     = ConfigNodeCollection("inputs", config)
    val processors = ConfigNodeCollection("processors", config)
    val outputs    = ConfigNodeCollection("outputs", config)

    inputs.nodes.length shouldBe 1
    inputs.nodes(0).typeName shouldBe "file"

    processors.nodes.length shouldBe 1
    processors.nodes(0).typeName shouldBe "filter"

    outputs.nodes.length shouldBe 1
    outputs.nodes(0).typeName shouldBe "database"
  }
}
