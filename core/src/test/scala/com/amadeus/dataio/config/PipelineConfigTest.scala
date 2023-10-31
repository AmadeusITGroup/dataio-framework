package com.amadeus.dataio.config

import com.amadeus.dataio.testutils.JavaImplicitConverters._
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PipelineConfigTest extends AnyWordSpec with Matchers {
  "PipelineConfig.apply" should {
    "return a PipelineConfig" when {
      "given a typesafe Config object" in {
        val config = ConfigFactory.parseMap(
          Map(
            "Input"        -> Map("Type" -> "com.Input"),
            "Processing"   -> Map("Type" -> "com.Processor"),
            "Output"       -> Map("Type" -> "com.Output"),
            "Distribution" -> Map("Type" -> "com.Distribution")
          )
        )

        val result = PipelineConfig(config)
        result.input.nodes.head.typeName shouldBe "com.Input"
        result.processing.nodes.head.typeName shouldBe "com.Processor"
        result.output.nodes.head.typeName shouldBe "com.Output"
        result.distribution.nodes.head.typeName shouldBe "com.Distribution"
      }
    }

    "return a PipelineConfig from the resources/application.conf file" when {

      "given the absolute path of application.conf" in {
        val result = PipelineConfig(getClass.getResource("/application.conf").getPath)
        result.input.nodes.head.typeName shouldBe "com.DefaultConfigInput"
        result.processing.nodes.head.typeName shouldBe "com.DefaultConfigProcessor"
        result.output.nodes.head.typeName shouldBe "com.DefaultConfigOutput"
        result.distribution.nodes.head.typeName shouldBe "com.DefaultConfigDistribution"
      }

      "given a relative path to application.conf" in {
        val result = PipelineConfig(getClass.getResource("/application.conf").getPath)
        result.input.nodes.head.typeName shouldBe "com.DefaultConfigInput"
        result.processing.nodes.head.typeName shouldBe "com.DefaultConfigProcessor"
        result.output.nodes.head.typeName shouldBe "com.DefaultConfigOutput"
        result.distribution.nodes.head.typeName shouldBe "com.DefaultConfigDistribution"
      }

      "given a URL to application.conf" in {
        val result = PipelineConfig(getClass.getResource("/application.conf").getPath)
        result.input.nodes.head.typeName shouldBe "com.DefaultConfigInput"
        result.processing.nodes.head.typeName shouldBe "com.DefaultConfigProcessor"
        result.output.nodes.head.typeName shouldBe "com.DefaultConfigOutput"
        result.distribution.nodes.head.typeName shouldBe "com.DefaultConfigDistribution"
      }
    }
  }
}
